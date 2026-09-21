"""
playlist_builder.py — Movierulz -> IPTV Playlist Builder
=======================================================
Scrapes Movierulz movie listings, extracts stream URLs, and builds
an IPTV-compatible M3U playlist compatible with OTT Navigator, TiviMate,
VLC, and any HLS player.

Usage:
    python playlist_builder.py                          # build and print
    python playlist_builder.py --output playlist.m3u    # build and save
    python playlist_builder.py --append                  # append new to existing

GitHub Actions:
    The update.yml workflow runs this script on a schedule,
    appends new movies to playlist.m3u, and commits the result.
"""

import argparse
import hashlib
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from urllib.parse import urljoin

import requests


# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

HEADERS = {"User-Agent": USER_AGENT}

PROXY_TEMPLATE = os.environ.get(
    "PROXY_TEMPLATE",
    "https://movierulz.babuperumana.workers.dev/proxy?url={url}",
)

MAX_LIST_PAGES = int(os.environ.get("MAX_LIST_PAGES", "500"))
POLITE_DELAY = float(os.environ.get("POLITE_DELAY", "0.5"))

# Concurrent workers for page discovery (fast, read-only)
DISCOVERY_WORKERS = int(os.environ.get("DISCOVERY_WORKERS", "50"))
MAX_DISCOVERY_PAGES = int(os.environ.get("MAX_DISCOVERY_PAGES", "200"))  # Cap page discovery to prevent unbounded scraping

# Max movies to extract streams for per run (to stay within timeout).
# The playlist grows incrementally over multiple runs.
MAX_STREAMS_PER_RUN = int(os.environ.get("MAX_STREAMS_PER_RUN", "100"))

MOVIERULZ_DOMAINS = [
    "https://www.5movierulz.ventures",
    "https://www.5movierulz.cfd",
    "https://www.5movierulz.watch",
    "https://www.5movierulz.lat",
    "https://www.5movierulz.pe",
    "https://www.5movierulz.vc",
    "https://www.5movierulz.ms",
    "https://www.5movierulz.plumbing",
    "https://www.5movierulz.st",
    "https://www.5movierulz.mx",
    "https://www.5movierulz.ch",
    "https://www.5movierulz.to",
    "https://www.5movierulz.gd",
    "https://www.5movierulz.green",
    "https://www.4movierulz.tv",
    "https://www.3movierulz.in",
    "https://ww1.5movierulz.to",
]

DEFAULT_CACHE_FILE = ".playlist_cache.json"
DEFAULT_PAGE_CACHE_FILE = ".page_cache.json"


# ---------------------------------------------------------------------------
# HTTP helpers
# ---------------------------------------------------------------------------

def _get(session, url, **kwargs):
    """GET with standard User-Agent and error handling."""
    try:
        resp = session.get(url, headers=HEADERS, timeout=15, **kwargs)
        return resp
    except requests.RequestException as exc:
        print(f"  [warn] Request failed: {url} ({exc})")
        return None


# ---------------------------------------------------------------------------
# Step 1: Discover movie listing pages
# ---------------------------------------------------------------------------

def _fetch_page(session, url):
    """Fetch a single page, return (url, html_or_None)."""
    try:
        resp = session.get(url, headers=HEADERS, timeout=10)
        if resp.status_code == 200:
            return url, resp.text
    except Exception:
        pass
    return url, None


def _fetch_pages_concurrent(session, urls, max_workers=50):
    """Fetch multiple pages concurrently using thread pool."""
    results = {}
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_fetch_page, session, url): url for url in urls}
        for future in as_completed(futures):
            try:
                url, html = future.result(timeout=15)
                if html:
                    results[url] = html
            except Exception:
                pass
    return results


def _page_has_movies(html):
    """Check if a listing page HTML has movie entries."""
    titles = re.findall(r'<a[^>]+title="([^"]+)"[^>]+href="(https?://[^"]+\.html)"', html)
    return any(MOVIE_URL_RE.match(u) for _, u in titles)


def _extract_movie_urls(html):
    """Extract unique movie URLs from a listing page HTML."""
    titles = re.findall(r'<a[^>]+title="([^"]+)"[^>]+href="(https?://[^"]+\.html)"', html)
    return list(set(u for _, u in titles if MOVIE_URL_RE.match(u)))


def discover_listing_pages(session, base_url, page_cache=None):
    """
    Find paginated movie listing pages incrementally.

    Strategy:
      1. Check cache for last discovered page (resume point)
      2. Fetch the next batch of pages concurrently (MAX_DISCOVERY_PAGES per run)
      3. Empty pages get marked with [] so they're not retried
      4. Over multiple runs, all pages are discovered
    """
    if page_cache is None:
        page_cache = {}

    # Determine where to resume from
    last_discovered = page_cache.get("_last_discovered_page", 0)
    print(f"[scraper] Last discovered page: {last_discovered}")

    domain = base_url
    for _ in [1]:
        resp = _get(session, domain)
        if not resp or resp.status_code != 200:
            continue

        html = resp.text

        # Get max linked page from pagination (usually 15-421)
        page_nums = [
            int(m.group(1))
            for link in set(re.findall(r'href=["\']([^"\']*\/page\/\d+[^"\']*)["\']', html))
            if (m := re.search(r'/page/(\d+)', link))
        ]
        if not page_nums:
            print("[scraper] No pagination found")
            return [], page_cache

        max_linked = max(page_nums)
        print(f"[scraper] Max linked page: {max_linked}")

        # Determine the range to discover this run
        # First run: fetch pages 1..max_linked + next batch
        # Subsequent runs: resume from last_discovered + 1
        if last_discovered == 0:
            start_page = 1
            end_page = start_page + MAX_DISCOVERY_PAGES - 1
            print(f"[scraper] First run: discovering pages {start_page}-{end_page}")
        else:
            start_page = last_discovered + 1
            end_page = start_page + MAX_DISCOVERY_PAGES - 1
            print(f"[scraper] Resuming: discovering pages {start_page}-{end_page}")

        # Build list of pages to fetch (include pages marked empty to retry)
        to_fetch = []
        
        # Always check first 5 pages for newly added movies
        for i in range(1, 6):
            to_fetch.append(f"{domain}/movies/page/{i}")
            
        for i in range(start_page, end_page + 1):
            url = f"{domain}/movies/page/{i}"
            if url not in page_cache or not page_cache[url]:
                if url not in to_fetch:
                    to_fetch.append(url)

        if not to_fetch:
            print("[scraper] No new pages to discover")
            # Return all known page URLs
            final_urls = sorted([u for u in page_cache
                                 if u.startswith(f"{domain}/movies/page/") and not u.startswith("_")])
            return final_urls, page_cache

        print(f"[scraper] Fetching {len(to_fetch)} pages concurrently...")

        # Concurrent batch fetch
        fetched = {}
        batch_size = DISCOVERY_WORKERS
        for i in range(0, len(to_fetch), batch_size):
            batch = to_fetch[i:i + batch_size]
            results = _fetch_pages_concurrent(session, batch, max_workers=DISCOVERY_WORKERS)
            fetched.update(results)

        # Process results: find last page with movies, update cache
        last_with_content = last_discovered
        for url, html in fetched.items():
            if html and _page_has_movies(html):
                page_cache[url] = _extract_movie_urls(html)
                m = re.search(r'/movies/page/(\d+)', url)
                if m:
                    pg = int(m.group(1))
                    last_with_content = max(last_with_content, pg)
            else:
                page_cache[url] = []

        # Update resume marker
        page_cache["_last_discovered_page"] = last_with_content

        # Return all known page URLs
        final_urls = sorted([u for u in page_cache
                             if u.startswith(f"{domain}/movies/page/") and not u.startswith("_")])
        print(f"[scraper] Discovery done: last page with content = {last_with_content}, total known = {len(final_urls)}")
        return final_urls, page_cache

    print("[scraper] Could not reach any Movierulz domain")
    return [], page_cache
# ---------------------------------------------------------------------------
# Step 2: Extract movie page links from listing pages
# ---------------------------------------------------------------------------

# Current URL pattern: /title-year-quality-language-ID.html or /title/movie-watch-online-free-ID.html
MOVIE_URL_RE = re.compile(
    r'^https?://[^/]+/[a-z0-9-]+(?:/[a-z0-9-]+)*\.html$'
)


GENERIC_TITLES = {
    "movie watch online free",
    "watch online free",
    "full movie watch online",
    "watch online",
    "movierulz",
    "home",
    "featured movies free",
}


def _clean_title(title, url):
    """Sanitize generic title strings and fall back to url/slug parsing if necessary."""
    if not title:
        return _guess_title_from_url(url)
    cleaned = re.sub(
        r'(?i)\b(movie watch online free|watch online free|full movie watch online|watch online)\b',
        '',
        title
    ).strip()
    if not cleaned or cleaned.lower() in GENERIC_TITLES:
        return _guess_title_from_url(url)
    return cleaned


def extract_movie_links(session, listing_urls, page_cache=None):
    """
    Scrape each listing page and collect movie entries.
    Uses page_cache to skip already-scraped pages.
    """
    if page_cache is None:
        page_cache = {}

    movies = []
    seen_urls = set()

    for listing_url in listing_urls:
        # If this page is already cached, use cached URLs (no HTTP request)
        cached_urls = page_cache.get(listing_url)
        if cached_urls is not None:
            print(f"[scraper]   {listing_url} -> {len(cached_urls)} movies (from cache)")
            for url in cached_urls:
                if url not in seen_urls:
                    seen_urls.add(url)
                    title = _guess_title_from_url(url)
                    year = _guess_year_from_url(url)
                    language = _guess_language(title, url)
                    quality = _guess_quality(title)
                    movies.append({
                        "title": title,
                        "url": url,
                        "year": year,
                        "language": language,
                        "quality": quality,
                    })
            continue

        # Not cached — fetch and scrape
        resp = _get(session, listing_url)
        if not resp or resp.status_code != 200:
            continue

        html = resp.text
        found = {}

        # Strategy 1: Extract from <div class="boxed film"> <p><b>title</b></p> (most reliable)
        for block in re.findall(
            r'<div class="boxed film">(.*?)</div>\s*</li>', html, re.DOTALL
        ):
            title_match = re.search(r'<p><b>([^<]+)</b></p>', block)
            link_match = re.search(r'<a[^>]+href="(https?://[^"]+\.html)"', block)
            if title_match and link_match:
                url = link_match.group(1)
                title = _clean_title(title_match.group(1), url)
                if MOVIE_URL_RE.match(url) and url not in seen_urls:
                    found[url] = title

        # Strategy 2: Extract from <a title="..." href="movie-url.html"> (fallback)
        for title, url in re.findall(
            r'<a[^>]+title="([^"]+)"[^>]+href="(https?://[^"]+\.html)"',
            html,
        ):
            if MOVIE_URL_RE.match(url) and url not in seen_urls and url not in found:
                cleaned_title = _clean_title(title, url)
                if cleaned_title and cleaned_title.lower() not in GENERIC_TITLES:
                    found[url] = cleaned_title

        for url, raw_title in found.items():
            seen_urls.add(url)
            title = _clean_title(raw_title, url)
            year = _guess_year(title) or _guess_year_from_url(url)
            language = _guess_language(title, url)
            quality = _guess_quality(title)
            movies.append({
                "title": title,
                "url": url,
                "year": year,
                "language": language,
                "quality": quality,
            })

        # Cache this page's results
        page_cache[listing_url] = list(found.keys())
        print(f"[scraper]   {listing_url} -> {len(found)} movies (fetched + cached)")
        time.sleep(POLITE_DELAY)

    return movies, page_cache



def _guess_title_from_url(url):
    """Guess a title from a movie URL when no title is available."""
    try:
        parts = [p for p in url.split("/") if p]
        slug = parts[-1].replace(".html", "")
        if "movie-watch-online" in slug or "watch-online" in slug or slug.isdigit():
            if len(parts) >= 2 and not parts[-2].startswith("http") and "movierulz" not in parts[-2]:
                slug = parts[-2]

        subparts = slug.split("-")
        title_parts = []
        for p in subparts:
            if re.match(r"^\d{4}$", p):
                val = int(p)
                if 1920 <= val <= 2030:
                    break
            title_parts.append(p)
        title = " ".join(title_parts).replace("-", " ").strip().title()
        if not title or title.lower() in GENERIC_TITLES:
            return slug.replace("-", " ").title()
        return title
    except Exception:
        return "Unknown"



def _guess_year_from_url(url):
    """Extract 4-digit year from URL pattern."""
    if not url:
        return None
    match = re.search(r'[-_\s/](20[0-2][0-9])[-_\s/.]', url)
    if match:
        return int(match.group(1))
    return None


def _guess_year(title):
    """Extract year from title string."""
    if not title:
        return None
    match = re.search(r'\b(20[0-2][0-9])\b', title)
    return int(match.group(1)) if match else None



def _guess_language(title, url):
    """Detect language from title brackets or URL."""
    combined = (title + " " + url).lower()
    languages = {
        "telugu": "Telugu",
        "tamil": "Tamil",
        "malayalam": "Malayalam",
        "hindi": "Hindi",
        "kannada": "Kannada",
        "bengali": "Bengali",
        "punjabi": "Punjabi",
        "english": "English",
        "hollywood": "English",
    }
    for key, label in languages.items():
        if key in combined:
            return label
    return "Other"


def _guess_quality(title):
    """Extract quality from title like 'DVDScr', 'HDRip', 'CAM', etc."""
    match = re.search(r'\((\d{4})\)\s+([^\s\[]+)', title)
    if match:
        return match.group(2)
    return ""


# ---------------------------------------------------------------------------
# Step 3: Extract stream URLs from movie pages
# ---------------------------------------------------------------------------

def extract_streams(session, movies):
    """
    For each movie page, extract the raw HLS stream URL.
    """
    results = []
    total = len(movies)

    for idx, movie in enumerate(movies, 1):
        print(f"[extractor] ({idx}/{total}) {movie['title']}")

        resp = _get(session, movie["url"])
        if not resp or resp.status_code != 200:
            print(f"  [skip] could not fetch page")
            continue

        html = resp.text

        # Extract poster image for tvg-logo
        img_match = re.search(r'<img[^>]+src=["\'](https?://[^"\']+/uploads/[^"\']+)["\']', html)
        if img_match:
            movie["logo"] = img_match.group(1)

        # Find embedded player iframes: var locations = ["url1", "url2"];
        # Also check for: var players = [...]
        iframe_urls = []

        # Pattern 1: var locations = [...]
        match = re.search(r'var\s+locations\s*=\s*\[(.*?)\];', resp.text, re.DOTALL)
        if match:
            for raw_url in re.findall(r'"([^"]+)"', match.group(1)):
                iframe_urls.append(raw_url.replace('\\/', '/'))

        # Pattern 2: var players = [...]
        if not iframe_urls:
            match = re.search(r'var\s+players\s*=\s*\[(.*?)\];', resp.text, re.DOTALL)
            if match:
                for raw_url in re.findall(r'"([^"]+)"', match.group(1)):
                    iframe_urls.append(raw_url.replace('\\/', '/'))

        # Pattern 3: var file = "..." (single URL)
        if not iframe_urls:
            match = re.search(r'var\s+file\s*=\s*["\']([^"\']+)["\']', resp.text)
            if match:
                iframe_urls.append(match.group(1))

        if not iframe_urls:
            # Also try to find any iframe/src on the page
            iframe_urls = re.findall(r'(?:iframe|src)\s*[=:]\s*["\']([^"\']*(?:player|embed|stream|video)[^"\']*)["\']', html, re.IGNORECASE)

        if not iframe_urls:
            print(f"  [skip] no player iframes found")
            continue

        stream_url = None
        for iframe_url in iframe_urls:
            if '.m3u8' in iframe_url or '.mp4' in iframe_url or '.mkv' in iframe_url:
                stream_url = iframe_url
                break
            stream_url = _extract_from_iframe(session, iframe_url, movie["url"])
            if stream_url:
                break
            time.sleep(POLITE_DELAY)

        if stream_url:
            movie["stream_url"] = stream_url
            movie["added_at"] = datetime.now(timezone.utc).isoformat()
            movie["entry_id"] = _make_entry_id(movie["url"])
            results.append(movie)
            print(f"  [ok] stream found")
        else:
            print(f"  [skip] could not extract stream from any mirror")

    return results


def _extract_from_iframe(session, iframe_url, referer):
    """Try 3 regex strategies on an iframe page to find the HLS URL."""
    try:
        resp = session.get(
            iframe_url,
            headers={"User-Agent": USER_AGENT, "Referer": referer},
            timeout=12,
        )
        if resp.status_code != 200:
            return None

        html = resp.text

        # Strategy 1: <source src="...">
        m = re.search(r'<source[^>]+src=["\']([^"\']+)["\']', html)
        if m:
            url = m.group(1).strip()
            if url.startswith("http"):
                return url

        # Strategy 2: const source = "..."
        m = re.search(r'(?:const|var|let)\s+source\s*=\s*["\']([^"\']+)["\']', html)
        if m:
            url = m.group(1).strip()
            if url.startswith("http"):
                return url

        # Strategy 3: General HLS/VCDN pattern
        m = re.search(r'["\'](https?://[^"\']*(?:vcdn|hls|m3u8)[^"\']*)["\']', html)
        if m:
            return m.group(1).replace('\\/', '/')

    except Exception as exc:
        print(f"    [warn] iframe error: {exc}")

    return None


# ---------------------------------------------------------------------------
# Step 4: Build M3U playlist
# ---------------------------------------------------------------------------

def build_m3u(movies, proxy_template):
    """Generate an IPTV-compatible M3U playlist string."""
    lines = ["#EXTM3U", ""]

    for movie in movies:
        raw_url = movie.get("stream_url", "")
        if not raw_url:
            continue

        proxy_url = proxy_template.replace("{url}", requests.utils.quote(raw_url, safe=""))

        title = movie["title"]
        year = movie.get("year")
        group = _guess_group(movie)
        logo = movie.get("logo", "")

        display_name = title

        lines.append(
            f'#EXTINF:-1 tvg-name="{title}" tvg-logo="{logo}" '
            f'group-title="{group}",{display_name}'
        )
        lines.append(proxy_url)

    return "\n".join(lines) + "\n"


def _guess_group(movie):
    """Determine a category group with language and year subcategories from the movie data."""
    lang = movie.get("language")
    if not lang or lang == "Other":
        combined = (movie.get("url", "") + " " + movie.get("title", "")).lower()
        languages = {
            "telugu": "Telugu",
            "tamil": "Tamil",
            "malayalam": "Malayalam",
            "hindi": "Hindi",
            "kannada": "Kannada",
            "bengali": "Bengali",
            "punjabi": "Punjabi",
            "english": "English",
            "hollywood": "English",
        }
        for key, label in languages.items():
            if key in combined:
                lang = label
                break
        else:
            lang = "Other"

    year = movie.get("year")
    if not year:
        year = _guess_year(movie.get("title", "")) or _guess_year_from_url(movie.get("url", ""))

    if year:
        return f"Movies / {lang} / {year}"
    return f"Movies / {lang}"



# ---------------------------------------------------------------------------
# Page listing cache
# ---------------------------------------------------------------------------

def load_page_cache(cache_file):
    """Load cached page→movie URL mappings from disk."""
    path = Path(cache_file)
    if path.exists():
        try:
            return json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def save_page_cache(cache, cache_file):
    """Persist the page cache to disk."""
    Path(cache_file).write_text(json.dumps(cache, indent=2))


def get_known_page_urls(page_cache):
    """Get set of all movie URLs already seen from page scraping."""
    urls = set()
    for page_url, movie_urls in page_cache.items():
        urls.update(movie_urls)
    return urls


# ---------------------------------------------------------------------------
# Step 5: Deduplication & caching
# ---------------------------------------------------------------------------

def _make_entry_id(url):
    """Generate a stable hash ID from the movie page URL."""
    return hashlib.sha256(url.encode()).hexdigest()[:16]


def load_cache(cache_file):
    """Load the dedup cache from disk."""
    path = Path(cache_file)
    if path.exists():
        try:
            return json.loads(path.read_text())
        except (json.JSONDecodeError, OSError):
            pass
    return {}


def save_cache(cache, cache_file):
    """Persist the dedup cache to disk."""
    Path(cache_file).write_text(json.dumps(cache, indent=2))


def deduplicate(movies, cache):
    """Filter out movies whose entry_id is already in the cache."""
    new_movies = []
    for movie in movies:
        eid = movie.get("entry_id")
        if eid and eid in cache:
            continue
        if eid:
            cache[eid] = movie
        new_movies.append(movie)
    return new_movies, cache


def merge_with_existing(new_content, existing_file):
    """
    Append new movie entries to the existing playlist.
    Skips entries whose stream URL or (name, group) already exists in the playlist.
    Preserves all existing entries and their URLs intact.
    """
    existing_path = Path(existing_file)
    if not existing_path.exists() or existing_path.stat().st_size == 0:
        return new_content

    existing = existing_path.read_text()

    # Collect existing stream URLs and (tvg-name, group-title) keys for dedup
    existing_urls = set()
    existing_keys = set()
    lines = existing.split("\n")
    current_hdr = None
    for line in lines:
        stripped = line.strip()
        if stripped.startswith("#EXTINF"):
            current_hdr = stripped
        elif stripped and not stripped.startswith("#"):
            existing_urls.add(stripped)
            if current_hdr:
                name_m = re.search(r'tvg-name="([^"]+)"', current_hdr)
                grp_m = re.search(r'group-title="([^"]+)"', current_hdr)
                if name_m and grp_m:
                    existing_keys.add((name_m.group(1), grp_m.group(1)))
            current_hdr = None

    # Parse new entries into (header, url) pairs
    new_entries = []
    current_header = None
    for line in new_content.strip().split("\n"):
        stripped = line.strip()
        if stripped == "#EXTM3U" or stripped == "":
            continue
        if stripped.startswith("#EXTINF"):
            current_header = stripped
        elif not stripped.startswith("#"):
            current_url = stripped
            if current_header:
                new_entries.append((current_header, current_url))
                current_header = None

    # Filter: only keep entries whose URL and key aren't already present
    unique_new = []
    for header, url in new_entries:
        if url in existing_urls:
            continue
        name_m = re.search(r'tvg-name="([^"]+)"', header)
        grp_m = re.search(r'group-title="([^"]+)"', header)
        if name_m and grp_m:
            key = (name_m.group(1), grp_m.group(1))
            if key in existing_keys:
                continue
            existing_keys.add(key)
        existing_urls.add(url)
        unique_new.append((header, url))

    if not unique_new:
        return existing

    # Build the appended content
    appended_lines = ["", ""]
    for header, url in unique_new:
        appended_lines.append(header)
        appended_lines.append(url)
    appended_lines.append("")

    return existing.rstrip("\n") + "\n" + "\n".join(appended_lines) + "\n"



# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _get_existing_keys(existing_file):
    """Read the existing M3U file and return a set of (tvg-name, group-title)."""
    existing_keys = set()
    existing_path = Path(existing_file)
    if not existing_path.exists() or existing_path.stat().st_size == 0:
        return existing_keys

    try:
        existing = existing_path.read_text(encoding="utf-8")
        lines = existing.split("\n")
        current_hdr = None
        for line in lines:
            stripped = line.strip()
            if stripped.startswith("#EXTINF"):
                current_hdr = stripped
            elif stripped and not stripped.startswith("#"):
                if current_hdr:
                    name_m = re.search(r'tvg-name="([^"]+)"', current_hdr)
                    grp_m = re.search(r'group-title="([^"]+)"', current_hdr)
                    if name_m and grp_m:
                        existing_keys.add((name_m.group(1), grp_m.group(1)))
                current_hdr = None
    except Exception as exc:
        print(f"[warn] Failed to read existing keys: {exc}")
    return existing_keys


def run(output="playlist.m3u", append=False, cache_file=DEFAULT_CACHE_FILE):
    """
    Run the full pipeline: scrape -> extract -> build M3U.

    This is incremental:
      - Uses page cache to skip already-scraped listing pages
      - Only discovers up to MAX_DISCOVERY_PAGES per run (first run discovers all)
      - Skips movies already in the entry cache
      - Extracts streams for up to MAX_STREAMS_PER_RUN new movies
      - Over multiple runs, the playlist grows to include all movies
    """
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})

    # Load both caches
    cache = load_cache(cache_file)
    page_cache = load_page_cache(DEFAULT_PAGE_CACHE_FILE)

    # Step 1: Discover listing pages (uses cache to skip known pages)
    all_listing_urls = []
    for domain in MOVIERULZ_DOMAINS:
        print(f"[scraper] Discovering listing pages from {domain}...")
        try:
            listing_urls, page_cache = discover_listing_pages(session, domain, page_cache)
            if listing_urls:
                all_listing_urls.extend(listing_urls)
                print(f"[scraper] Found {len(listing_urls)} pages on {domain}")
        except Exception as e:
            print(f"[warn] Failed to discover pages on {domain}: {e}")

    if not all_listing_urls:
        print("[error] No listing pages found on any domain -- is Movierulz reachable?")
        sys.exit(1)
        
    listing_urls = list(set(all_listing_urls)) # optional deduplication

    # Step 2: Collect movie links (uses cache for instant page loads)
    movies, page_cache = extract_movie_links(session, listing_urls, page_cache)
    print(f"[scraper] Found {len(movies)} movies across all pages")

    if not movies:
        print("[info] No movies found -- skipping this run")
        save_page_cache(page_cache, DEFAULT_PAGE_CACHE_FILE)
        m3u_content = "#EXTM3U\n"
        if append:
            m3u_content = merge_with_existing(m3u_content, output)
        return m3u_content

    # Step 3: Filter out already-processed movies using cache and existing playlist
    pending = []
    existing_keys = _get_existing_keys(output) if append else set()
    current_time = time.time()

    for movie in movies:
        eid = _make_entry_id(movie["url"])
        if eid in cache:
            # If the movie failed in a previous run, try again after 3 days (259200 seconds)
            cached_data = cache[eid]
            if isinstance(cached_data, dict) and cached_data.get("failed") is True:
                failed_at = cached_data.get("failed_at", 0)
                if current_time - failed_at > 259200:
                    pass # Retry this movie
                else:
                    continue
            else:
                continue
            
        # Check against existing playlist so we don't extract duplicates
        if append:
            group = _guess_group(movie)
            if (movie["title"], group) in existing_keys:
                cache[eid] = {"url": movie["url"], "failed": False, "reason": "duplicate_in_playlist"}
                continue

        pending.append(movie)

    print(f"[dedup] {len(movies) - len(pending)} already processed, {len(pending)} pending")

    if not pending:
        print("[info] All movies already in playlist -- up to date")
        save_page_cache(page_cache, DEFAULT_PAGE_CACHE_FILE)
        m3u_content = "#EXTM3U\n"
        if append:
            m3u_content = merge_with_existing(m3u_content, output)
        return m3u_content

    # Prioritize 2026 movies (False/0 comes before True/1)
    pending.sort(key=lambda m: m.get("year") != 2026)

    # Step 4: Extract streams for a batch of pending movies
    batch = pending[:MAX_STREAMS_PER_RUN]
    print(f"[extractor] Processing batch of {len(batch)} movies (max per run: {MAX_STREAMS_PER_RUN})")

    enriched = extract_streams(session, batch)
    print(f"[extractor] Successfully extracted {len(enriched)} streams")

    if not enriched:
        print("[info] No streams extracted -- skipping this run")
        save_page_cache(page_cache, DEFAULT_PAGE_CACHE_FILE)
        m3u_content = "#EXTM3U\n"
        if append:
            m3u_content = merge_with_existing(m3u_content, output)
        return m3u_content

    # Step 5: Build M3U content
    m3u_content = build_m3u(enriched, PROXY_TEMPLATE)

    # Step 6: Merge with existing playlist if appending
    if append:
        m3u_content = merge_with_existing(m3u_content, output)

    # Step 7: Ensure no duplicate #EXTM3U headers
    lines = m3u_content.split("\n")
    cleaned = []
    extm3u_seen = False
    for line in lines:
        if line.strip() == "#EXTM3U":
            if not extm3u_seen:
                cleaned.append(line)
                extm3u_seen = True
        else:
            cleaned.append(line)
    m3u_content = "\n".join(cleaned)

    # Step 8: Update caches
    for movie in enriched:
        cache[_make_entry_id(movie["url"])] = movie
        
    enriched_urls = {_make_entry_id(m["url"]) for m in enriched}
    current_time = time.time()
    for movie in batch:
        eid = _make_entry_id(movie["url"])
        if eid not in enriched_urls:
            if eid not in cache:
                cache[eid] = {"url": movie["url"], "failed": True, "failed_at": current_time}
            elif isinstance(cache[eid], dict) and cache[eid].get("failed") is True:
                cache[eid]["failed_at"] = current_time

    save_cache(cache, cache_file)
    save_page_cache(page_cache, DEFAULT_PAGE_CACHE_FILE)

    return m3u_content


def main():
    parser = argparse.ArgumentParser(
        description="Build an IPTV M3U playlist from Movierulz"
    )
    parser.add_argument(
        "--output", "-o", default="playlist.m3u",
        help="Output M3U file path (default: playlist.m3u)",
    )
    parser.add_argument(
        "--append", "-a", action="store_true",
        help="Append new movies to existing playlist instead of overwriting",
    )
    parser.add_argument(
        "--cache", "-c", default=DEFAULT_CACHE_FILE,
        help=f"Cache file for dedup (default: {DEFAULT_CACHE_FILE})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Generate M3U but don't write to file",
    )
    args = parser.parse_args()

    content = run(output=args.output, append=args.append, cache_file=args.cache)

    if args.dry_run:
        print(content)
    else:
        Path(args.output).write_text(content)
        entry_count = len([l for l in content.split("\n") if l.startswith("#EXTINF")])
        print(f"[done] Wrote {entry_count} entries to {args.output}")


if __name__ == "__main__":
    main()
