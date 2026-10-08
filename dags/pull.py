"""Download the web pages listed in 00_urls.txt and save each one as an .html file.

Works with any site, not just the MIT catalog. Put one URL per line in
00_urls.txt. Lines that do not start with http:// or https:// (blank lines,
headers, comments starting with #) are ignored.
"""
import hashlib
import os
import re
import time
import urllib.request
import urllib.robotparser
from urllib.parse import urlparse

URL_FILE = "/opt/airflow/dags/00_urls.txt"
OUTPUT_DIR = "/opt/airflow/data"   # shared folder, also mounted on your computer
DELAY_SECONDS = 15         # pause between requests, to be polite to the server
TIMEOUT_SECONDS = 30       # give up on a page that takes longer than this

BOT_NAME = "course-project-bot"
HEADERS = {
    # Many sites reject urllib's default "Python-urllib" agent, so identify ourselves.
    "User-Agent": f"Mozilla/5.0 (compatible; {BOT_NAME}/1.0)",
    "Accept": "text/html,application/xhtml+xml",
    "Accept-Language": "en-US,en;q=0.9",
}

_robots_cache = {}


def read_urls(path=None):
    """Return every http(s) URL in the URL file, one per line."""
    path = path or URL_FILE
    urls = []
    with open(path) as f:
        for line in f:
            line = line.strip()
            if line.startswith("http://") or line.startswith("https://"):
                urls.append(line)
    return urls


def url_to_filename(url):
    """Build a safe, readable file name from any URL (no site-specific prefix)."""
    parts = urlparse(url)
    name = parts.netloc + parts.path
    if parts.query:
        name += "_" + parts.query
    name = re.sub(r"[^A-Za-z0-9._-]+", "_", name).strip("_.")
    if name.endswith(".html"):
        name = name[:-5]
    if len(name) > 120:
        # very long URLs: shorten and add a hash so two long URLs never collide
        name = name[:110] + "_" + hashlib.md5(url.encode()).hexdigest()[:8]
    return (name or "page") + ".html"


def allowed_by_robots(url):
    """Check the site's robots.txt. If it cannot be read, the page is treated as allowed."""
    parts = urlparse(url)
    root = f"{parts.scheme}://{parts.netloc}"
    if root not in _robots_cache:
        parser = urllib.robotparser.RobotFileParser()
        try:
            req = urllib.request.Request(root + "/robots.txt", headers=HEADERS)
            with urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS) as response:
                text = response.read().decode("utf-8", errors="replace")
            parser.parse(text.splitlines())
        except Exception:
            parser = None
        _robots_cache[root] = parser
    parser = _robots_cache[root]
    return True if parser is None else parser.can_fetch(BOT_NAME, url)


def pull(url):
    """Download one page and return its HTML as text, or None if it fails."""
    req = urllib.request.Request(url, headers=HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=TIMEOUT_SECONDS) as response:
            raw = response.read()
            charset = response.headers.get_content_charset() or "utf-8"
            return raw.decode(charset, errors="replace")
    except Exception as e:
        print(f"Failed to fetch {url}: {e}")
        return None


def store(data, url):
    """Save the HTML in a file named after the URL."""
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, url_to_filename(url))
    with open(path, "w", encoding="utf-8") as f:
        f.write(data)
    print("wrote file: " + path)


def catalog():
    """Airflow task: pull every URL in the URL file and save the pages."""
    urls = read_urls()
    if not urls:
        raise RuntimeError(f"No URLs found in {URL_FILE}")

    saved = 0
    for i, url in enumerate(urls):
        if not allowed_by_robots(url):
            print("Skipped (robots.txt disallows it): " + url)
            continue

        data = pull(url)
        if data is not None:
            store(data, url)
            saved += 1
            print("pulled: " + url)

        if i < len(urls) - 1:
            print("--- waiting ---")
            time.sleep(DELAY_SECONDS)

    print(f"Saved {saved} of {len(urls)} pages")
    if saved == 0:
        raise RuntimeError("No pages were downloaded, check the log above")
