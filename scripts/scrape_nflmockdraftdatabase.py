#!/usr/bin/env python
from __future__ import annotations

import argparse
import json
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta
from html import unescape
from pathlib import Path
from typing import Any
from urllib.parse import urlencode, urljoin
from urllib.parse import urlparse

import pandas as pd
import requests


BASE_URL = "https://www.nflmockdraftdatabase.com"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)
DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
SUPPORTED_SECTIONS = ("mock-drafts", "team-mock-drafts", "teams")
ROUND_ENDS = (32, 64, 102, 138, 176, 217, 257)
REACT_PROPS_PATTERN = re.compile(r'data-react-props="(.*?)" data-react-cache-id="([^"]+)"', re.S)
DEFAULT_DEBUG_DIR = Path(__file__).resolve().parents[1] / "data" / "raw" / "_debug" / "fetch_failures"
TEAM_ARTICLE_PREFIX_TO_SLUG = {
    "49ers": "san-francisco-49ers",
    "bears": "chicago-bears",
    "bengals": "cincinnati-bengals",
    "bills": "buffalo-bills",
    "broncos": "denver-broncos",
    "browns": "cleveland-browns",
    "buccaneers": "tampa-bay-buccaneers",
    "cardinals": "arizona-cardinals",
    "chargers": "los-angeles-chargers",
    "chiefs": "kansas-city-chiefs",
    "colts": "indianapolis-colts",
    "commanders": "washington-commanders",
    "cowboys": "dallas-cowboys",
    "dolphins": "miami-dolphins",
    "eagles": "philadelphia-eagles",
    "falcons": "atlanta-falcons",
    "giants": "new-york-giants",
    "jaguars": "jacksonville-jaguars",
    "jets": "new-york-jets",
    "lions": "detroit-lions",
    "packers": "green-bay-packers",
    "panthers": "carolina-panthers",
    "patriots": "new-england-patriots",
    "raiders": "las-vegas-raiders",
    "rams": "los-angeles-rams",
    "ravens": "baltimore-ravens",
    "saints": "new-orleans-saints",
    "seahawks": "seattle-seahawks",
    "steelers": "pittsburgh-steelers",
    "texans": "houston-texans",
    "titans": "tennessee-titans",
    "vikings": "minnesota-vikings",
}


class RestrictedRedirectError(RuntimeError):
    """Raised when the site redirects a scripted request into the /restricted loop."""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Scrape NFL Mock Draft Database list pages and individual mock pages into normalized CSVs."
    )
    parser.add_argument("--year", type=int, required=True, help="Draft year, for example 2025 or 2026.")
    parser.add_argument(
        "--section",
        action="append",
        dest="sections",
        choices=SUPPORTED_SECTIONS,
        help="Site section to scrape. Repeat the flag to scrape multiple sections.",
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Base data directory. Raw files go to data/raw and processed CSVs go to data/processed.",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=25,
        help="Safety cap on paginated list pages to scrape per section.",
    )
    parser.add_argument(
        "--max-mocks",
        type=int,
        default=None,
        help="Optional cap on the number of individual mock pages to fetch per section.",
    )
    parser.add_argument(
        "--published-month",
        action="append",
        dest="published_months",
        type=int,
        choices=range(1, 13),
        help="Only keep mocks whose published_at month matches one of these values. Repeat the flag for multiple months.",
    )
    parser.add_argument(
        "--published-day-min",
        type=int,
        choices=range(1, 32),
        help="Only keep mocks whose published_at day-of-month is at least this value.",
    )
    parser.add_argument(
        "--published-date-from",
        type=str,
        help="Only keep mocks published on or after this ISO date, for example 2026-03-13.",
    )
    parser.add_argument(
        "--published-date-to",
        type=str,
        help="Only keep mocks published on or before this ISO date, for example 2026-03-27.",
    )
    parser.add_argument(
        "--published-days-back",
        type=int,
        default=None,
        help="Only keep mocks from the most recent N days, inclusive of the as-of date.",
    )
    parser.add_argument(
        "--as-of-date",
        type=str,
        default=None,
        help="Reference ISO date for --published-days-back, for example 2026-03-27. Defaults to today.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.0,
        help="Optional pause between mock-page fetches.",
    )
    parser.add_argument(
        "--include-actual-results",
        action="store_true",
        help="Also scrape the actual draft-results page for the chosen year if it exists.",
    )
    parser.add_argument(
        "--fetch-backend",
        choices=("auto", "requests", "curl", "powershell"),
        default="auto",
        help="HTTP backend. 'auto' tries requests first, then curl, then PowerShell on Windows.",
    )
    parser.add_argument(
        "--checkpoint-every",
        type=int,
        default=25,
        help="Write checkpoint CSVs and a progress JSON after this many newly parsed mocks.",
    )
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Reuse existing raw HTML files and resume from checkpoint files when available.",
    )
    parser.add_argument(
        "--refresh-list-pages",
        action="store_true",
        help="When used with --resume, refetch index/list pages and team aggregator pages while still reusing existing individual mock pages.",
    )
    parser.add_argument(
        "--latest-author-mock-only",
        action="store_true",
        help="Keep only the latest mock per author on the list page before fetching individual mock pages.",
    )
    parser.add_argument(
        "--team-slug",
        action="append",
        dest="team_slugs",
        help="Optional team slug to limit --section teams, for example philadelphia-eagles. Repeat for multiple teams.",
    )
    parser.add_argument(
        "--mock-url",
        action="append",
        dest="mock_urls",
        help="Optional direct mock URL to ingest, for example a /mock-drafts/ or /team-mock-drafts/ page. Repeat for multiple URLs.",
    )
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def write_fetch_debug_artifact(
    *,
    backend: str,
    url: str,
    stdout: str,
    stderr: str,
    returncode: int,
) -> Path:
    ensure_dir(DEFAULT_DEBUG_DIR)
    parsed = urlparse(url)
    path_slug = slugify(f"{parsed.netloc}_{parsed.path}_{parsed.query}") or "response"
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    debug_path = DEFAULT_DEBUG_DIR / f"{timestamp}__{backend}__{path_slug}.txt"
    debug_text = (
        f"timestamp: {timestamp}\n"
        f"backend: {backend}\n"
        f"url: {url}\n"
        f"returncode: {returncode}\n"
        f"stdout_chars: {len(stdout)}\n"
        f"stderr_chars: {len(stderr)}\n"
        "\n=== STDERR ===\n"
        f"{stderr}\n"
        "\n=== STDOUT ===\n"
        f"{stdout}\n"
    )
    debug_path.write_text(debug_text, encoding="utf-8", errors="replace")
    return debug_path


def slugify(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", value.lower()).strip("_")


def parse_published_month(value: Any) -> int | None:
    published_dt = parse_published_date(value)
    if published_dt is None:
        return None
    return published_dt.month


def parse_published_day(value: Any) -> int | None:
    published_dt = parse_published_date(value)
    if published_dt is None:
        return None
    return published_dt.day


def parse_published_date(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    for fmt in ("%m/%d/%y", "%m/%d/%Y"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def parse_iso_date(value: str | None) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        return datetime.strptime(text, "%Y-%m-%d")
    except ValueError as exc:
        raise ValueError(f"Invalid ISO date '{value}'. Expected YYYY-MM-DD.") from exc


def is_restricted_url(url: str | None) -> bool:
    return "/restricted" in str(url or "").lower()


def compute_recent_window(
    *,
    published_days_back: int | None,
    as_of_date_text: str | None,
) -> tuple[datetime | None, datetime | None]:
    if published_days_back is None:
        return None, None
    if published_days_back < 1:
        raise ValueError("--published-days-back must be at least 1.")

    as_of_date = parse_iso_date(as_of_date_text) if as_of_date_text else datetime.now()
    as_of_date = as_of_date.replace(hour=0, minute=0, second=0, microsecond=0)
    window_start = as_of_date - timedelta(days=published_days_back - 1)
    return window_start, as_of_date


def build_section_url(section: str, year: int, page: int | None = None) -> str:
    base = f"{BASE_URL}/{section}/{year}"
    if page and page > 1:
        return f"{base}?{urlencode({'page': page})}"
    return base


def build_team_page_url(year: int, team_slug: str, page: int | None = None) -> str:
    base = f"{BASE_URL}/teams/{year}/{team_slug}"
    if page and page > 1:
        return f"{base}?{urlencode({'page': page})}"
    return base


def build_team_seed_url(year: int) -> str:
    return build_team_page_url(year, "arizona-cardinals")


def build_actual_results_url(year: int) -> str:
    return f"{BASE_URL}/nfl-draft-results-{year}"


def fetch_with_requests(url: str, timeout: int = 60) -> str:
    session = requests.Session()
    current_url = url
    seen_urls: set[str] = set()

    for _ in range(12):
        response = session.get(current_url, headers=DEFAULT_HEADERS, timeout=timeout, allow_redirects=False)
        if 300 <= response.status_code < 400:
            location = response.headers.get("Location")
            next_url = urljoin(current_url, location) if location else current_url
            if is_restricted_url(next_url):
                raise RestrictedRedirectError(f"Site redirected request to /restricted for {url}")
            if next_url in seen_urls:
                raise RuntimeError(f"Redirect loop detected for {url}: {next_url}")
            seen_urls.add(next_url)
            current_url = next_url
            continue

        if is_restricted_url(str(response.url)):
            raise RestrictedRedirectError(f"Site redirected request to /restricted for {url}")
        response.raise_for_status()
        return response.text

    raise RuntimeError(f"Too many redirects while fetching {url}")


def fetch_with_curl(url: str, timeout: int = 60) -> str:
    curl_path = shutil.which("curl")
    if curl_path is None:
        raise RuntimeError("curl is not available on PATH.")

    command = [
        curl_path,
        "--silent",
        "--show-error",
        "--location",
        "--compressed",
        "--http1.1",
        "--max-redirs",
        "10",
        "--connect-timeout",
        str(int(timeout)),
        "--max-time",
        str(int(timeout)),
        "--user-agent",
        USER_AGENT,
        "--header",
        f"Accept: {DEFAULT_HEADERS['Accept']}",
        "--header",
        f"Accept-Language: {DEFAULT_HEADERS['Accept-Language']}",
        url,
    ]
    result = subprocess.run(
        command,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip() or result.stdout.strip()
        if "Maximum (" in stderr and "redirects followed" in stderr:
            raise RestrictedRedirectError(f"Site redirected request to /restricted for {url}")
        raise RuntimeError(f"curl fetch failed for {url}: {stderr}")
    return result.stdout


def extract_embedded_json(text: str, prefix: str) -> dict[str, Any] | None:
    pattern = re.compile(re.escape(prefix) + r"(\{.*?\})(?:\r?\n|$)")
    match = pattern.search(text or "")
    if match is None:
        return None
    try:
        return json.loads(match.group(1))
    except json.JSONDecodeError:
        return None


def fetch_with_powershell(url: str, timeout: int = 60) -> str:
    if sys.platform != "win32":
        raise RuntimeError("PowerShell fallback is only available on Windows.")

    escaped_url = url.replace("'", "''")
    command = (
        "$ProgressPreference='SilentlyContinue'; "
        "$ErrorActionPreference='Stop'; "
        "[Console]::OutputEncoding=[System.Text.Encoding]::UTF8; "
        "try { "
        f"$resp=Invoke-WebRequest -UseBasicParsing -ErrorAction Stop -Uri '{escaped_url}' -MaximumRedirection 10 -TimeoutSec {int(timeout)}; "
        "$contentLength = if ($null -ne $resp.Content) { $resp.Content.Length } else { 0 }; "
        "$headers=@{}; "
        "foreach($key in $resp.Headers.Keys){ $headers[$key]=[string]$resp.Headers[$key]; }; "
        "$meta=[ordered]@{ "
        f"url='{escaped_url}'; "
        "status_code=([int]$resp.StatusCode); "
        "final_uri=([string]$resp.BaseResponse.ResponseUri.AbsoluteUri); "
        "content_length=$contentLength; "
        "headers=$headers "
        "}; "
        "[Console]::Error.WriteLine('__FETCH_META__' + ($meta | ConvertTo-Json -Compress -Depth 6)); "
        "$resp.Content"
        "} catch { "
        "$location = ''; "
        "$statusCode = $null; "
        "if ($_.Exception.Response) { "
        "  try { $location = [string]$_.Exception.Response.Headers['Location']; } catch { } "
        "  try { $statusCode = [int]$_.Exception.Response.StatusCode.value__; } catch { } "
        "} "
        "$errorMeta=[ordered]@{ message=$_.Exception.Message; location=$location; status_code=$statusCode }; "
        "[Console]::Error.WriteLine('__FETCH_ERROR__' + ($errorMeta | ConvertTo-Json -Compress -Depth 4)); "
        "throw "
        "}"
    )
    result = subprocess.run(
        ["powershell.exe", "-NoProfile", "-Command", command],
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip() or result.stdout.strip()
        error_meta = extract_embedded_json(result.stderr, "__FETCH_ERROR__")
        debug_path = write_fetch_debug_artifact(
            backend="powershell",
            url=url,
            stdout=result.stdout,
            stderr=result.stderr,
            returncode=result.returncode,
        )
        if error_meta and is_restricted_url(error_meta.get("location")):
            raise RestrictedRedirectError(
                f"Site redirected request to /restricted for {url} (debug saved to {debug_path})"
            )
        raise RuntimeError(f"PowerShell fetch failed for {url} (debug saved to {debug_path}): {stderr}")
    if not result.stdout.strip():
        fetch_meta = extract_embedded_json(result.stderr, "__FETCH_META__")
        debug_path = write_fetch_debug_artifact(
            backend="powershell",
            url=url,
            stdout=result.stdout,
            stderr=result.stderr,
            returncode=result.returncode,
        )
        if fetch_meta and is_restricted_url(fetch_meta.get("final_uri")):
            raise RestrictedRedirectError(
                f"Site redirected request to /restricted for {url} (debug saved to {debug_path})"
            )
        raise RuntimeError(f"PowerShell fetch returned an empty response for {url} (debug saved to {debug_path})")
    return result.stdout


def fetch_text(url: str, backend: str = "auto", timeout: int = 60) -> str:
    errors: list[str] = []

    if backend in {"auto", "requests"}:
        try:
            return fetch_with_requests(url, timeout=timeout)
        except RestrictedRedirectError:
            raise
        except Exception as exc:  # noqa: BLE001
            errors.append(f"requests: {exc}")
            if backend == "requests":
                raise

    if backend in {"auto", "curl"}:
        try:
            return fetch_with_curl(url, timeout=timeout)
        except RestrictedRedirectError:
            raise
        except Exception as exc:  # noqa: BLE001
            errors.append(f"curl: {exc}")
            if backend == "curl":
                raise

    if backend in {"auto", "powershell"}:
        try:
            return fetch_with_powershell(url, timeout=timeout)
        except RestrictedRedirectError:
            raise
        except Exception as exc:  # noqa: BLE001
            errors.append(f"powershell: {exc}")
            raise RuntimeError("; ".join(errors)) from exc

    raise RuntimeError("; ".join(errors) if errors else f"Unsupported backend: {backend}")


def save_text(path: Path, text: str) -> None:
    ensure_dir(path.parent)
    path.write_text(text, encoding="utf-8")


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


def has_react_props(html: str) -> bool:
    return REACT_PROPS_PATTERN.search(html) is not None


def load_cached_html_if_valid(path: Path, *, require_react_props: bool = True) -> str | None:
    if not path.exists():
        return None
    try:
        if path.stat().st_size <= 0:
            print(f"Ignoring empty cached HTML file: {path}", flush=True)
            return None
    except OSError:
        return None

    html = read_text(path)
    if not html.strip():
        print(f"Ignoring blank cached HTML file: {path}", flush=True)
        return None
    if require_react_props and not has_react_props(html):
        print(f"Ignoring cached HTML without React props: {path}", flush=True)
        return None
    return html


def summarize_nonreact_html(html: str) -> str:
    text = re.sub(r"\s+", " ", str(html or "")).strip()
    if not text:
        return "empty response body"
    lowered = text.lower()
    if "/restricted" in lowered or "restricted" in lowered:
        return "site returned a restricted page"
    if "access denied" in lowered or "forbidden" in lowered:
        return "site returned an access denied page"
    return text[:160]


def extract_react_props(html: str) -> tuple[dict[str, Any], str]:
    match = REACT_PROPS_PATTERN.search(html)
    if match is None:
        raise ValueError("Could not find embedded React props in the page HTML.")
    props = json.loads(unescape(match.group(1)))
    return props, match.group(2)


def team_slug_from_url(team_url: str | None) -> str | None:
    if not team_url:
        return None
    parts = [part for part in team_url.strip("/").split("/") if part]
    return parts[-1] if parts else None


def title_case_token(token: str) -> str:
    if token == "49ers":
        return "49ers"
    return token.capitalize()


def team_name_from_slug(team_slug: str | None) -> str | None:
    if not team_slug:
        return None
    return " ".join(title_case_token(token) for token in team_slug.split("-"))


def coerce_pick_number(pick_value: Any) -> int | None:
    if pick_value is None:
        return None
    if isinstance(pick_value, int):
        return pick_value
    if isinstance(pick_value, float):
        if pd.isna(pick_value):
            return None
        return int(pick_value)

    text = str(pick_value).strip()
    if not text:
        return None
    if text.isdigit():
        return int(text)
    return None


def infer_round_number(pick: int | None, round_value: Any, pick_label: Any = None) -> int | None:
    if round_value is not None:
        text = str(round_value).strip()
        if text.isdigit():
            return int(text)
    if pick_label is not None:
        text = str(pick_label).strip().upper()
        match = re.match(r"R(\d+)$", text)
        if match:
            return int(match.group(1))
    if pick is None:
        return None
    for round_number, round_end in enumerate(ROUND_ENDS, start=1):
        if pick <= round_end:
            return round_number
    return 7


def parse_selection(
    selection: dict[str, Any],
    *,
    year: int,
    section: str,
    mock_relative_url: str,
    mock_absolute_url: str,
    mock_name: str | None,
    author_name: str | None,
    published_at: str | None,
    external_url: str | None,
) -> dict[str, Any]:
    player = selection.get("player") or {}
    college = player.get("college") or {}
    team = selection.get("team") or {}
    team_url = team.get("url")
    team_slug = team_slug_from_url(team_url)
    pick_label = selection.get("pick")
    pick = coerce_pick_number(pick_label)

    return {
        "section": section,
        "year": year,
        "mock_relative_url": mock_relative_url,
        "mock_absolute_url": mock_absolute_url,
        "mock_name": mock_name,
        "author_name": author_name,
        "published_at": published_at,
        "external_url": external_url,
        "pick": pick,
        "pick_label": pick_label,
        "round_number": infer_round_number(pick, selection.get("round"), pick_label),
        "player_name": player.get("name"),
        "player_position": player.get("position"),
        "player_url": urljoin(BASE_URL, player.get("url") or ""),
        "college_name": college.get("name"),
        "college_url": urljoin(BASE_URL, college.get("url") or ""),
        "team_slug": team_slug,
        "team_name": team_name_from_slug(team_slug),
        "team_url": urljoin(BASE_URL, team_url or ""),
        "team_color": team.get("color"),
        "correct": selection.get("correct"),
        "traded": selection.get("traded"),
        "user_pick": selection.get("user"),
        "blurb": selection.get("blurb"),
        "value_pick": player.get("value_pick"),
        "reach_pick": player.get("reach_pick"),
        "rare_pick": player.get("rare_pick"),
        "in_mock_year": player.get("in_mock_year"),
    }


def parse_mock_page(
    html: str,
    *,
    year: int,
    section: str,
    mock_relative_url: str,
    mock_absolute_url: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    props, cache_id = extract_react_props(html)
    mock = props["mock"]

    metadata = {
        "section": section,
        "year": year,
        "cache_id": cache_id,
        "mock_relative_url": mock_relative_url,
        "mock_absolute_url": mock_absolute_url,
        "mock_name": mock.get("name"),
        "author_name": mock.get("author_name"),
        "published_at": mock.get("published_at"),
        "description": mock.get("description"),
        "intro": mock.get("intro"),
        "external_url": mock.get("external_url"),
        "rankings_url": urljoin(BASE_URL, mock.get("rankings_url") or ""),
        "completed_url": urljoin(BASE_URL, mock.get("completed_url") or ""),
        "completion_percentage": mock.get("completion_percentage"),
        "completion_place": mock.get("completion_place"),
        "most_accurate": mock.get("most_accurate"),
        "temporary_mock": mock.get("temporary_mock"),
        "scheduled_to_be_deleted": mock.get("scheduled_to_be_deleted"),
        "time_left_before_deletion": mock.get("time_left_before_deletion"),
        "top_authors_count": mock.get("top_authors_count"),
        "twitter": mock.get("twitter"),
        "user_mock": mock.get("user_mock"),
        "user_premium": mock.get("user_premium"),
        "selection_count": len(mock.get("selections") or []),
        "previous_mock_date_count": len(props.get("previous_mock_dates") or []),
    }

    picks = [
        parse_selection(
            selection,
            year=year,
            section=section,
            mock_relative_url=mock_relative_url,
            mock_absolute_url=mock_absolute_url,
            mock_name=metadata["mock_name"],
            author_name=metadata["author_name"],
            published_at=metadata["published_at"],
            external_url=metadata["external_url"],
        )
        for selection in mock.get("selections") or []
    ]
    return metadata, picks


def parse_index_page(html: str) -> tuple[list[dict[str, Any]], dict[str, Any], str]:
    props, cache_id = extract_react_props(html)
    return props.get("mocks") or [], props.get("pagination") or {}, cache_id


def parse_team_page(
    html: str,
) -> tuple[dict[str, Any], dict[str, Any], list[dict[str, Any]], dict[str, Any], str]:
    props, cache_id = extract_react_props(html)
    team = props.get("team") or {}
    base_props = props.get("base_props") or {}
    selections = team.get("selections") or []
    pagination = team.get("pagination") or {}
    return team, base_props, selections, pagination, cache_id


def build_author_dedupe_key(author_name: Any, mock_name: Any) -> str:
    author = str(author_name or "").strip()
    outlet = str(mock_name or "").strip()
    generic_authors = {"", "staff", "media", "editors", "editorial staff"}
    if author.lower() in generic_authors:
        return f"{author.lower()}::{outlet.lower()}"
    return author.lower()


def filter_items_by_date(
    items: list[dict[str, Any]],
    *,
    year: int | None,
    months: set[int] | None,
    day_min: int | None,
    date_from: datetime | None,
    date_to: datetime | None,
) -> list[dict[str, Any]]:
    if not months and day_min is None and date_from is None and date_to is None:
        return items
    filtered: list[dict[str, Any]] = []
    for item in items:
        published_dt = parse_published_date(item.get("published_at"))
        if published_dt is None:
            continue
        if year is not None and published_dt.year != year:
            continue
        if months and published_dt.month not in months:
            continue
        if day_min is not None and published_dt.day < day_min:
            continue
        if date_from is not None and published_dt < date_from:
            continue
        if date_to is not None and published_dt > date_to:
            continue
        filtered.append(item)
    return filtered


def dedupe_items_to_latest_author_mock(
    items: list[dict[str, Any]],
    *,
    extra_key_fields: tuple[str, ...] = (),
) -> list[dict[str, Any]]:
    ranked_items: list[dict[str, Any]] = []
    for index, item in enumerate(items, start=1):
        ranked_item = dict(item)
        ranked_item["_source_list_rank"] = index
        ranked_item["_published_dt"] = parse_published_date(item.get("published_at"))
        dedupe_key_parts = [
            build_author_dedupe_key(
                item.get("author_name"),
                item.get("name"),
            )
        ]
        for field_name in extra_key_fields:
            dedupe_key_parts.append(str(item.get(field_name) or "").strip().lower())
        ranked_item["_author_dedupe_key"] = "||".join(dedupe_key_parts)
        ranked_items.append(ranked_item)

    ranked_items.sort(
        key=lambda item: (
            item["_author_dedupe_key"],
            item["_published_dt"] is None,
            -(item["_published_dt"].timestamp()) if item["_published_dt"] is not None else 0.0,
            item["_source_list_rank"],
            str(item.get("url") or ""),
        )
    )

    kept_by_author: dict[str, dict[str, Any]] = {}
    for item in ranked_items:
        dedupe_key = item["_author_dedupe_key"]
        if dedupe_key not in kept_by_author:
            kept_by_author[dedupe_key] = item

    kept_items = sorted(
        kept_by_author.values(),
        key=lambda item: (
            item["_source_list_rank"],
            str(item.get("url") or ""),
        ),
    )

    cleaned_items: list[dict[str, Any]] = []
    for item in kept_items:
        cleaned = dict(item)
        cleaned.pop("_source_list_rank", None)
        cleaned.pop("_published_dt", None)
        cleaned.pop("_author_dedupe_key", None)
        cleaned_items.append(cleaned)
    return cleaned_items


def load_checkpoint(
    *,
    processed_dir: Path,
    year: int,
    section: str,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], set[str]]:
    year_dir = processed_dir / str(year)
    metadata_path = year_dir / f"{section}__mock_metadata.checkpoint.csv"
    picks_path = year_dir / f"{section}__mock_picks.checkpoint.csv"
    if not metadata_path.exists():
        return [], [], set()

    metadata_df = pd.read_csv(metadata_path)
    picks_df = pd.read_csv(picks_path) if picks_path.exists() else pd.DataFrame()
    done_urls = set(metadata_df.get("mock_relative_url", pd.Series(dtype=str)).dropna().astype(str))
    metadata_rows = metadata_df.to_dict(orient="records")
    pick_rows = picks_df.to_dict(orient="records") if not picks_df.empty else []
    return metadata_rows, pick_rows, done_urls


def write_outputs(
    *,
    metadata_rows: list[dict[str, Any]],
    pick_rows: list[dict[str, Any]],
    processed_dir: Path,
    year: int,
    section: str,
    checkpoint: bool,
    progress_payload: dict[str, Any] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    year_dir = processed_dir / str(year)
    ensure_dir(year_dir)

    metadata_df = pd.DataFrame(metadata_rows)
    picks_df = pd.DataFrame(pick_rows)

    suffix = ".checkpoint" if checkpoint else ""
    metadata_path = year_dir / f"{section}__mock_metadata{suffix}.csv"
    picks_path = year_dir / f"{section}__mock_picks{suffix}.csv"
    metadata_df.to_csv(metadata_path, index=False)
    picks_df.to_csv(picks_path, index=False)

    if progress_payload is not None:
        progress_path = year_dir / f"{section}__progress.json"
        progress_path.write_text(json.dumps(progress_payload, indent=2), encoding="utf-8")

    return metadata_df, picks_df


def fetch_index_items(
    *,
    year: int,
    section: str,
    backend: str,
    raw_dir: Path,
    max_pages: int,
    resume: bool,
    refresh_list_pages: bool,
) -> list[dict[str, Any]]:
    all_items: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    page = 1
    total_pages = 1

    while page <= total_pages and page <= max_pages:
        url = build_section_url(section, year, page=page)
        raw_path = raw_dir / str(year) / section / "index" / f"page_{page}.html"
        cached_html = load_cached_html_if_valid(raw_path)
        if resume and cached_html is not None and not refresh_list_pages:
            html = cached_html
            source = "reused"
        else:
            fetched_html = fetch_text(url, backend=backend)
            if has_react_props(fetched_html):
                html = fetched_html
                save_text(raw_path, html)
                source = "fetched"
            elif resume and cached_html is not None:
                html = cached_html
                source = "reused-fallback"
                print(
                    f"[index {page}] {section} {year}: fetched page did not contain React props "
                    f"({summarize_nonreact_html(fetched_html)}); falling back to cached page",
                    flush=True,
                )
            else:
                raise ValueError(
                    f"{section} {year} index page {page} did not contain React props. "
                    f"The site may have returned a restricted or blocked page: "
                    f"{summarize_nonreact_html(fetched_html)}"
                )

        items, pagination, _ = parse_index_page(html)
        total_pages = max(int(pagination.get("total_pages") or 1), total_pages)
        print(
            f"[index {page}/{min(total_pages, max_pages)}] {section} {year}: {source} page with {len(items)} list rows",
            flush=True,
        )

        for item in items:
            relative_url = item.get("url")
            if not relative_url or relative_url in seen_urls:
                continue
            seen_urls.add(relative_url)
            all_items.append(item)

        page += 1

    return all_items


def discover_team_slugs(
    *,
    year: int,
    backend: str,
    raw_dir: Path,
    resume: bool,
    refresh_list_pages: bool,
    explicit_team_slugs: list[str] | None,
) -> list[dict[str, Any]]:
    if explicit_team_slugs:
        return [
            {
                "url": f"/teams/{year}/{team_slug}",
                "slug": team_slug,
                "code": None,
                "city": team_name_from_slug(team_slug),
                "name": None,
            }
            for team_slug in explicit_team_slugs
        ]

    seed_url = build_team_seed_url(year)
    raw_path = raw_dir / str(year) / "teams" / "team_pages" / "seed__page_1.html"
    cached_html = load_cached_html_if_valid(raw_path)
    if resume and cached_html is not None and not refresh_list_pages:
        html = cached_html
        source = "reused"
    else:
        fetched_html = fetch_text(seed_url, backend=backend)
        if has_react_props(fetched_html):
            html = fetched_html
            save_text(raw_path, html)
            source = "fetched"
        elif resume and cached_html is not None:
            html = cached_html
            source = "reused-fallback"
            print(
                f"Seed page for teams {year} did not contain React props "
                f"({summarize_nonreact_html(fetched_html)}); falling back to cached seed page",
                flush=True,
            )
        else:
            raise ValueError(
                f"Teams seed page for {year} did not contain React props. "
                f"The site may have returned a restricted or blocked page: "
                f"{summarize_nonreact_html(fetched_html)}"
            )

    team, base_props, _, _, _ = parse_team_page(html)
    nav_teams = ((base_props.get("nav") or {}).get("teams") or [])
    if nav_teams:
        print(
            f"Discovered {len(nav_teams)} team pages for {year} from {source} seed page",
            flush=True,
        )
        return nav_teams

    fallback_slug = team_slug_from_url(team.get("url"))
    if fallback_slug is None:
        raise ValueError(f"Could not discover team pages for {year}.")
    print(
        f"Seed page did not include nav teams for {year}; falling back to {fallback_slug}",
        flush=True,
    )
    return [
        {
            "url": f"/teams/{year}/{fallback_slug}",
            "slug": fallback_slug,
            "code": team.get("code"),
            "city": team.get("city"),
            "name": team.get("name"),
        }
    ]


def build_team_selection_item(
    selection: dict[str, Any],
    *,
    year: int,
    team_slug: str,
    team_name: str | None,
    team_code: str | None,
    team_city: str | None,
    team_page_url: str,
    team_page_number: int,
    team_page_total_pages: int,
) -> dict[str, Any]:
    item = dict(selection)
    item["source_team_slug"] = team_slug
    item["source_team_name"] = team_name
    item["source_team_code"] = team_code
    item["source_team_city"] = team_city
    item["source_team_page_url"] = team_page_url
    item["source_team_page_number"] = team_page_number
    item["source_team_total_pages"] = team_page_total_pages
    item["source_team_year"] = year
    return item


def parse_team_consensus_rows(
    *,
    year: int,
    team_slug: str,
    team_name: str | None,
    team_code: str | None,
    team_city: str | None,
    team_page_url: str,
    consensus_picks: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for pick_entry in consensus_picks:
        pick_number = coerce_pick_number(pick_entry.get("pick"))
        bucket_data = pick_entry.get("data") or {}
        for window_name, players in bucket_data.items():
            for player_entry in players or []:
                rows.append(
                    {
                        "year": year,
                        "team_slug": team_slug,
                        "team_name": team_name,
                        "team_code": team_code,
                        "team_city": team_city,
                        "team_page_url": team_page_url,
                        "pick": pick_number,
                        "window_name": window_name,
                        "rank": player_entry.get("rank"),
                        "count": player_entry.get("count"),
                        "player_name": player_entry.get("name"),
                        "player_position": player_entry.get("position"),
                        "college_name": player_entry.get("college"),
                        "player_url": urljoin(BASE_URL, player_entry.get("player_url") or ""),
                    }
                )
    return rows


def fetch_team_index_items(
    *,
    year: int,
    backend: str,
    raw_dir: Path,
    max_pages: int,
    resume: bool,
    refresh_list_pages: bool,
    explicit_team_slugs: list[str] | None,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    team_entries = discover_team_slugs(
        year=year,
        backend=backend,
        raw_dir=raw_dir,
        resume=resume,
        refresh_list_pages=refresh_list_pages,
        explicit_team_slugs=explicit_team_slugs,
    )
    all_items: list[dict[str, Any]] = []
    team_rows: list[dict[str, Any]] = []
    consensus_rows: list[dict[str, Any]] = []
    seen_urls: set[str] = set()

    for team_entry in team_entries:
        team_url = str(team_entry.get("url") or "")
        team_slug = team_slug_from_url(team_url) or str(team_entry.get("slug") or "").strip()
        if not team_slug:
            continue
        absolute_team_url = urljoin(BASE_URL, team_url or f"/teams/{year}/{team_slug}")
        page = 1
        total_pages = 1
        first_page = True

        while page <= total_pages and page <= max_pages:
            url = build_team_page_url(year, team_slug, page=page)
            raw_path = raw_dir / str(year) / "teams" / "team_pages" / f"{team_slug}__page_{page}.html"
            cached_html = load_cached_html_if_valid(raw_path)
            if resume and cached_html is not None and not refresh_list_pages:
                html = cached_html
                source = "reused"
            else:
                fetched_html = fetch_text(url, backend=backend)
                if has_react_props(fetched_html):
                    html = fetched_html
                    save_text(raw_path, html)
                    source = "fetched"
                elif resume and cached_html is not None:
                    html = cached_html
                    source = "reused-fallback"
                    print(
                        f"[team {team_slug} page {page}] {year}: fetched page did not contain React props "
                        f"({summarize_nonreact_html(fetched_html)}); falling back to cached page",
                        flush=True,
                    )
                else:
                    raise ValueError(
                        f"teams {year} team page {team_slug} page {page} did not contain React props. "
                        f"The site may have returned a restricted or blocked page: "
                        f"{summarize_nonreact_html(fetched_html)}"
                    )

            team, _, selections, pagination, _ = parse_team_page(html)
            total_pages = max(int(pagination.get("total_pages") or 1), total_pages)
            team_name = " ".join(
                part for part in [team.get("city"), team.get("name")] if str(part or "").strip()
            ) or team_name_from_slug(team_slug)
            team_code = team.get("code") or team_entry.get("code")
            team_city = team.get("city") or team_entry.get("city")
            print(
                f"[team {team_slug} page {page}/{min(total_pages, max_pages)}] {year}: "
                f"{source} page with {len(selections)} list rows",
                flush=True,
            )

            if first_page:
                team_rows.append(
                    {
                        "year": year,
                        "team_slug": team_slug,
                        "team_name": team_name,
                        "team_code": team_code,
                        "team_city": team_city,
                        "team_page_url": absolute_team_url,
                        "team_needs": "|".join(str(need) for need in (team.get("team_needs") or [])),
                        "draft_pick_count": len(team.get("draft_picks") or []),
                        "team_page_total_pages": int(pagination.get("total_pages") or 1),
                        "team_page_total_count": int(pagination.get("total_count") or 0),
                    }
                )
                consensus_rows.extend(
                    parse_team_consensus_rows(
                        year=year,
                        team_slug=team_slug,
                        team_name=team_name,
                        team_code=team_code,
                        team_city=team_city,
                        team_page_url=absolute_team_url,
                        consensus_picks=team.get("consensus_picks") or [],
                    )
                )
                first_page = False

            for selection in selections:
                item = build_team_selection_item(
                    selection,
                    year=year,
                    team_slug=team_slug,
                    team_name=team_name,
                    team_code=team_code,
                    team_city=team_city,
                    team_page_url=absolute_team_url,
                    team_page_number=page,
                    team_page_total_pages=total_pages,
                )
                relative_url = item.get("url")
                if not relative_url or relative_url in seen_urls:
                    continue
                seen_urls.add(str(relative_url))
                all_items.append(item)

            page += 1

    return all_items, team_rows, consensus_rows


def relative_mock_url(item: dict[str, Any]) -> str:
    url = item.get("url")
    if not url:
        raise ValueError("Mock list item did not include a URL.")
    return str(url)


def normalize_input_mock_url(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        raise ValueError("Mock URL cannot be empty.")
    if text.startswith("/"):
        return text
    parsed = urlparse(text)
    if parsed.netloc and parsed.netloc.lower() not in {
        "www.nflmockdraftdatabase.com",
        "nflmockdraftdatabase.com",
    }:
        raise ValueError(f"Unsupported mock URL domain: {parsed.netloc}")
    relative = parsed.path or ""
    if parsed.query:
        relative = f"{relative}?{parsed.query}"
    if not relative.startswith("/"):
        relative = "/" + relative
    return relative


def infer_section_from_mock_url(relative_url: str) -> str:
    if relative_url.startswith("/mock-drafts/") or relative_url == "/mock-draft-2026":
        return "mock-drafts"
    if relative_url.startswith("/team-mock-drafts/"):
        return "team-mock-drafts"
    raise ValueError(f"Unsupported mock URL path: {relative_url}")


def infer_source_team_slug_from_team_mock_url(relative_url: str) -> str | None:
    parts = [part for part in relative_url.split("?")[0].strip("/").split("/") if part]
    if len(parts) < 3:
        return None
    article_slug = parts[2]
    prefix = article_slug.split("-")[0].strip().lower()
    return TEAM_ARTICLE_PREFIX_TO_SLUG.get(prefix)


def build_manual_mock_items(
    *,
    year: int,
    mock_urls: list[str],
) -> dict[str, list[dict[str, Any]]]:
    grouped_items: dict[str, list[dict[str, Any]]] = {"mock-drafts": [], "teams": []}
    seen_urls: set[str] = set()

    for input_url in mock_urls:
        relative_url = normalize_input_mock_url(input_url)
        if relative_url in seen_urls:
            continue
        seen_urls.add(relative_url)
        section = infer_section_from_mock_url(relative_url)
        item: dict[str, Any] = {"url": relative_url}
        if section == "teams":
            source_team_slug = infer_source_team_slug_from_team_mock_url(relative_url)
            if source_team_slug:
                item["source_team_slug"] = source_team_slug
                item["source_team_name"] = team_name_from_slug(source_team_slug)
                item["source_team_year"] = year
        grouped_items.setdefault(section, []).append(item)

    return grouped_items


def write_section_outputs(
    *,
    year: int,
    section: str,
    items: list[dict[str, Any]],
    backend: str,
    raw_dir: Path,
    processed_dir: Path,
    max_mocks: int | None,
    sleep_seconds: float,
    checkpoint_every: int,
    resume: bool,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if resume:
        metadata_rows, pick_rows, done_urls = load_checkpoint(
            processed_dir=processed_dir,
            year=year,
            section=section,
        )
    else:
        metadata_rows, pick_rows, done_urls = [], [], set()

    mock_items = items[: max_mocks or len(items)]
    mock_dir = raw_dir / str(year) / section / "mocks"
    ensure_dir(mock_dir)
    checkpoint_counter = 0
    fetched_count = 0
    reused_count = 0
    skipped_count = 0

    for index, item in enumerate(mock_items, start=1):
        relative_url = relative_mock_url(item)
        if relative_url in done_urls:
            skipped_count += 1
            if skipped_count <= 5 or skipped_count % 25 == 0:
                print(
                    f"[mock {index}/{len(mock_items)}] {section} {year}: skipped already checkpointed {relative_url}",
                    flush=True,
                )
            continue

        absolute_url = urljoin(BASE_URL, relative_url)
        file_stub = slugify(relative_url.strip("/"))
        mock_path = mock_dir / f"{file_stub}.html"
        cached_html = load_cached_html_if_valid(mock_path)
        if resume and cached_html is not None:
            html = cached_html
            fetch_state = "reused"
            reused_count += 1
        else:
            fetched_html = fetch_text(absolute_url, backend=backend)
            if has_react_props(fetched_html):
                html = fetched_html
                save_text(mock_path, html)
                fetch_state = "fetched"
                fetched_count += 1
            elif resume and cached_html is not None:
                html = cached_html
                fetch_state = "reused-fallback"
                reused_count += 1
                print(
                    f"[mock {index}/{len(mock_items)}] {section} {year}: fetched page did not contain React props "
                    f"for {relative_url} ({summarize_nonreact_html(fetched_html)}); falling back to cached page",
                    flush=True,
                )
            else:
                raise ValueError(
                    f"{section} {year} mock page did not contain React props for {relative_url}. "
                    f"The site may have returned a restricted or blocked page: "
                    f"{summarize_nonreact_html(fetched_html)}"
                )

        metadata, picks = parse_mock_page(
            html,
            year=year,
            section=section,
            mock_relative_url=relative_url,
            mock_absolute_url=absolute_url,
        )
        for field_name in (
            "source_team_slug",
            "source_team_name",
            "source_team_code",
            "source_team_city",
            "source_team_page_url",
            "source_team_page_number",
            "source_team_total_pages",
            "source_team_year",
        ):
            if field_name in item:
                metadata[field_name] = item.get(field_name)
                for pick_row in picks:
                    pick_row[field_name] = item.get(field_name)
        metadata["list_rank"] = index
        metadata_rows.append(metadata)
        pick_rows.extend(picks)
        done_urls.add(relative_url)
        checkpoint_counter += 1

        print(
            f"[mock {index}/{len(mock_items)}] {section} {year}: {fetch_state} {relative_url} "
            f"({len(metadata_rows)} parsed, {fetched_count} fetched, {reused_count} reused, {skipped_count} skipped)",
            flush=True,
        )

        if checkpoint_counter >= checkpoint_every:
            progress_payload = {
                "year": year,
                "section": section,
                "total_target_mocks": len(mock_items),
                "parsed_mocks": len(metadata_rows),
                "parsed_picks": len(pick_rows),
                "fetched_mock_pages": fetched_count,
                "reused_mock_pages": reused_count,
                "skipped_checkpointed_mocks": skipped_count,
                "last_mock_relative_url": relative_url,
                "checkpoint_written_at_unix": time.time(),
            }
            write_outputs(
                metadata_rows=metadata_rows,
                pick_rows=pick_rows,
                processed_dir=processed_dir,
                year=year,
                section=section,
                checkpoint=True,
                progress_payload=progress_payload,
            )
            print(
                f"  checkpoint saved for {section} {year}: {len(metadata_rows)} mocks, {len(pick_rows)} picks",
                flush=True,
            )
            checkpoint_counter = 0

        if sleep_seconds > 0:
            time.sleep(sleep_seconds)

    progress_payload = {
        "year": year,
        "section": section,
        "total_target_mocks": len(mock_items),
        "parsed_mocks": len(metadata_rows),
        "parsed_picks": len(pick_rows),
        "fetched_mock_pages": fetched_count,
        "reused_mock_pages": reused_count,
        "skipped_checkpointed_mocks": skipped_count,
        "checkpoint_written_at_unix": time.time(),
        "complete": True,
    }
    metadata_df, picks_df = write_outputs(
        metadata_rows=metadata_rows,
        pick_rows=pick_rows,
        processed_dir=processed_dir,
        year=year,
        section=section,
        checkpoint=False,
        progress_payload=progress_payload,
    )
    write_outputs(
        metadata_rows=metadata_rows,
        pick_rows=pick_rows,
        processed_dir=processed_dir,
        year=year,
        section=section,
        checkpoint=True,
        progress_payload=progress_payload,
    )
    return metadata_df, picks_df


def write_team_support_outputs(
    *,
    processed_dir: Path,
    year: int,
    team_rows: list[dict[str, Any]],
    consensus_rows: list[dict[str, Any]],
) -> None:
    year_dir = processed_dir / str(year)
    ensure_dir(year_dir)
    pd.DataFrame(team_rows).to_csv(year_dir / "teams__team_pages.csv", index=False)
    pd.DataFrame(consensus_rows).to_csv(year_dir / "teams__team_consensus.csv", index=False)


def scrape_actual_results(
    *,
    year: int,
    backend: str,
    raw_dir: Path,
    processed_dir: Path,
    resume: bool,
) -> pd.DataFrame:
    url = build_actual_results_url(year)
    raw_path = raw_dir / str(year) / "actual" / f"nfl_draft_results_{year}.html"
    cached_html = load_cached_html_if_valid(raw_path)
    if resume and cached_html is not None:
        html = cached_html
        print(f"Reused existing actual draft results page for {year}", flush=True)
    else:
        html = fetch_text(url, backend=backend)
        save_text(raw_path, html)
        print(f"Fetched actual draft results page for {year}", flush=True)

    props, _ = extract_react_props(html)
    mock = props["mock"]
    rows = [
        parse_selection(
            selection,
            year=year,
            section="actual-draft-results",
            mock_relative_url=mock.get("url") or f"/nfl-draft-results-{year}",
            mock_absolute_url=url,
            mock_name=mock.get("name"),
            author_name=mock.get("author_name"),
            published_at=mock.get("published_at"),
            external_url=mock.get("external_url"),
        )
        for selection in mock.get("selections") or []
    ]
    actual_df = pd.DataFrame(rows)
    ensure_dir(processed_dir / str(year))
    actual_df.to_csv(processed_dir / str(year) / f"actual_draft_results_{year}.csv", index=False)
    return actual_df


def main() -> None:
    args = parse_args()
    explicit_mock_urls = args.mock_urls or []
    manual_items_by_section = (
        build_manual_mock_items(year=args.year, mock_urls=explicit_mock_urls)
        if explicit_mock_urls
        else {}
    )
    sections = (
        [section for section in SUPPORTED_SECTIONS if manual_items_by_section.get(section)]
        if manual_items_by_section
        else (args.sections or list(SUPPORTED_SECTIONS))
    )
    target_months = set(args.published_months or [])
    rolling_date_from, rolling_date_to = compute_recent_window(
        published_days_back=args.published_days_back,
        as_of_date_text=args.as_of_date,
    )
    target_date_from = parse_iso_date(args.published_date_from) or rolling_date_from
    target_date_to = parse_iso_date(args.published_date_to) or rolling_date_to
    if target_date_from and target_date_to and target_date_from > target_date_to:
        raise ValueError("--published-date-from cannot be later than --published-date-to.")
    data_dir = Path(args.data_dir)
    raw_dir = data_dir / "raw"
    processed_dir = data_dir / "processed"

    for section in sections:
        print(f"Scraping {section} for {args.year}...")
        team_rows: list[dict[str, Any]] = []
        consensus_rows: list[dict[str, Any]] = []
        if manual_items_by_section:
            items = manual_items_by_section.get(section, [])
            list_count = len(items)
            date_filtered_count = len(items)
            print(
                f"Using {len(items)} manually provided mock URL(s) for {section} {args.year}",
                flush=True,
            )
        elif section == "teams":
            items, team_rows, consensus_rows = fetch_team_index_items(
                year=args.year,
                backend=args.fetch_backend,
                raw_dir=raw_dir,
                max_pages=args.max_pages,
                resume=args.resume,
                refresh_list_pages=args.refresh_list_pages,
                explicit_team_slugs=args.team_slugs,
            )
            write_team_support_outputs(
                processed_dir=processed_dir,
                year=args.year,
                team_rows=team_rows,
                consensus_rows=consensus_rows,
            )
            list_count = len(items)
        else:
            items = fetch_index_items(
                year=args.year,
                section=section,
                backend=args.fetch_backend,
                raw_dir=raw_dir,
                max_pages=args.max_pages,
                resume=args.resume,
                refresh_list_pages=args.refresh_list_pages,
            )
            list_count = len(items)
            items = filter_items_by_date(
                items,
                year=args.year,
                months=target_months if target_months else None,
                day_min=args.published_day_min,
                date_from=target_date_from,
                date_to=target_date_to,
            )
            if target_months or args.published_day_min is not None or target_date_from or target_date_to:
                filter_parts: list[str] = []
                if target_months:
                    month_label = ",".join(str(month) for month in sorted(target_months))
                    filter_parts.append(f"month(s): {month_label}")
                if args.published_day_min is not None:
                    filter_parts.append(f"day >= {args.published_day_min}")
                if target_date_from is not None:
                    filter_parts.append(f"date >= {target_date_from.strftime('%Y-%m-%d')}")
                if target_date_to is not None:
                    filter_parts.append(f"date <= {target_date_to.strftime('%Y-%m-%d')}")
                if args.published_days_back is not None:
                    filter_parts.append(f"rolling window: last {args.published_days_back} day(s)")
                print(
                    f"Filtered {section} {args.year} list items from {list_count} to {len(items)} "
                    f"for published {' and '.join(filter_parts)}",
                    flush=True,
                )
            date_filtered_count = len(items)
        if args.latest_author_mock_only and not manual_items_by_section:
            before_dedupe_count = len(items)
            dedupe_kwargs: dict[str, Any] = {}
            if section == "teams":
                dedupe_kwargs["extra_key_fields"] = ("source_team_slug",)
            items = dedupe_items_to_latest_author_mock(items, **dedupe_kwargs)
            print(
                f"Deduped {section} {args.year} list items from {before_dedupe_count} to {len(items)} "
                f"using the latest mock per author",
                flush=True,
            )
        metadata_df, picks_df = write_section_outputs(
            year=args.year,
            section=section,
            items=items,
            backend=args.fetch_backend,
            raw_dir=raw_dir,
            processed_dir=processed_dir,
            max_mocks=args.max_mocks,
            sleep_seconds=args.sleep_seconds,
            checkpoint_every=max(args.checkpoint_every, 1),
            resume=args.resume,
        )
        print(
            f"  completed {section} {args.year}: {len(metadata_df)} mock pages and {len(picks_df)} picks "
            f"(list rows: {list_count}, after date filter: {date_filtered_count}, final targets: {len(items)})",
            flush=True,
        )

    if args.include_actual_results:
        try:
            actual_df = scrape_actual_results(
                year=args.year,
                backend=args.fetch_backend,
                raw_dir=raw_dir,
                processed_dir=processed_dir,
                resume=args.resume,
            )
            print(f"Scraped actual draft results: {len(actual_df)} picks")
        except Exception as exc:  # noqa: BLE001
            print(f"Could not scrape actual draft results for {args.year}: {exc}")


if __name__ == "__main__":
    main()
