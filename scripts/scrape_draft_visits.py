#!/usr/bin/env python
from __future__ import annotations

import argparse
import math
import re
from collections import Counter
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from time import sleep
from typing import Iterable

import pandas as pd
import requests
from bs4 import BeautifulSoup
from bs4 import NavigableString
from bs4 import Tag


USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36"
)
DEFAULT_HEADERS = {
    "User-Agent": USER_AGENT,
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
WALTER_SOURCE = "walterfootball"
NTR_SOURCE = "nfltraderumors"
DRAFTCOUNTDOWN_SOURCE = "draftcountdown"
SOURCE_PRIORITY = {
    WALTER_SOURCE: 1,
    NTR_SOURCE: 2,
    DRAFTCOUNTDOWN_SOURCE: 3,
}
TEAM_SLUG_TO_NAME = {
    "arizona-cardinals": "Arizona Cardinals",
    "atlanta-falcons": "Atlanta Falcons",
    "baltimore-ravens": "Baltimore Ravens",
    "buffalo-bills": "Buffalo Bills",
    "carolina-panthers": "Carolina Panthers",
    "chicago-bears": "Chicago Bears",
    "cincinnati-bengals": "Cincinnati Bengals",
    "cleveland-browns": "Cleveland Browns",
    "dallas-cowboys": "Dallas Cowboys",
    "denver-broncos": "Denver Broncos",
    "detroit-lions": "Detroit Lions",
    "green-bay-packers": "Green Bay Packers",
    "houston-texans": "Houston Texans",
    "indianapolis-colts": "Indianapolis Colts",
    "jacksonville-jaguars": "Jacksonville Jaguars",
    "kansas-city-chiefs": "Kansas City Chiefs",
    "las-vegas-raiders": "Las Vegas Raiders",
    "los-angeles-chargers": "Los Angeles Chargers",
    "los-angeles-rams": "Los Angeles Rams",
    "miami-dolphins": "Miami Dolphins",
    "minnesota-vikings": "Minnesota Vikings",
    "new-england-patriots": "New England Patriots",
    "new-orleans-saints": "New Orleans Saints",
    "new-york-giants": "New York Giants",
    "new-york-jets": "New York Jets",
    "philadelphia-eagles": "Philadelphia Eagles",
    "pittsburgh-steelers": "Pittsburgh Steelers",
    "san-francisco-49ers": "San Francisco 49ers",
    "seattle-seahawks": "Seattle Seahawks",
    "tampa-bay-buccaneers": "Tampa Bay Buccaneers",
    "tennessee-titans": "Tennessee Titans",
    "washington-commanders": "Washington Commanders",
}
TEAM_LABEL_TO_SLUG = {
    "49ers": "san-francisco-49ers",
    "Arizona Cardinals": "arizona-cardinals",
    "Atlanta Falcons": "atlanta-falcons",
    "Baltimore Ravens": "baltimore-ravens",
    "Bears": "chicago-bears",
    "Bengals": "cincinnati-bengals",
    "Bills": "buffalo-bills",
    "Broncos": "denver-broncos",
    "Browns": "cleveland-browns",
    "Buccaneers": "tampa-bay-buccaneers",
    "Buffalo Bills": "buffalo-bills",
    "Cardinals": "arizona-cardinals",
    "Chargers": "los-angeles-chargers",
    "Chiefs": "kansas-city-chiefs",
    "Colts": "indianapolis-colts",
    "Commanders": "washington-commanders",
    "Cowboys": "dallas-cowboys",
    "Dolphins": "miami-dolphins",
    "Eagles": "philadelphia-eagles",
    "Falcons": "atlanta-falcons",
    "Giants": "new-york-giants",
    "Jaguars": "jacksonville-jaguars",
    "Jets": "new-york-jets",
    "Lions": "detroit-lions",
    "Packers": "green-bay-packers",
    "Panthers": "carolina-panthers",
    "Patriots": "new-england-patriots",
    "Raiders": "las-vegas-raiders",
    "Rams": "los-angeles-rams",
    "Ravens": "baltimore-ravens",
    "Redskins": "washington-commanders",
    "Saints": "new-orleans-saints",
    "Seahawks": "seattle-seahawks",
    "Steelers": "pittsburgh-steelers",
    "Texans": "houston-texans",
    "Titans": "tennessee-titans",
    "Vikings": "minnesota-vikings",
    "Washington": "washington-commanders",
}
TEAM_ABBR_TO_SLUG = {
    "ARI": "arizona-cardinals",
    "ATL": "atlanta-falcons",
    "BAL": "baltimore-ravens",
    "BUF": "buffalo-bills",
    "CAR": "carolina-panthers",
    "CHI": "chicago-bears",
    "CIN": "cincinnati-bengals",
    "CLE": "cleveland-browns",
    "DAL": "dallas-cowboys",
    "DEN": "denver-broncos",
    "DET": "detroit-lions",
    "GB": "green-bay-packers",
    "HOU": "houston-texans",
    "IND": "indianapolis-colts",
    "JAC": "jacksonville-jaguars",
    "KC": "kansas-city-chiefs",
    "LV": "las-vegas-raiders",
    "LAC": "los-angeles-chargers",
    "LAR": "los-angeles-rams",
    "MIA": "miami-dolphins",
    "MIN": "minnesota-vikings",
    "NE": "new-england-patriots",
    "NO": "new-orleans-saints",
    "NYG": "new-york-giants",
    "NYJ": "new-york-jets",
    "PHI": "philadelphia-eagles",
    "PIT": "pittsburgh-steelers",
    "SF": "san-francisco-49ers",
    "SEA": "seattle-seahawks",
    "TB": "tampa-bay-buccaneers",
    "TEN": "tennessee-titans",
    "WAS": "washington-commanders",
}
NTR_URLS = {
    2026: "https://nfltraderumors.co/2026-nfl-draft-visit-tracker/",
    2025: "https://nfltraderumors.co/2025-nfl-draft-visit-tracker/",
    2024: "https://nfltraderumors.co/2024-nfl-draft-prospect-visit-tracker/",
    2023: "https://nfltraderumors.co/2023-nfl-draft-visit-tracker/",
    2022: "https://nfltraderumors.co/2022-nfl-draft-visits-tracker/",
    2021: "https://nfltraderumors.co/2021-nfl-draft-prospect-meeting-tracker/",
    2020: "https://nfltraderumors.co/2020-nfl-draft-prospect-visit-tracker/",
}
WALTER_CODE_MAP = {
    "SR": "senior_bowl_meeting",
    "EW": "shrine_bowl_meeting",
    "COM": "combine_meeting",
    "INT": "interest",
    "VINT": "interest",
    "PRO": "pro_day_or_campus_meeting_workout",
    "LOC": "local_visit",
    "T30": "top_30_visit",
    "WOR": "private_workout",
    "STM": "meeting_unspecified",
    "VIR": "virtual_meeting",
}
VISIT_TYPE_ORDER = [
    "top_30_visit",
    "local_visit",
    "virtual_meeting",
    "combine_meeting",
    "senior_bowl_meeting",
    "shrine_bowl_meeting",
    "pro_day_or_campus_meeting_workout",
    "pro_day_meeting",
    "private_workout",
    "private_meeting",
    "general_visit",
    "meeting_unspecified",
    "interest",
]
STATUS_ORDER = ["reported", "scheduled", "interest", "unconfirmed"]
PLAYER_NAME_ALIAS_MAP = {
    "Caeden Wallace": "Caedan Wallace",
    "Cade Otten": "Cade Otton",
    "Cameron Taylor-Britt": "Cam Taylor-Britt",
    "Cam Johnston": "Cam Johnson",
    "Carson Schwesinge": "Carson Schwesinger",
    "Chig Anusiem": "Chigozie Anusiem",
    "Chris Oladuokun": "Chris Oladokun",
    "Christian Braswell": "Chris Braswell",
    "Donovan McMillan": "Donovan McMillon",
    "Jalen Wydermye": "Jalen Wydermyer",
    "Jayson Carlies": "Jaylon Carlies",
    "Jaylin Carlies": "Jaylon Carlies",
    "Jayden Ott": "Jaydn Ott",
    "Joe Vaughn": "Joseph Vaughn",
    "Kayden Proctor": "Kadyn Proctor",
    "Marist Liufao": "Marist Liufau",
    "Markquese Bell": "Markquese Bell",
    "Marquese Bell": "Markquese Bell",
    "Matt Hibner": "Matthew Hibner",
    "Max Iheanaschor": "Max Iheanachor",
    "Max Iheanschor": "Max Iheanachor",
    "Monray Baldwin": "Monaray Baldwin",
    "Nasir Green": "Nasir Greer",
    "Omar Norman-Lott": "Omarr Norman-Lott",
    "Ruke Orhohoro": "Ruke Orhorhoro",
    "Troy Anderson": "Troy Andersen",
}
SCHOOL_ALIAS_MAP = {
    "Miami (FL)": "Miami",
    "N.C. State": "NC State",
}


@dataclass
class FetchResult:
    requested_url: str
    final_url: str | None
    status_code: int | None
    html: str | None
    available: bool
    availability_note: str


POSITION_CANONICAL_MAP = {
    "ATH": "ATH",
    "C": "C",
    "CB": "CB",
    "CORNERBACK": "CB",
    "DB": "DB",
    "DE": "DE",
    "DEFENSIVE BACK": "DB",
    "DEFENSIVE END": "DE",
    "DEFENSIVE LINE": "DL",
    "DEFENSIVE LINEMAN": "DL",
    "DEFENSIVE TACKLE": "DT",
    "DL": "DL",
    "DT": "DT",
    "EDGE": "EDGE",
    "FB": "FB",
    "FREE SAFETY": "S",
    "FS": "S",
    "FULLBACK": "FB",
    "G": "G",
    "GUARD": "G",
    "IOL": "IOL",
    "IDL": "IDL",
    "ILB": "LB",
    "INSIDE LINEBACKER": "LB",
    "INTERIOR OFFENSIVE LINE": "IOL",
    "K": "K",
    "KICKER": "K",
    "LB": "LB",
    "LG": "G",
    "LINEBACKER": "LB",
    "LONG SNAPPER": "LS",
    "LS": "LS",
    "LT": "OT",
    "MLB": "LB",
    "NT": "DT",
    "OC": "C",
    "OFFENSIVE GUARD": "G",
    "OFFENSIVE LINE": "OL",
    "OFFENSIVE LINEMAN": "OL",
    "OFFENSIVE TACKLE": "OT",
    "OG": "G",
    "OL": "OL",
    "OLB": "LB",
    "OT": "OT",
    "OUTSIDE LINEBACKER": "LB",
    "P": "P",
    "PUNTER": "P",
    "QB": "QB",
    "QUARTERBACK": "QB",
    "RB": "RB",
    "RG": "G",
    "RT": "OT",
    "RUNNING BACK": "RB",
    "S": "S",
    "SAF": "S",
    "SAFETY": "S",
    "SS": "S",
    "STRONG SAFETY": "S",
    "T": "OT",
    "TE": "TE",
    "TIGHT END": "TE",
    "WIDE RECEIVER": "WR",
    "WR": "WR",
}
POSITION_SEQUENCES = sorted(
    {tuple(position.split()) for position in POSITION_CANONICAL_MAP if "/" not in position},
    key=lambda parts: (-len(parts), parts),
)
PLAYER_NAME_RE = re.compile(r"[A-Za-z]")
PLAIN_TEXT_PLAYER_RE = re.compile(
    r"(?P<school>[A-Z0-9][A-Za-z0-9.&'()/ -]{1,60}?)\s+"
    r"(?P<position>QB|RB|WR|TE|OT|LT|RT|OL|OG|G|C|OC|IOL|DL|DT|DE|EDGE|NT|LB|ILB|MLB|OLB|CB|S|FS|SS|DB|K|P|LS|FB|ATH)\s+"
    r"(?P<name>[A-Z][A-Za-z.'’-]+(?:\s+[A-Z][A-Za-z.'’-]+){1,4})"
)
FILLER_TOKENS = {
    "already",
    "a",
    "an",
    "and",
    "around",
    "are",
    "as",
    "at",
    "bringing",
    "confirmed",
    "for",
    "from",
    "had",
    "has",
    "have",
    "hosted",
    "hosting",
    "in",
    "include",
    "includes",
    "including",
    "is",
    "of",
    "on",
    "or",
    "out",
    "report",
    "reporting",
    "reports",
    "said",
    "says",
    "set",
    "that",
    "the",
    "their",
    "tracked",
    "to",
    "upcoming",
    "visit",
    "visits",
    "with",
    "will",
    "worked",
}


def parse_args() -> argparse.Namespace:
    current_year = date.today().year
    parser = argparse.ArgumentParser(
        description=(
            "Scrape NFL draft prospect visit data from WalterFootball, NFLTradeRumors, "
            "and Draft Countdown, then cross-reference missing fields across sources."
        )
    )
    parser.add_argument(
        "--start-year",
        type=int,
        default=current_year - 9,
        help="First draft year to attempt. Defaults to a rolling 10-year window.",
    )
    parser.add_argument(
        "--end-year",
        type=int,
        default=current_year,
        help="Last draft year to attempt. Defaults to the current year.",
    )
    parser.add_argument(
        "--current-year",
        type=int,
        default=current_year,
        help="Season to snapshot separately as the current cycle output for app consumption.",
    )
    parser.add_argument(
        "--data-dir",
        default="data",
        help="Base data directory. Raw HTML goes under data/raw and CSVs under data/processed.",
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Refetch source pages even if cached raw HTML already exists.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=45,
        help="HTTP timeout in seconds per page.",
    )
    parser.add_argument(
        "--sleep-seconds",
        type=float,
        default=0.0,
        help="Optional pause between network requests.",
    )
    parser.add_argument(
        "--replace-existing-outputs",
        action="store_true",
        help=(
            "Overwrite processed outputs with only the requested year range. "
            "By default, the scraper preserves previously scraped years outside the requested window."
        ),
    )
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def normalize_space(value: str | None) -> str:
    if value is None:
        return ""
    return " ".join(str(value).replace("\xa0", " ").split())


def canonicalize_player_name(value: str | None) -> str | None:
    text = normalize_space(value)
    if not text:
        return None
    text = text.replace("\u2019", "'").replace("\u2018", "'").replace("`", "'")
    text = text.replace("\u2013", "-").replace("\u2014", "-")
    text = re.sub(r"[%^#]+", "", text)
    text = re.sub(r"'s$", "", text, flags=re.I)
    text = re.sub(r"\.$", "", text)
    text = re.sub(r"\s+[A-Za-z]$", "", text)
    text = normalize_space(text.strip(" ,;:"))
    return PLAYER_NAME_ALIAS_MAP.get(text, text) or None


def canonicalize_school_name(value: str | None) -> str | None:
    text = normalize_space(value)
    if not text:
        return None
    text = text.replace("\u2019", "'").replace("\u2018", "'")
    text = SCHOOL_ALIAS_MAP.get(text, text)
    return text or None


def normalize_name(value: str | None) -> str | None:
    text = canonicalize_player_name(value)
    if not text:
        return None
    parts = re.findall(r"[A-Za-z0-9]+", text.lower())
    while parts and parts[-1] in {"jr", "sr", "ii", "iii", "iv", "v"}:
        parts.pop()
    while parts and len(parts[-1]) == 1 and len(parts) >= 3:
        parts.pop()
    return "".join(parts) or None


def normalize_school(value: str | None) -> str | None:
    text = canonicalize_school_name(value)
    return text or None


def normalize_position(value: str | None) -> str | None:
    text = normalize_space(value)
    if not text:
        return None
    return POSITION_CANONICAL_MAP.get(text.upper(), text.upper())


def team_name_from_slug(team_slug: str | None) -> str | None:
    if not team_slug:
        return None
    return TEAM_SLUG_TO_NAME.get(team_slug)


def team_slug_from_label(label: str | None) -> str | None:
    text = normalize_space(label)
    if not text:
        return None
    if text in TEAM_LABEL_TO_SLUG:
        return TEAM_LABEL_TO_SLUG[text]
    for team_slug, team_name in TEAM_SLUG_TO_NAME.items():
        if text == team_name:
            return team_slug
    return None


def parse_requested_url(source: str, year: int) -> str | None:
    if source == WALTER_SOURCE:
        return f"https://walterfootball.com/ProspectMeetingsByTeam{year}.php"
    if source == NTR_SOURCE:
        return NTR_URLS.get(year)
    if source == DRAFTCOUNTDOWN_SOURCE:
        return f"https://www.draftcountdown.com/{year}-nfl-draft/visits/top-30-visit-tracker/"
    raise ValueError(f"Unsupported source: {source}")


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


def fetch_html(url: str, *, timeout: int) -> FetchResult:
    try:
        response = requests.get(url, headers=DEFAULT_HEADERS, timeout=timeout, allow_redirects=True)
        response.encoding = response.apparent_encoding or response.encoding or "utf-8"
        html = response.text
        available = response.status_code == 200
        note = "ok" if available else f"http_{response.status_code}"
        return FetchResult(
            requested_url=url,
            final_url=str(response.url),
            status_code=response.status_code,
            html=html,
            available=available,
            availability_note=note,
        )
    except requests.RequestException as exc:
        return FetchResult(
            requested_url=url,
            final_url=None,
            status_code=None,
            html=None,
            available=False,
            availability_note=f"request_error: {exc}",
        )


def load_or_fetch_page(
    *,
    source: str,
    year: int,
    raw_dir: Path,
    refresh: bool,
    timeout: int,
) -> FetchResult:
    requested_url = parse_requested_url(source, year)
    if requested_url is None:
        return FetchResult("", None, None, None, False, "no_known_url_for_year")

    raw_path = raw_dir / str(year) / f"{source}.html"
    if raw_path.exists() and not refresh:
        return FetchResult(requested_url, requested_url, 200, read_text(raw_path), True, "cached")

    result = fetch_html(requested_url, timeout=timeout)
    if result.available and result.html:
        ensure_dir(raw_path.parent)
        raw_path.write_text(result.html, encoding="utf-8")
    return result


def walter_page_available(year: int, fetch_result: FetchResult) -> tuple[bool, str]:
    if not fetch_result.available or not fetch_result.html:
        return False, fetch_result.availability_note
    final_url = fetch_result.final_url or ""
    if f"ProspectMeetingsByTeam{year}.php" not in final_url:
        return False, f"unexpected_final_url: {final_url}"
    if f"{year} NFL Draft Prospect Visits Tracker" not in fetch_result.html:
        return False, "expected_walter_title_not_found"
    return True, fetch_result.availability_note


def ntr_page_available(year: int, fetch_result: FetchResult) -> tuple[bool, str]:
    if not fetch_result.available or not fetch_result.html:
        return False, fetch_result.availability_note
    expected_url = NTR_URLS.get(year)
    final_url = fetch_result.final_url or ""
    if expected_url and final_url.rstrip("/") != expected_url.rstrip("/"):
        return False, f"unexpected_final_url: {final_url}"
    if f"{year} NFL Draft" not in fetch_result.html:
        return False, "expected_ntr_year_marker_not_found"
    return True, fetch_result.availability_note


def draftcountdown_page_available(year: int, fetch_result: FetchResult) -> tuple[bool, str]:
    if not fetch_result.available or not fetch_result.html:
        return False, fetch_result.availability_note
    final_url = (fetch_result.final_url or "").rstrip("/")
    if year != 2022:
        if "/2022-nfl-draft/" in final_url:
            return False, "redirected_to_2022_page"
        return False, f"unexpected_final_url: {final_url}"
    if "/2022-nfl-draft/" not in final_url:
        return False, f"unexpected_final_url: {final_url}"
    if "Top 30 Visit Tracker" not in fetch_result.html:
        return False, "expected_draftcountdown_marker_not_found"
    return True, fetch_result.availability_note


def sort_values(values: Iterable[str], order: list[str] | None = None) -> list[str]:
    unique = [normalize_space(value) for value in values if normalize_space(value)]
    if not unique:
        return []
    deduped = sorted(set(unique))
    if not order:
        return deduped
    order_map = {value: idx for idx, value in enumerate(order)}
    return sorted(deduped, key=lambda value: (order_map.get(value, math.inf), value))


def join_pipe(values: Iterable[str], order: list[str] | None = None) -> str | None:
    ordered = sort_values(values, order=order)
    return "|".join(ordered) if ordered else None


def choose_preferred_text(values: Iterable[str]) -> str | None:
    cleaned = [normalize_space(value) for value in values if normalize_space(value)]
    if not cleaned:
        return None
    counts = Counter(cleaned)
    return sorted(counts.items(), key=lambda item: (-item[1], -len(item[0]), item[0]))[0][0]


def row_text_before_tag(root: Tag, target: Tag) -> str:
    parts: list[str] = []
    for descendant in root.descendants:
        if descendant is target:
            break
        if isinstance(descendant, NavigableString):
            parts.append(str(descendant))
    return normalize_space("".join(parts))


def looks_like_person_name(value: str | None) -> bool:
    text = normalize_space(value)
    if not text or not PLAYER_NAME_RE.search(text):
        return False
    tokens = text.split()
    if len(tokens) < 2 or len(tokens) > 6:
        return False
    if any(token.lower() in {"coach", "gm", "hc"} for token in tokens):
        return False
    return True


def parse_school_position_from_prefix(prefix_text: str) -> tuple[str | None, str | None]:
    text = normalize_space(prefix_text).strip(" ,;:-")
    if not text:
        return None, None
    clause = re.split(r"[.;:]", text)[-1].strip()
    if "," in clause:
        clause = clause.split(",")[-1].strip()
    tokens = clause.split()
    if not tokens:
        return None, None

    for position_parts in POSITION_SEQUENCES:
        if len(tokens) < len(position_parts):
            continue
        tail = tuple(token.upper() for token in tokens[-len(position_parts) :])
        if tail != position_parts:
            continue
        school_tokens = tokens[: -len(position_parts)]
        if not school_tokens:
            continue
        cut_index = 0
        for idx, token in enumerate(school_tokens):
            normalized = token.lower().strip("()[]{}'’\"")
            if normalized in FILLER_TOKENS:
                cut_index = idx + 1
        school_tokens = school_tokens[cut_index:]
        if not school_tokens:
            continue
        school = normalize_school(" ".join(school_tokens))
        raw_position = " ".join(position_parts)
        return school, normalize_position(raw_position)
    return None, None


def infer_ntr_visit_types(text: str, section_label: str | None) -> list[str]:
    lowered = normalize_space(text).lower()
    section_lower = normalize_space(section_label).lower()
    visit_types: set[str] = set()

    if "30 visit" in lowered or "top 30" in lowered or "official 30" in lowered:
        visit_types.add("top_30_visit")
    if "combine" in lowered:
        visit_types.add("combine_meeting")
    if "virtual" in lowered or "zoom" in lowered:
        visit_types.add("virtual_meeting")
    if "senior bowl" in lowered:
        visit_types.add("senior_bowl_meeting")
    if "shrine bowl" in lowered or "east-west shrine" in lowered:
        visit_types.add("shrine_bowl_meeting")
    if "pro day" in lowered:
        visit_types.add("pro_day_meeting")
    if "private workout" in lowered or "worked out" in lowered or "workout with" in lowered or " for a workout" in lowered:
        visit_types.add("private_workout")
    if "private meeting" in lowered or "in-person meeting" in lowered:
        visit_types.add("private_meeting")
    if "local prospect day" in lowered or "local visit" in lowered or "local prospect" in lowered:
        visit_types.add("local_visit")
    if "interested in hosting" in lowered:
        visit_types.add("interest")

    if "30 visit" in section_lower:
        visit_types.add("top_30_visit")
    if "combine" in section_lower:
        visit_types.add("combine_meeting")
    if "virtual" in section_lower:
        visit_types.add("virtual_meeting")
    if "senior bowl" in section_lower:
        visit_types.add("senior_bowl_meeting")
    if "shrine" in section_lower:
        visit_types.add("shrine_bowl_meeting")
    if "local" in section_lower:
        visit_types.add("local_visit")
    if "pro day" in section_lower:
        visit_types.add("pro_day_meeting")
        if "private" in section_lower or "workout" in section_lower:
            visit_types.add("private_workout")

    if not visit_types and "visit" in lowered:
        visit_types.add("general_visit")
    if not visit_types and ("interview" in lowered or "meeting" in lowered or "met " in lowered):
        visit_types.add("meeting_unspecified")
    return sort_values(visit_types, order=VISIT_TYPE_ORDER)


def infer_ntr_statuses(text: str) -> list[str]:
    lowered = normalize_space(text).lower()
    statuses: set[str] = set()
    if "unable to confirm" in lowered or "could not confirm" in lowered or "unconfirmed" in lowered:
        statuses.add("unconfirmed")
    if "interested in hosting" in lowered:
        statuses.add("interest")
    if any(
        marker in lowered
        for marker in (
            "will take",
            "will visit",
            "scheduled",
            "set up",
            "lined up",
            "coming up",
            "upcoming",
            "will have",
            "has a visit",
            "will also meet",
        )
    ):
        statuses.add("scheduled")
    if any(
        marker in lowered
        for marker in (
            "visited",
            "took a visit",
            "had a visit",
            "hosted",
            "met with",
            "met formally",
            "formal interview",
            "private workout",
            "worked out",
            "interview with",
            "met at the combine",
            "met around his pro day",
            "had coaches at",
            "confirmed he had",
            "said he visited",
        )
    ):
        statuses.add("reported")
    if not statuses:
        statuses.add("reported")
    return sort_values(statuses, order=STATUS_ORDER)


def normalize_ntr_section_label(tag: Tag) -> str | None:
    text = normalize_space(tag.get_text(" ", strip=True))
    lowered = text.lower()
    if tag.name != "p" or len(text) > 120 or tag.find("strong") is None:
        return None
    markers = ("30 visit", "combine", "virtual", "pro day", "private", "senior bowl", "shrine", "local")
    return text if any(marker in lowered for marker in markers) else None


def parse_ntr_plain_text_candidates(text: str) -> list[tuple[str, str | None, str | None]]:
    candidates: list[tuple[str, str | None, str | None]] = []
    for match in PLAIN_TEXT_PLAYER_RE.finditer(text):
        name = canonicalize_player_name(match.group("name"))
        school = normalize_school(match.group("school"))
        position = normalize_position(match.group("position"))
        if looks_like_person_name(name):
            candidates.append((name, school, position))
    return candidates


def parse_ntr_li(
    *,
    year: int,
    team_slug: str,
    section_label: str | None,
    source_url: str,
    li: Tag,
) -> list[dict[str, object]]:
    bullet_text = normalize_space(li.get_text(" ", strip=True))
    visit_types = infer_ntr_visit_types(bullet_text, section_label)
    statuses = infer_ntr_statuses(bullet_text)
    records: list[dict[str, object]] = []
    seen_names: set[str] = set()

    strong_tags = [strong for strong in li.find_all("strong") if normalize_space(strong.get_text(" ", strip=True))]
    for strong in strong_tags:
        player_name = canonicalize_player_name(strong.get_text(" ", strip=True))
        if not looks_like_person_name(player_name):
            continue

        prefix_text = row_text_before_tag(li, strong)
        school, position = parse_school_position_from_prefix(prefix_text[-220:])
        lowered_prefix = prefix_text.lower()
        if school is None and position is None and len(strong_tags) > 1:
            if "also goes by" in lowered_prefix or "coach" in lowered_prefix or "coaches" in lowered_prefix:
                continue

        player_norm = normalize_name(player_name)
        if player_norm in seen_names:
            continue
        seen_names.add(player_norm or player_name)
        records.append(
            {
                "year": year,
                "source": NTR_SOURCE,
                "source_priority": SOURCE_PRIORITY[NTR_SOURCE],
                "source_url": source_url,
                "team_slug": team_slug,
                "team_name": team_name_from_slug(team_slug),
                "player_name": player_name,
                "player_norm": player_norm,
                "position_raw": position,
                "position_normalized": normalize_position(position),
                "school": school,
                "visit_types_normalized": join_pipe(visit_types, order=VISIT_TYPE_ORDER),
                "visit_statuses": join_pipe(statuses, order=STATUS_ORDER),
                "section_label": section_label,
                "raw_visit_codes": None,
                "raw_visit_markers": None,
                "note": bullet_text,
            }
        )

    if records:
        return records

    for name, school, position in parse_ntr_plain_text_candidates(bullet_text):
        player_norm = normalize_name(name)
        if player_norm in seen_names:
            continue
        seen_names.add(player_norm or name)
        records.append(
            {
                "year": year,
                "source": NTR_SOURCE,
                "source_priority": SOURCE_PRIORITY[NTR_SOURCE],
                "source_url": source_url,
                "team_slug": team_slug,
                "team_name": team_name_from_slug(team_slug),
                "player_name": name,
                "player_norm": player_norm,
                "position_raw": position,
                "position_normalized": normalize_position(position),
                "school": school,
                "visit_types_normalized": join_pipe(visit_types, order=VISIT_TYPE_ORDER),
                "visit_statuses": join_pipe(statuses, order=STATUS_ORDER),
                "section_label": section_label,
                "raw_visit_codes": None,
                "raw_visit_markers": None,
                "note": bullet_text,
            }
        )
    return records


def parse_nfltraderumors(year: int, html: str, source_url: str) -> list[dict[str, object]]:
    soup = BeautifulSoup(html, "html.parser")
    content = soup.find("div", class_="td-post-content") or soup.find("div", class_="entry-content") or soup.find("article")
    if content is None:
        return []

    current_team_slug: str | None = None
    current_section_label: str | None = None
    records: list[dict[str, object]] = []
    for child in content.children:
        if not isinstance(child, Tag):
            continue
        child_text = normalize_space(child.get_text(" ", strip=True))
        team_slug = team_slug_from_label(child_text)
        if child.name in {"h1", "h2", "h3", "h4"} and team_slug:
            current_team_slug = team_slug
            current_section_label = None
            continue

        section_label = normalize_ntr_section_label(child)
        if section_label:
            current_section_label = section_label
            continue

        if child.name not in {"ul", "ol"} or current_team_slug is None:
            continue
        for li in child.find_all("li", recursive=False):
            records.extend(
                parse_ntr_li(
                    year=year,
                    team_slug=current_team_slug,
                    section_label=current_section_label,
                    source_url=source_url,
                    li=li,
                )
            )
    return records


def parse_walter_codes(raw_codes: str) -> tuple[list[str], str | None]:
    markers = "".join(sorted({ch for ch in raw_codes if ch in {"%", "^", "#"}}))
    cleaned = re.sub(r"[%^#]", "", raw_codes)
    codes = [normalize_space(code) for code in cleaned.split(",") if normalize_space(code)]
    return codes, markers or None


def parse_walterfootball(year: int, html: str, source_url: str) -> list[dict[str, object]]:
    soup = BeautifulSoup(html, "html.parser")
    main_list: Tag | None = None
    for candidate in soup.find_all("ul"):
        if candidate.find("div") and candidate.find("li"):
            main_list = candidate
            break
    if main_list is None:
        return []

    current_team_slug: str | None = None
    records: list[dict[str, object]] = []
    for child in main_list.children:
        if not isinstance(child, Tag):
            continue
        if child.name == "div":
            current_team_slug = team_slug_from_label(normalize_space(child.get_text(" ", strip=True)))
            continue
        if child.name != "li" or current_team_slug is None:
            continue

        row_text = normalize_space(child.get_text(" ", strip=True))
        match = re.match(
            r"^(?P<player_name>.+?),\s+(?P<position_raw>.+?),\s+(?P<school>.+?)\s+\((?P<raw_codes>[^)]+)\)$",
            row_text,
        )
        if match is None:
            continue
        code_list, markers = parse_walter_codes(match.group("raw_codes"))
        visit_types = [WALTER_CODE_MAP.get(code, code.lower()) for code in code_list]
        statuses = ["interest"] if visit_types and set(visit_types) == {"interest"} else ["reported"]
        player_name = canonicalize_player_name(match.group("player_name"))
        position_raw = normalize_space(match.group("position_raw"))
        school = normalize_school(match.group("school"))
        records.append(
            {
                "year": year,
                "source": WALTER_SOURCE,
                "source_priority": SOURCE_PRIORITY[WALTER_SOURCE],
                "source_url": source_url,
                "team_slug": current_team_slug,
                "team_name": team_name_from_slug(current_team_slug),
                "player_name": player_name,
                "player_norm": normalize_name(player_name),
                "position_raw": position_raw,
                "position_normalized": normalize_position(position_raw),
                "school": school,
                "visit_types_normalized": join_pipe(visit_types, order=VISIT_TYPE_ORDER),
                "visit_statuses": join_pipe(statuses, order=STATUS_ORDER),
                "section_label": None,
                "raw_visit_codes": join_pipe(code_list),
                "raw_visit_markers": markers,
                "note": row_text,
            }
        )
    return records


def parse_draftcountdown(year: int, html: str, source_url: str) -> list[dict[str, object]]:
    soup = BeautifulSoup(html, "html.parser")
    panel_to_team_slug: dict[str, str] = {}
    for anchor in soup.find_all("a", href=True):
        team_abbr = normalize_space(anchor.get_text(" ", strip=True))
        if team_abbr not in TEAM_ABBR_TO_SLUG:
            continue
        href = anchor.get("href", "")
        if href.startswith("#"):
            panel_to_team_slug[href[1:]] = TEAM_ABBR_TO_SLUG[team_abbr]

    records: list[dict[str, object]] = []
    for panel_id, team_slug in panel_to_team_slug.items():
        panel = soup.find(id=panel_id)
        table = panel.find("table") if panel else None
        if table is None:
            continue
        for row in table.find_all("tr")[1:]:
            cells = [normalize_space(cell.get_text(" ", strip=True)) for cell in row.find_all(["td", "th"])]
            if len(cells) < 4:
                continue
            first_name, last_name, position_raw, school = cells[:4]
            player_name = canonicalize_player_name(f"{first_name} {last_name}")
            if not player_name:
                continue
            records.append(
                {
                    "year": year,
                    "source": DRAFTCOUNTDOWN_SOURCE,
                    "source_priority": SOURCE_PRIORITY[DRAFTCOUNTDOWN_SOURCE],
                    "source_url": source_url,
                    "team_slug": team_slug,
                    "team_name": team_name_from_slug(team_slug),
                    "player_name": player_name,
                    "player_norm": normalize_name(player_name),
                    "position_raw": position_raw,
                    "position_normalized": normalize_position(position_raw),
                    "school": normalize_school(school),
                    "visit_types_normalized": "top_30_visit",
                    "visit_statuses": "reported",
                    "section_label": "Top 30 Visit Tracker",
                    "raw_visit_codes": None,
                    "raw_visit_markers": None,
                    "note": "Draft Countdown team tab entry from the Top 30 Visit Tracker.",
                }
            )
    return records


def aggregate_source_rollup(records_df: pd.DataFrame) -> pd.DataFrame:
    if records_df.empty:
        return pd.DataFrame()

    rows: list[dict[str, object]] = []
    for (year, source, team_slug, player_norm), group in records_df.groupby(
        ["year", "source", "team_slug", "player_norm"], dropna=False
    ):
        rows.append(
            {
                "year": year,
                "source": source,
                "source_priority": int(group["source_priority"].min()),
                "source_url": choose_preferred_text(group["source_url"].dropna().astype(str)),
                "team_slug": team_slug,
                "team_name": choose_preferred_text(group["team_name"].dropna().astype(str)),
                "player_norm": player_norm,
                "player_name": choose_preferred_text(group["player_name"].dropna().astype(str)),
                "position_raw": choose_preferred_text(group["position_raw"].dropna().astype(str)),
                "position_normalized": choose_preferred_text(group["position_normalized"].dropna().astype(str)),
                "school": choose_preferred_text(group["school"].dropna().astype(str)),
                "visit_types_normalized": join_pipe(
                    (value for cell in group["visit_types_normalized"].dropna().astype(str) for value in cell.split("|")),
                    order=VISIT_TYPE_ORDER,
                ),
                "visit_statuses": join_pipe(
                    (value for cell in group["visit_statuses"].dropna().astype(str) for value in cell.split("|")),
                    order=STATUS_ORDER,
                ),
                "raw_visit_codes": join_pipe(
                    (value for cell in group["raw_visit_codes"].dropna().astype(str) for value in cell.split("|"))
                ),
                "raw_visit_markers": join_pipe(
                    (value for cell in group["raw_visit_markers"].dropna().astype(str) for value in cell.split("|"))
                ),
                "section_labels": join_pipe(group["section_label"].dropna().astype(str)),
                "note_count": int(group["note"].notna().sum()),
                "source_record_count": int(len(group)),
                "missing_position": choose_preferred_text(group["position_normalized"].dropna().astype(str)) is None,
                "missing_school": choose_preferred_text(group["school"].dropna().astype(str)) is None,
                "missing_visit_type": choose_preferred_text(group["visit_types_normalized"].dropna().astype(str))
                is None,
            }
        )
    return pd.DataFrame(rows).sort_values(["year", "source", "team_name", "player_name"]).reset_index(drop=True)


def aggregate_cross_source(source_rollup_df: pd.DataFrame) -> pd.DataFrame:
    if source_rollup_df.empty:
        return pd.DataFrame()

    rows: list[dict[str, object]] = []
    for (year, team_slug, player_norm), group in source_rollup_df.groupby(
        ["year", "team_slug", "player_norm"], dropna=False
    ):
        group = group.sort_values(["source_priority", "source", "player_name"])
        source_count = int(group["source"].nunique())
        rows.append(
            {
                "year": year,
                "team_slug": team_slug,
                "team_name": choose_preferred_text(group["team_name"].dropna().astype(str)),
                "player_norm": player_norm,
                "player_name": choose_preferred_text(group["player_name"].dropna().astype(str)),
                "position_raw": choose_preferred_text(group["position_raw"].dropna().astype(str)),
                "position_normalized": choose_preferred_text(group["position_normalized"].dropna().astype(str)),
                "school": choose_preferred_text(group["school"].dropna().astype(str)),
                "visit_types_normalized": join_pipe(
                    (value for cell in group["visit_types_normalized"].dropna().astype(str) for value in cell.split("|")),
                    order=VISIT_TYPE_ORDER,
                ),
                "visit_statuses": join_pipe(
                    (value for cell in group["visit_statuses"].dropna().astype(str) for value in cell.split("|")),
                    order=STATUS_ORDER,
                ),
                "sources": join_pipe(group["source"].dropna().astype(str)),
                "source_count": source_count,
                "source_record_count": int(group["source_record_count"].sum()),
                "position_sources": int(group["position_normalized"].notna().sum()),
                "school_sources": int(group["school"].notna().sum()),
                "visit_type_sources": int(group["visit_types_normalized"].notna().sum()),
                "backfilled_position": bool(source_count > int(group["position_normalized"].notna().sum()) >= 1),
                "backfilled_school": bool(source_count > int(group["school"].notna().sum()) >= 1),
                "backfilled_visit_type": bool(source_count > int(group["visit_types_normalized"].notna().sum()) >= 1),
                "raw_visit_codes": join_pipe(
                    (value for cell in group["raw_visit_codes"].dropna().astype(str) for value in cell.split("|"))
                ),
                "raw_visit_markers": join_pipe(
                    (value for cell in group["raw_visit_markers"].dropna().astype(str) for value in cell.split("|"))
                ),
            }
        )
    return pd.DataFrame(rows).sort_values(["year", "team_name", "player_name"]).reset_index(drop=True)


def build_year_source_summary(availability_df: pd.DataFrame, source_rollup_df: pd.DataFrame) -> pd.DataFrame:
    if availability_df.empty:
        return pd.DataFrame()
    summary = availability_df.copy()
    if source_rollup_df.empty:
        summary["player_rows"] = 0
        summary["missing_position_rows"] = 0
        summary["missing_school_rows"] = 0
        summary["missing_visit_type_rows"] = 0
        return summary

    stats = (
        source_rollup_df.groupby(["year", "source"], dropna=False)
        .agg(
            player_rows=("player_name", "count"),
            missing_position_rows=("missing_position", "sum"),
            missing_school_rows=("missing_school", "sum"),
            missing_visit_type_rows=("missing_visit_type", "sum"),
        )
        .reset_index()
    )
    return summary.merge(stats, on=["year", "source"], how="left").fillna(
        {"player_rows": 0, "missing_position_rows": 0, "missing_school_rows": 0, "missing_visit_type_rows": 0}
    )


def build_backfill_summary(merged_df: pd.DataFrame) -> pd.DataFrame:
    if merged_df.empty:
        return pd.DataFrame()
    return (
        merged_df.assign(multi_source=merged_df["source_count"] > 1)
        .groupby("year", dropna=False)
        .agg(
            multi_source_rows=("multi_source", "sum"),
            backfilled_position_rows=("backfilled_position", "sum"),
            backfilled_school_rows=("backfilled_school", "sum"),
            backfilled_visit_type_rows=("backfilled_visit_type", "sum"),
        )
        .reset_index()
    )


def load_existing_output(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame()
    existing = pd.read_csv(path)
    if "year" in existing.columns:
        existing["year"] = pd.to_numeric(existing["year"], errors="coerce")
    return existing


def replace_requested_years(
    existing_df: pd.DataFrame,
    new_df: pd.DataFrame,
    *,
    requested_years: set[int],
    sort_by: list[str],
) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    if not existing_df.empty:
        if "year" in existing_df.columns:
            existing_years = pd.to_numeric(existing_df["year"], errors="coerce")
            existing_df = existing_df[~existing_years.isin(requested_years)].copy()
        frames.append(existing_df)
    if not new_df.empty:
        frames.append(new_df.copy())
    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True, sort=False)
    sort_columns = [column_name for column_name in sort_by if column_name in combined.columns]
    if sort_columns:
        combined = combined.sort_values(sort_columns).reset_index(drop=True)
    return combined


def main() -> None:
    args = parse_args()
    if args.start_year > args.end_year:
        raise ValueError("--start-year must be less than or equal to --end-year.")

    data_dir = Path(args.data_dir)
    raw_dir = data_dir / "raw" / "draft-visits"
    processed_dir = data_dir / "processed" / "draft-visits"
    ensure_dir(raw_dir)
    ensure_dir(processed_dir)

    parser_map = {
        WALTER_SOURCE: (walter_page_available, parse_walterfootball),
        NTR_SOURCE: (ntr_page_available, parse_nfltraderumors),
        DRAFTCOUNTDOWN_SOURCE: (draftcountdown_page_available, parse_draftcountdown),
    }
    availability_rows: list[dict[str, object]] = []
    source_records: list[dict[str, object]] = []

    for year in range(args.start_year, args.end_year + 1):
        for source in (WALTER_SOURCE, NTR_SOURCE, DRAFTCOUNTDOWN_SOURCE):
            requested_url = parse_requested_url(source, year)
            if requested_url is None:
                availability_rows.append(
                    {
                        "year": year,
                        "source": source,
                        "requested_url": None,
                        "final_url": None,
                        "status_code": None,
                        "available": False,
                        "availability_note": "no_known_url_for_year",
                    }
                )
                continue

            fetch_result = load_or_fetch_page(
                source=source,
                year=year,
                raw_dir=raw_dir,
                refresh=args.refresh,
                timeout=args.timeout,
            )
            available_fn, parser_fn = parser_map[source]
            available, note = available_fn(year, fetch_result)
            availability_rows.append(
                {
                    "year": year,
                    "source": source,
                    "requested_url": requested_url,
                    "final_url": fetch_result.final_url,
                    "status_code": fetch_result.status_code,
                    "available": available,
                    "availability_note": note,
                }
            )
            if available and fetch_result.html:
                source_records.extend(parser_fn(year, fetch_result.html, fetch_result.final_url or requested_url))
            if args.sleep_seconds:
                sleep(args.sleep_seconds)

    availability_df = pd.DataFrame(availability_rows).sort_values(["year", "source"]).reset_index(drop=True)
    records_df = pd.DataFrame(source_records)
    if not records_df.empty:
        records_df = records_df.sort_values(["year", "source", "team_name", "player_name"]).reset_index(drop=True)
    source_rollup_df = aggregate_source_rollup(records_df)
    merged_df = aggregate_cross_source(source_rollup_df)
    year_source_summary_df = build_year_source_summary(availability_df, source_rollup_df)
    backfill_summary_df = build_backfill_summary(merged_df)

    requested_years = set(range(args.start_year, args.end_year + 1))
    if not args.replace_existing_outputs:
        availability_df = replace_requested_years(
            load_existing_output(processed_dir / "draft_visits__availability.csv"),
            availability_df,
            requested_years=requested_years,
            sort_by=["year", "source"],
        )
        records_df = replace_requested_years(
            load_existing_output(processed_dir / "draft_visits__source_records.csv"),
            records_df,
            requested_years=requested_years,
            sort_by=["year", "source", "team_name", "player_name"],
        )
        source_rollup_df = replace_requested_years(
            load_existing_output(processed_dir / "draft_visits__source_player_rollup.csv"),
            source_rollup_df,
            requested_years=requested_years,
            sort_by=["year", "source", "team_name", "player_name"],
        )
        merged_df = replace_requested_years(
            load_existing_output(processed_dir / "draft_visits__merged.csv"),
            merged_df,
            requested_years=requested_years,
            sort_by=["year", "team_name", "player_name"],
        )
        year_source_summary_df = replace_requested_years(
            load_existing_output(processed_dir / "draft_visits__summary_by_year_source.csv"),
            year_source_summary_df,
            requested_years=requested_years,
            sort_by=["year", "source"],
        )
        backfill_summary_df = replace_requested_years(
            load_existing_output(processed_dir / "draft_visits__backfill_summary.csv"),
            backfill_summary_df,
            requested_years=requested_years,
            sort_by=["year"],
        )

    if not merged_df.empty and "year" in merged_df.columns:
        merged_years = pd.to_numeric(merged_df["year"], errors="coerce")
        current_cycle_df = merged_df[merged_years == args.current_year].copy()
    else:
        current_cycle_df = pd.DataFrame(columns=merged_df.columns if not merged_df.empty else [])

    availability_df.to_csv(processed_dir / "draft_visits__availability.csv", index=False)
    records_df.to_csv(processed_dir / "draft_visits__source_records.csv", index=False)
    source_rollup_df.to_csv(processed_dir / "draft_visits__source_player_rollup.csv", index=False)
    merged_df.to_csv(processed_dir / "draft_visits__merged.csv", index=False)
    year_source_summary_df.to_csv(processed_dir / "draft_visits__summary_by_year_source.csv", index=False)
    backfill_summary_df.to_csv(processed_dir / "draft_visits__backfill_summary.csv", index=False)
    current_cycle_df.to_csv(processed_dir / "draft_visits__current_cycle.csv", index=False)
    current_cycle_df.to_csv(processed_dir / f"draft_visits__current_{args.current_year}.csv", index=False)

    print(f"Wrote visit availability rows: {len(availability_df)}", flush=True)
    print(f"Available source-year pages: {int(availability_df['available'].sum()) if not availability_df.empty else 0}", flush=True)
    print(f"Parsed source records: {len(records_df)}", flush=True)
    print(f"Source player rollups: {len(source_rollup_df)}", flush=True)
    print(f"Merged player-team-year rows: {len(merged_df)}", flush=True)
    print(f"Current-cycle {args.current_year} rows: {len(current_cycle_df)}", flush=True)
    if not backfill_summary_df.empty:
        print(backfill_summary_df.to_string(index=False), flush=True)


if __name__ == "__main__":
    main()
