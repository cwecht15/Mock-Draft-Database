from __future__ import annotations

import html
import os
import re
import subprocess
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

import pandas as pd
import streamlit as st
from scripts.scrape_nflmockdraftdatabase import (
    infer_section_from_mock_url,
    infer_source_team_slug_from_team_mock_url,
    normalize_input_mock_url,
    parse_mock_page,
    slugify,
    team_name_from_slug,
)


ROOT_DIR = Path(__file__).resolve().parent
PROCESSED_DIR = ROOT_DIR / "data" / "processed"
CURRENT_YEAR = 2026
HISTORICAL_YEARS = list(range(2020, 2026))
GENERIC_AUTHORS = {"", "staff", "media", "editors", "editorial staff"}
ROUND_CAPITAL_WEIGHTS = {
    1: 100.0,
    2: 55.0,
    3: 30.0,
    4: 16.0,
    5: 9.0,
    6: 5.0,
    7: 3.0,
}
REFRESH_COOLDOWN_MINUTES = 20
REFRESH_SLEEP_SECONDS = 1.5


def is_read_only_mode() -> bool:
    try:
        streamlit_secret_value = (
            str(st.secrets.get("MOCK_DRAFT_APP_READ_ONLY", ""))
            or str(st.secrets.get("PUBLIC_READ_ONLY", ""))
            or str(st.secrets.get("READ_ONLY", ""))
        )
    except Exception:  # noqa: BLE001
        streamlit_secret_value = ""
    value = (
        os.getenv("MOCK_DRAFT_APP_READ_ONLY")
        or os.getenv("PUBLIC_READ_ONLY")
        or os.getenv("READ_ONLY")
        or streamlit_secret_value
        or ""
    ).strip().lower()
    return value in {"1", "true", "yes", "on"}


def ingestion_history_path() -> Path:
    year_dir = PROCESSED_DIR / str(CURRENT_YEAR)
    year_dir.mkdir(parents=True, exist_ok=True)
    return year_dir / "ingestion_history.csv"


def normalize_author(author_name: str | None) -> str:
    return (author_name or "").strip().lower()


def round_capital_weight(round_number: object) -> float:
    if pd.isna(round_number):
        return 0.0
    try:
        return ROUND_CAPITAL_WEIGHTS.get(int(round_number), 0.0)
    except (TypeError, ValueError):
        return 0.0


def mode_or_first(values: pd.Series) -> object:
    cleaned = values.dropna()
    if cleaned.empty:
        return None
    modes = cleaned.mode()
    if not modes.empty:
        return modes.iloc[0]
    return cleaned.iloc[0]


def current_cycle_archive_paths(section: str) -> tuple[Path, Path]:
    year_dir = PROCESSED_DIR / str(CURRENT_YEAR)
    return (
        year_dir / f"{section}__mock_metadata.archive.csv",
        year_dir / f"{section}__mock_picks.archive.csv",
    )


def build_archive_author_dedupe_key(row: pd.Series, section: str) -> str:
    author = normalize_author(row.get("author_name"))
    mock_name = str(row.get("mock_name") or "").strip().lower()
    dedupe_key = author if author not in GENERIC_AUTHORS else f"{author}::{mock_name}"
    if section in {"teams", "team-mock-drafts"}:
        team_slug = str(row.get("source_team_slug") or "").strip().lower()
        dedupe_key = f"{dedupe_key}::{team_slug}"
    return dedupe_key


def dedupe_current_cycle_section(
    metadata: pd.DataFrame,
    picks: pd.DataFrame,
    *,
    section: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    if metadata.empty:
        return metadata, picks.iloc[0:0].copy()

    metadata = metadata.copy()
    picks = picks.copy()
    metadata["published_dt"] = pd.to_datetime(metadata["published_at"], format="%m/%d/%y", errors="coerce")
    metadata["author_dedupe_key"] = metadata.apply(
        lambda row: build_archive_author_dedupe_key(row, section),
        axis=1,
    )
    if "source_section" in metadata.columns:
        metadata["source_section_priority"] = metadata["source_section"].map(
            {"team-mock-drafts": 1, "teams": 0}
        ).fillna(0)
    else:
        metadata["source_section_priority"] = 0
    metadata = metadata.sort_values(
        by=["author_dedupe_key", "published_dt", "source_section_priority", "mock_relative_url"],
        ascending=[True, False, False, True],
        na_position="last",
    )
    metadata = metadata.drop_duplicates(subset=["author_dedupe_key"], keep="first").copy()
    kept_urls = set(metadata["mock_relative_url"].dropna().astype(str))
    if kept_urls:
        picks = picks[picks["mock_relative_url"].astype(str).isin(kept_urls)].copy()
    else:
        picks = picks.iloc[0:0].copy()
    if not picks.empty:
        # Archive refreshes can append the same normalized pick rows for a kept mock URL.
        # Collapse exact duplicates here so downstream tables show each mocked selection once.
        picks = picks.drop_duplicates().copy()
    metadata = metadata.drop(
        columns=["published_dt", "author_dedupe_key", "source_section_priority"],
        errors="ignore",
    )
    return metadata, picks


def ensure_team_mock_source_fields(
    metadata: pd.DataFrame,
    picks: pd.DataFrame,
    *,
    section: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    metadata = metadata.copy()
    picks = picks.copy()
    if section != "team-mock-drafts":
        return metadata, picks

    if "source_team_slug" not in metadata.columns:
        metadata["source_team_slug"] = metadata["mock_relative_url"].map(infer_source_team_slug_from_team_mock_url)
    else:
        metadata["source_team_slug"] = metadata["source_team_slug"].fillna(
            metadata["mock_relative_url"].map(infer_source_team_slug_from_team_mock_url)
        )
    if "source_team_name" not in metadata.columns:
        metadata["source_team_name"] = metadata["source_team_slug"].map(team_name_from_slug)
    else:
        metadata["source_team_name"] = metadata["source_team_name"].fillna(
            metadata["source_team_slug"].map(team_name_from_slug)
        )

    if "source_team_slug" not in picks.columns:
        if "mock_relative_url" in picks.columns:
            picks["source_team_slug"] = picks["mock_relative_url"].map(infer_source_team_slug_from_team_mock_url)
        else:
            picks["source_team_slug"] = pd.Series(dtype="object")
    else:
        picks["source_team_slug"] = picks["source_team_slug"].fillna(
            picks["mock_relative_url"].map(infer_source_team_slug_from_team_mock_url)
        )
    if "source_team_name" not in picks.columns:
        picks["source_team_name"] = picks["source_team_slug"].map(team_name_from_slug)
    else:
        picks["source_team_name"] = picks["source_team_name"].fillna(
            picks["source_team_slug"].map(team_name_from_slug)
        )
    return metadata, picks


def archive_current_cycle_section(section: str) -> str | None:
    year_dir = PROCESSED_DIR / str(CURRENT_YEAR)
    current_metadata_path = year_dir / f"{section}__mock_metadata.csv"
    current_picks_path = year_dir / f"{section}__mock_picks.csv"
    if not current_metadata_path.exists() or not current_picks_path.exists():
        return None

    archive_metadata_path, archive_picks_path = current_cycle_archive_paths(section)
    current_metadata = pd.read_csv(current_metadata_path)
    current_picks = pd.read_csv(current_picks_path)

    if archive_metadata_path.exists():
        archive_metadata = pd.read_csv(archive_metadata_path)
        combined_metadata = pd.concat([archive_metadata, current_metadata], ignore_index=True)
    else:
        combined_metadata = current_metadata.copy()
    if "mock_relative_url" in combined_metadata.columns:
        combined_metadata = combined_metadata.drop_duplicates(subset=["mock_relative_url"], keep="last")
    else:
        combined_metadata = combined_metadata.drop_duplicates()

    if archive_picks_path.exists():
        archive_picks = pd.read_csv(archive_picks_path)
        combined_picks = pd.concat([archive_picks, current_picks], ignore_index=True)
    else:
        combined_picks = current_picks.copy()

    combined_metadata, combined_picks = dedupe_current_cycle_section(
        combined_metadata,
        combined_picks,
        section=section,
    )

    combined_metadata.to_csv(archive_metadata_path, index=False)
    combined_picks.to_csv(archive_picks_path, index=False)
    return (
        f"{section}: archived {len(current_metadata)} current mocks into "
        f"{len(combined_metadata)} season-to-date mocks"
    )


def current_cycle_metadata_url_set(section: str) -> set[str]:
    year_dir = PROCESSED_DIR / str(CURRENT_YEAR)
    archive_metadata_path, _ = current_cycle_archive_paths(section)
    metadata_path = archive_metadata_path if archive_metadata_path.exists() else year_dir / f"{section}__mock_metadata.csv"
    if not metadata_path.exists():
        return set()
    metadata = pd.read_csv(metadata_path, usecols=["mock_relative_url"])
    return set(metadata["mock_relative_url"].dropna().astype(str))


def append_ingestion_history(events: list[dict[str, object]]) -> None:
    if not events:
        return
    path = ingestion_history_path()
    new_rows = pd.DataFrame(events)
    if path.exists():
        existing = pd.read_csv(path)
        combined = pd.concat([existing, new_rows], ignore_index=True)
    else:
        combined = new_rows.copy()
    combined.to_csv(path, index=False)


@st.cache_data(show_spinner=False)
def load_ingestion_history() -> pd.DataFrame:
    path = ingestion_history_path()
    if not path.exists():
        return pd.DataFrame()
    history = pd.read_csv(path)
    if "ingested_at" in history.columns:
        history["ingested_dt"] = pd.to_datetime(history["ingested_at"], errors="coerce")
    return history.sort_values(by=["ingested_dt"], ascending=[False], na_position="last")


@st.cache_data(show_spinner=False)
def load_current_cycle_mock_metadata() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for section in ("mock-drafts", "team-mock-drafts"):
        archive_metadata_path, _ = current_cycle_archive_paths(section)
        current_path = PROCESSED_DIR / str(CURRENT_YEAR) / f"{section}__mock_metadata.csv"
        path = archive_metadata_path if archive_metadata_path.exists() else current_path
        if not path.exists():
            continue
        frame = pd.read_csv(path)
        if frame.empty:
            continue
        frame = frame.copy()
        if section == "team-mock-drafts":
            frame, _ = ensure_team_mock_source_fields(frame, pd.DataFrame(), section=section)
        frame["section"] = section
        frame["published_dt"] = pd.to_datetime(frame.get("published_at"), format="%m/%d/%y", errors="coerce")
        frames.append(frame)
    if not frames:
        return pd.DataFrame()
    combined = pd.concat(frames, ignore_index=True)
    return combined.sort_values(
        by=["published_dt", "section", "author_name", "mock_name"],
        ascending=[False, True, True, True],
        na_position="last",
    )


def summarize_refresh_delta(
    *,
    section: str,
    before_urls: set[str],
    after_urls: set[str],
) -> str:
    labels = {
        "mock-drafts": "first-round mocks",
        "team-mock-drafts": "team mocks",
        "teams": "team mocks",
    }
    label = labels.get(section, section)
    added_count = len(after_urls - before_urls)
    removed_count = len(before_urls - after_urls)
    if added_count == 0 and removed_count == 0:
        return f"No new {label} found."
    if added_count > 0 and removed_count > 0:
        return f"Found {added_count} new {label} and replaced {removed_count} older author versions."
    if added_count > 0:
        return f"Found {added_count} new {label}."
    return f"Updated {label}; {removed_count} older author versions were removed."


def refresh_current_cycle_data(status_callback=None) -> tuple[bool, str]:
    def update_status(message: str) -> None:
        if status_callback is not None:
            status_callback(message)

    before_url_sets = {
        "mock-drafts": current_cycle_metadata_url_set("mock-drafts"),
        "team-mock-drafts": current_cycle_metadata_url_set("team-mock-drafts"),
    }

    commands = [
        (
            "Refreshing rolling 14-day first-round mocks and team mocks...",
            [
                sys.executable,
                str(ROOT_DIR / "scripts" / "scrape_nflmockdraftdatabase.py"),
                "--year",
                str(CURRENT_YEAR),
                "--section",
                "mock-drafts",
                "--section",
                "team-mock-drafts",
                "--data-dir",
                str(ROOT_DIR / "data"),
                "--published-days-back",
                "14",
                "--sleep-seconds",
                str(REFRESH_SLEEP_SECONDS),
                "--latest-author-mock-only",
                "--resume",
                "--refresh-list-pages",
            ],
        ),
        (
            "Rebuilding current trend files...",
            [
                sys.executable,
                str(ROOT_DIR / "scripts" / "analyze_mock_trends.py"),
                "--year",
                str(CURRENT_YEAR),
                "--section",
                "mock-drafts",
                "--section",
                "team-mock-drafts",
                "--processed-dir",
                str(PROCESSED_DIR),
            ],
        ),
        (
            "Rebuilding weighted specialist outputs...",
            [
                sys.executable,
                str(ROOT_DIR / "scripts" / "build_team_specialist_weights.py"),
                "--processed-dir",
                str(PROCESSED_DIR),
                "--history-start-year",
                "2020",
                "--history-end-year",
                "2025",
                "--min-attempts",
                "5",
                "--min-years-covered",
                "4",
                "--target-year",
                str(CURRENT_YEAR),
            ],
        ),
    ]

    outputs: list[str] = []
    for index, (status_message, command) in enumerate(commands, start=1):
        update_status(status_message)
        result = subprocess.run(
            command,
            cwd=ROOT_DIR,
            capture_output=True,
            text=True,
            check=False,
        )
        command_label = " ".join(command[1:3]) if len(command) >= 3 else " ".join(command)
        if result.returncode != 0:
            error_text = result.stderr.strip() or result.stdout.strip() or "Unknown error"
            return False, f"{command_label} failed: {error_text}"
        output_text = result.stdout.strip()
        if output_text:
            outputs.append(output_text)
        if index == 1:
            archive_messages: list[str] = []
            delta_messages: list[str] = []
            for section in ("mock-drafts", "team-mock-drafts"):
                archive_message = archive_current_cycle_section(section)
                if archive_message:
                    archive_messages.append(archive_message)
                after_urls = current_cycle_metadata_url_set(section)
                delta_messages.append(
                    summarize_refresh_delta(
                        section=section,
                        before_urls=before_url_sets.get(section, set()),
                        after_urls=after_urls,
                    )
                )
            outputs.extend(archive_messages)
            outputs.extend(delta_messages)
            update_status(" ".join(delta_messages))

    st.cache_data.clear()
    return True, "\n\n".join(outputs) if outputs else "Refresh completed."


def build_refresh_highlight_lines(message: str) -> list[str]:
    summary_lines = [line.strip() for line in message.splitlines() if line.strip()]
    return [
        line
        for line in summary_lines
        if ("new first-round mocks" in line.lower())
        or ("new team mocks" in line.lower())
        or ("no new first-round mocks found" in line.lower())
        or ("no new team mocks found" in line.lower())
        or ("updated first-round mocks" in line.lower())
        or ("updated team mocks" in line.lower())
    ]


def summarize_result_for_sidebar(message: str, *, ok: bool) -> list[str]:
    highlight_lines = build_refresh_highlight_lines(message)
    if highlight_lines:
        return highlight_lines
    summary_lines = [line.strip() for line in message.splitlines() if line.strip()]
    if ok:
        return summary_lines[:3]
    return summary_lines[:5]


def extract_manual_mock_urls(text: str) -> list[str]:
    urls: list[str] = []
    seen: set[str] = set()
    for raw_line in text.splitlines():
        url = raw_line.strip()
        if not url or url in seen:
            continue
        seen.add(url)
        urls.append(url)
    return urls


def normalize_ingest_html_source(html_text: str) -> tuple[str, bool]:
    text = html_text or ""
    if not text.strip():
        return text, False

    if "<td class=\"line-content\">" not in text or "saved from url=" not in text.lower():
        return text, False

    cells = re.findall(r"<td class=\"line-content\">(.*?)</td>", text, flags=re.IGNORECASE | re.DOTALL)
    if not cells:
        return text, False

    lines: list[str] = []
    for cell in cells:
        cell = re.sub(r"<br\\s*/?>", "", cell, flags=re.IGNORECASE)
        cell = re.sub(r"<[^>]+>", "", cell)
        line = html.unescape(cell)
        lines.append(line)

    normalized = "\n".join(lines).strip()
    if not normalized:
        return text, False
    return normalized, True


def extract_mock_url_from_html(html_text: str) -> str | None:
    saved_from_match = re.search(r"saved from url=\(\d+\)(https://[^\s>]+)", html_text or "", flags=re.IGNORECASE)
    if saved_from_match:
        candidate = html.unescape(saved_from_match.group(1)).strip()
        try:
            relative_url = normalize_input_mock_url(candidate)
            infer_section_from_mock_url(relative_url)
            return candidate
        except Exception:  # noqa: BLE001
            pass

    html_text, _ = normalize_ingest_html_source(html_text)
    if not html_text.strip():
        return None

    patterns = [
        r'<link[^>]+rel=["\']canonical["\'][^>]+href=["\']([^"\']+)["\']',
        r'<meta[^>]+property=["\']og:url["\'][^>]+content=["\']([^"\']+)["\']',
        r'"canonical_url"\s*:\s*"([^"]+)"',
        r'"url"\s*:\s*"(https://www\.nflmockdraftdatabase\.com/[^"]+)"',
    ]
    for pattern in patterns:
        for match in re.finditer(pattern, html_text, flags=re.IGNORECASE):
            candidate = html.unescape(match.group(1)).strip()
            if "nflmockdraftdatabase.com" not in candidate and not candidate.startswith("/"):
                continue
            try:
                relative_url = normalize_input_mock_url(candidate)
                infer_section_from_mock_url(relative_url)
                return candidate if candidate.startswith("http") else f"https://www.nflmockdraftdatabase.com{relative_url}"
            except Exception:  # noqa: BLE001
                continue
    return None


def resolve_mock_url_for_html_ingest(mock_url_text: str, html_text: str) -> tuple[str, bool]:
    if mock_url_text.strip():
        return normalize_input_mock_url(mock_url_text), False

    detected_url = extract_mock_url_from_html(html_text)
    if not detected_url:
        raise ValueError("A mock URL is required unless the pasted/uploaded HTML includes a detectable canonical mock URL.")
    return normalize_input_mock_url(detected_url), True


def run_current_output_rebuilds(status_callback=None) -> tuple[bool, str]:
    def update_status(message: str) -> None:
        if status_callback is not None:
            status_callback(message)

    commands = [
        (
            "Rebuilding current trend files...",
            [
                sys.executable,
                str(ROOT_DIR / "scripts" / "analyze_mock_trends.py"),
                "--year",
                str(CURRENT_YEAR),
                "--section",
                "mock-drafts",
                "--section",
                "team-mock-drafts",
                "--processed-dir",
                str(PROCESSED_DIR),
            ],
        ),
        (
            "Rebuilding weighted specialist outputs...",
            [
                sys.executable,
                str(ROOT_DIR / "scripts" / "build_team_specialist_weights.py"),
                "--processed-dir",
                str(PROCESSED_DIR),
                "--history-start-year",
                "2020",
                "--history-end-year",
                "2025",
                "--min-attempts",
                "5",
                "--min-years-covered",
                "4",
                "--target-year",
                str(CURRENT_YEAR),
            ],
        ),
    ]
    outputs: list[str] = []
    for status_message, command in commands:
        update_status(status_message)
        result = subprocess.run(
            command,
            cwd=ROOT_DIR,
            capture_output=True,
            text=True,
            check=False,
        )
        if result.returncode != 0:
            error_text = result.stderr.strip() or result.stdout.strip() or "Unknown error"
            return False, error_text
        output_text = result.stdout.strip()
        if output_text:
            outputs.append(output_text)
    return True, "\n\n".join(outputs)


def ingest_manual_mock_urls(urls: list[str], status_callback=None) -> tuple[bool, str]:
    def update_status(message: str) -> None:
        if status_callback is not None:
            status_callback(message)

    if not urls:
        return False, "No mock URLs were provided."

    history_events: list[dict[str, object]] = []
    duplicate_urls: list[str] = []
    new_urls: list[str] = []
    for input_url in urls:
        try:
            relative_url = normalize_input_mock_url(input_url)
            section = infer_section_from_mock_url(relative_url)
        except Exception as exc:  # noqa: BLE001
            history_events.append(
                {
                    "ingested_at": datetime.now().isoformat(),
                    "method": "manual_url",
                    "status": "invalid",
                    "section": "",
                    "mock_relative_url": str(input_url),
                    "detail": str(exc),
                }
            )
            append_ingestion_history(history_events)
            st.cache_data.clear()
            return False, str(exc)
        if relative_url in current_cycle_metadata_url_set(section):
            duplicate_urls.append(relative_url)
            history_events.append(
                {
                    "ingested_at": datetime.now().isoformat(),
                    "method": "manual_url",
                    "status": "duplicate",
                    "section": section,
                    "mock_relative_url": relative_url,
                    "detail": "URL was already present in the local current-cycle dataset.",
                }
            )
        else:
            new_urls.append(input_url)

    if not new_urls:
        duplicate_text = "\n".join(f"Already ingested: {url}" for url in duplicate_urls[:10])
        message = "All entered mock URLs were already in the local dataset."
        if duplicate_text:
            message += "\n\n" + duplicate_text
        append_ingestion_history(history_events)
        st.cache_data.clear()
        return True, message

    before_url_sets = {
        "mock-drafts": current_cycle_metadata_url_set("mock-drafts"),
        "team-mock-drafts": current_cycle_metadata_url_set("team-mock-drafts"),
    }

    scrape_command = [
        sys.executable,
        str(ROOT_DIR / "scripts" / "scrape_nflmockdraftdatabase.py"),
        "--year",
        str(CURRENT_YEAR),
        "--data-dir",
        str(ROOT_DIR / "data"),
        "--sleep-seconds",
        str(REFRESH_SLEEP_SECONDS),
        "--resume",
    ]
    for url in new_urls:
        scrape_command.extend(["--mock-url", url])

    commands = [
        ("Fetching the manually entered mock URLs...", scrape_command),
        (
            "Rebuilding current trend files...",
            [
                sys.executable,
                str(ROOT_DIR / "scripts" / "analyze_mock_trends.py"),
                "--year",
                str(CURRENT_YEAR),
                "--section",
                "mock-drafts",
                "--section",
                "team-mock-drafts",
                "--processed-dir",
                str(PROCESSED_DIR),
            ],
        ),
        (
            "Rebuilding weighted specialist outputs...",
            [
                sys.executable,
                str(ROOT_DIR / "scripts" / "build_team_specialist_weights.py"),
                "--processed-dir",
                str(PROCESSED_DIR),
                "--history-start-year",
                "2020",
                "--history-end-year",
                "2025",
                "--min-attempts",
                "5",
                "--min-years-covered",
                "4",
                "--target-year",
                str(CURRENT_YEAR),
            ],
        ),
    ]

    outputs: list[str] = []
    for index, (status_message, command) in enumerate(commands, start=1):
        update_status(status_message)
        result = subprocess.run(
            command,
            cwd=ROOT_DIR,
            capture_output=True,
            text=True,
            check=False,
        )
        command_label = " ".join(command[1:3]) if len(command) >= 3 else " ".join(command)
        if result.returncode != 0:
            error_text = result.stderr.strip() or result.stdout.strip() or "Unknown error"
            return False, f"{command_label} failed: {error_text}"
        output_text = result.stdout.strip()
        if output_text:
            outputs.append(output_text)
        if index == 1:
            archive_messages: list[str] = []
            delta_messages: list[str] = []
            for section in ("mock-drafts", "team-mock-drafts"):
                archive_message = archive_current_cycle_section(section)
                if archive_message:
                    archive_messages.append(archive_message)
                after_urls = current_cycle_metadata_url_set(section)
                delta_messages.append(
                    summarize_refresh_delta(
                        section=section,
                        before_urls=before_url_sets.get(section, set()),
                        after_urls=after_urls,
                    )
                )
            outputs.extend(archive_messages)
            outputs.extend(delta_messages)
            update_status(" ".join(delta_messages))

    for url in new_urls:
        relative_url = normalize_input_mock_url(url)
        section = infer_section_from_mock_url(relative_url)
        status = "ingested" if relative_url in current_cycle_metadata_url_set(section) else "unknown"
        history_events.append(
            {
                "ingested_at": datetime.now().isoformat(),
                "method": "manual_url",
                "status": status,
                "section": section,
                "mock_relative_url": relative_url,
                "detail": "Fetched from direct mock URL entry.",
            }
        )
    append_ingestion_history(history_events)
    st.cache_data.clear()
    result_message = "\n\n".join(outputs) if outputs else "Manual mock ingest completed."
    if duplicate_urls:
        result_message += "\n\nSkipped already ingested URLs:\n" + "\n".join(duplicate_urls[:10])
    return True, result_message


def upsert_current_cycle_section_records(
    *,
    section: str,
    metadata_rows: list[dict[str, object]],
    pick_rows: list[dict[str, object]],
) -> tuple[int, int]:
    year_dir = PROCESSED_DIR / str(CURRENT_YEAR)
    year_dir.mkdir(parents=True, exist_ok=True)

    current_metadata_path = year_dir / f"{section}__mock_metadata.csv"
    current_picks_path = year_dir / f"{section}__mock_picks.csv"
    archive_metadata_path, archive_picks_path = current_cycle_archive_paths(section)

    metadata_df = pd.DataFrame(metadata_rows)
    picks_df = pd.DataFrame(pick_rows)

    existing_current_metadata = pd.read_csv(current_metadata_path) if current_metadata_path.exists() else pd.DataFrame()
    existing_current_picks = pd.read_csv(current_picks_path) if current_picks_path.exists() else pd.DataFrame()
    combined_current_metadata = pd.concat([existing_current_metadata, metadata_df], ignore_index=True)
    combined_current_picks = pd.concat([existing_current_picks, picks_df], ignore_index=True)
    if "mock_relative_url" in combined_current_metadata.columns:
        combined_current_metadata = combined_current_metadata.drop_duplicates(
            subset=["mock_relative_url"],
            keep="last",
        )
    else:
        combined_current_metadata = combined_current_metadata.drop_duplicates()
    combined_current_metadata, combined_current_picks = dedupe_current_cycle_section(
        combined_current_metadata,
        combined_current_picks,
        section=section,
    )
    combined_current_metadata.to_csv(current_metadata_path, index=False)
    combined_current_picks.to_csv(current_picks_path, index=False)

    existing_archive_metadata = pd.read_csv(archive_metadata_path) if archive_metadata_path.exists() else pd.DataFrame()
    existing_archive_picks = pd.read_csv(archive_picks_path) if archive_picks_path.exists() else pd.DataFrame()
    combined_archive_metadata = pd.concat([existing_archive_metadata, metadata_df], ignore_index=True)
    combined_archive_picks = pd.concat([existing_archive_picks, picks_df], ignore_index=True)
    if "mock_relative_url" in combined_archive_metadata.columns:
        combined_archive_metadata = combined_archive_metadata.drop_duplicates(
            subset=["mock_relative_url"],
            keep="last",
        )
    else:
        combined_archive_metadata = combined_archive_metadata.drop_duplicates()
    combined_archive_metadata, combined_archive_picks = dedupe_current_cycle_section(
        combined_archive_metadata,
        combined_archive_picks,
        section=section,
    )
    combined_archive_metadata.to_csv(archive_metadata_path, index=False)
    combined_archive_picks.to_csv(archive_picks_path, index=False)

    return len(combined_current_metadata), len(combined_archive_metadata)


def ingest_single_mock_html_record(
    *,
    mock_url_text: str,
    html_text: str,
    method: str,
    detail: str,
    status_callback=None,
) -> tuple[bool, str, dict[str, object]]:
    def update_status(message: str) -> None:
        if status_callback is not None:
            status_callback(message)

    if not html_text.strip():
        return (
            False,
            "Page source HTML is required.",
            {
                "ingested_at": datetime.now().isoformat(),
                "method": method,
                "status": "failed",
                "section": "",
                "mock_relative_url": mock_url_text.strip(),
                "detail": "Page source HTML is required.",
            },
        )

    try:
        normalized_html_text, unwrapped_view_source = normalize_ingest_html_source(html_text)
        relative_url, url_autodetected = resolve_mock_url_for_html_ingest(mock_url_text, html_text)
        section = infer_section_from_mock_url(relative_url)
        existed_before = relative_url in current_cycle_metadata_url_set(section)
        parser_section = section
        absolute_url = (
            relative_url if relative_url.startswith("http") else f"https://www.nflmockdraftdatabase.com{relative_url}"
        )
        update_status(f"Parsing HTML for {relative_url} ...")
        metadata, picks = parse_mock_page(
            normalized_html_text,
            year=CURRENT_YEAR,
            section=parser_section,
            mock_relative_url=relative_url,
            mock_absolute_url=absolute_url,
        )
        if section in {"teams", "team-mock-drafts"}:
            source_team_slug = infer_source_team_slug_from_team_mock_url(relative_url)
            metadata["source_team_slug"] = source_team_slug
            metadata["source_team_name"] = team_name_from_slug(source_team_slug)
            metadata["source_team_year"] = CURRENT_YEAR
            for pick in picks:
                pick["source_team_slug"] = source_team_slug
                pick["source_team_name"] = team_name_from_slug(source_team_slug)
                pick["source_team_year"] = CURRENT_YEAR
        raw_dir = ROOT_DIR / "data" / "raw" / str(CURRENT_YEAR) / section / "mocks"
        raw_dir.mkdir(parents=True, exist_ok=True)
        raw_path = raw_dir / f"{slugify(relative_url.strip('/'))}.html"
        raw_path.write_text(normalized_html_text, encoding="utf-8")
        update_status("Writing parsed mock into the current local dataset...")
        current_count, archive_count = upsert_current_cycle_section_records(
            section=section,
            metadata_rows=[metadata],
            pick_rows=picks,
        )
        auto_note = " (mock URL auto-detected from HTML)" if url_autodetected else ""
        unwrap_note = " (saved view-source HTML was unwrapped)" if unwrapped_view_source else ""
        outputs = [
            f"Ingested HTML for {relative_url}{auto_note}{unwrap_note}",
            (
                f"{section}: replaced existing local mock and now has {current_count} current mocks and "
                f"{archive_count} archived mocks"
                if existed_before
                else f"{section}: added a new local mock and now has {current_count} current mocks and "
                f"{archive_count} archived mocks"
            ),
        ]
        return (
            True,
            "\n\n".join(outputs),
            {
                "ingested_at": datetime.now().isoformat(),
                "method": method,
                "status": "replaced" if existed_before else "ingested",
                "section": section,
                "mock_relative_url": relative_url,
                "detail": detail
                + (" URL auto-detected from HTML." if url_autodetected else "")
                + (" Saved view-source HTML was unwrapped." if unwrapped_view_source else ""),
            },
        )
    except Exception as exc:  # noqa: BLE001
        fallback_url = mock_url_text.strip() or extract_mock_url_from_html(html_text) or ""
        return (
            False,
            str(exc),
            {
                "ingested_at": datetime.now().isoformat(),
                "method": method,
                "status": "failed",
                "section": "",
                "mock_relative_url": fallback_url,
                "detail": str(exc),
            },
        )


def ingest_pasted_mock_html(
    *,
    mock_url_text: str,
    html_text: str,
    status_callback=None,
) -> tuple[bool, str]:
    ok, message, history_event = ingest_single_mock_html_record(
        mock_url_text=mock_url_text,
        html_text=html_text,
        method="pasted_html",
        detail="Parsed from pasted page source HTML.",
        status_callback=status_callback,
    )
    append_ingestion_history([history_event])
    if not ok:
        st.cache_data.clear()
        return False, message

    rebuild_ok, rebuild_message = run_current_output_rebuilds(status_callback=status_callback)
    st.cache_data.clear()
    if rebuild_ok:
        return True, "\n\n".join(part for part in [message, rebuild_message] if part)
    return False, f"{message}\n\nRebuild failed after ingest:\n{rebuild_message}"


def ingest_uploaded_html_files(uploaded_files: list[object], status_callback=None) -> tuple[bool, str]:
    if not uploaded_files:
        return False, "No HTML files were uploaded."

    history_events: list[dict[str, object]] = []
    success_messages: list[str] = []
    failed_messages: list[str] = []

    for index, uploaded_file in enumerate(uploaded_files, start=1):
        filename = getattr(uploaded_file, "name", f"file_{index}.html")
        html_text = uploaded_file.getvalue().decode("utf-8", errors="replace")

        def update_prefixed_status(message: str, *, prefix: str = filename) -> None:
            if status_callback is not None:
                status_callback(f"{prefix}: {message}")

        ok, message, history_event = ingest_single_mock_html_record(
            mock_url_text="",
            html_text=html_text,
            method="uploaded_html",
            detail=f"Parsed from uploaded HTML file: {filename}.",
            status_callback=update_prefixed_status,
        )
        history_events.append(history_event)
        if ok:
            success_messages.append(f"{filename}: {message.splitlines()[0]}")
        else:
            failed_messages.append(f"{filename}: {message}")

    append_ingestion_history(history_events)

    if not success_messages:
        st.cache_data.clear()
        return False, "\n".join(failed_messages) if failed_messages else "No uploaded HTML files were ingested."

    rebuild_ok, rebuild_message = run_current_output_rebuilds(status_callback=status_callback)
    st.cache_data.clear()
    if not rebuild_ok:
        return False, f"Ingested {len(success_messages)} uploaded HTML file(s), but rebuild failed.\n\n{rebuild_message}"

    summary_parts = [f"Ingested {len(success_messages)} uploaded HTML file(s)."]
    summary_parts.extend(success_messages[:10])
    if failed_messages:
        summary_parts.append("")
        summary_parts.append("Some uploads failed:")
        summary_parts.extend(failed_messages[:10])
    if rebuild_message:
        summary_parts.append("")
        summary_parts.append(rebuild_message)
    return True, "\n".join(summary_parts)


@st.cache_data(show_spinner=False)
def load_historical_author_seasons() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for year in HISTORICAL_YEARS:
        path = PROCESSED_DIR / str(year) / "mock-drafts__author_accuracy.csv"
        if not path.exists():
            continue
        df = pd.read_csv(path)
        if df.empty:
            continue
        df["year"] = year
        frames.append(df)

    if not frames:
        raise FileNotFoundError("No historical author accuracy files were found.")

    historical = pd.concat(frames, ignore_index=True)
    historical = historical[~historical["author_name"].fillna("").str.strip().str.lower().isin(GENERIC_AUTHORS)]
    historical["season_average_score"] = historical.groupby("year")["avg_custom_accuracy_score"].transform("mean")
    historical["season_score_edge"] = (
        historical["avg_custom_accuracy_score"] - historical["season_average_score"]
    )
    historical["season_above_average"] = historical["season_score_edge"] > 0
    return historical


@st.cache_data(show_spinner=False)
def load_historical_team_author_seasons() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for year in HISTORICAL_YEARS:
        path = PROCESSED_DIR / str(year) / "teams__author_accuracy.csv"
        if not path.exists():
            continue
        df = pd.read_csv(path)
        if df.empty:
            continue
        df["year"] = year
        frames.append(df)

    if not frames:
        raise FileNotFoundError("No historical team-mock author accuracy files were found.")

    historical = pd.concat(frames, ignore_index=True)
    historical = historical[~historical["author_name"].fillna("").str.strip().str.lower().isin(GENERIC_AUTHORS)]
    historical["season_average_score"] = historical.groupby("year")["avg_custom_accuracy_score"].transform("mean")
    historical["season_score_edge"] = (
        historical["avg_custom_accuracy_score"] - historical["season_average_score"]
    )
    historical["season_above_average"] = historical["season_score_edge"] > 0
    return historical


@st.cache_data(show_spinner=False)
def load_historical_team_author_team_seasons() -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for year in HISTORICAL_YEARS:
        metadata_path = PROCESSED_DIR / str(year) / "teams__mock_metadata.csv"
        accuracy_path = PROCESSED_DIR / str(year) / "teams__mock_accuracy.csv"
        if not metadata_path.exists() or not accuracy_path.exists():
            continue
        metadata = pd.read_csv(
            metadata_path,
            usecols=["mock_relative_url", "author_name", "source_team_slug", "source_team_name"],
        )
        accuracy = pd.read_csv(accuracy_path)
        if metadata.empty or accuracy.empty:
            continue
        merged = accuracy.merge(metadata, on=["mock_relative_url", "author_name"], how="left")
        merged = merged.dropna(subset=["source_team_slug"]).copy()
        yearly = (
            merged.groupby(["author_name", "source_team_slug", "source_team_name"], dropna=False)
            .agg(
                mocks_scraped=("mock_relative_url", "count"),
                avg_custom_accuracy_score=("custom_accuracy_score", "mean"),
                correct_player_in_round_matches=("correct_player_in_round_matches", "sum"),
                same_position_plus_minus_one_round_matches=("same_position_plus_minus_one_round_matches", "sum"),
            )
            .reset_index()
        )
        yearly["year"] = year
        frames.append(yearly)

    if not frames:
        return pd.DataFrame()

    historical = pd.concat(frames, ignore_index=True)
    historical = historical[
        ~historical["author_name"].fillna("").str.strip().str.lower().isin(GENERIC_AUTHORS)
    ].copy()
    historical["author_name_norm"] = historical["author_name"].map(normalize_author)
    historical["author_team_key"] = (
        historical["author_name_norm"].fillna("")
        + "::"
        + historical["source_team_slug"].fillna("").astype(str).str.strip().str.lower()
    )
    historical["season_average_score"] = historical.groupby("year")["avg_custom_accuracy_score"].transform("mean")
    historical["season_score_edge"] = (
        historical["avg_custom_accuracy_score"] - historical["season_average_score"]
    )
    historical["season_above_average"] = historical["season_score_edge"] > 0
    return historical


@st.cache_data(show_spinner=False)
def load_current_picks() -> pd.DataFrame:
    archive_metadata_path, archive_picks_path = current_cycle_archive_paths("mock-drafts")
    path = archive_picks_path if archive_picks_path.exists() else PROCESSED_DIR / str(CURRENT_YEAR) / "mock-drafts__mock_picks.csv"
    if not path.exists():
        raise FileNotFoundError(f"Current picks file not found: {path}")

    picks = pd.read_csv(path)
    metadata_path = archive_metadata_path if archive_metadata_path.exists() else PROCESSED_DIR / str(CURRENT_YEAR) / "mock-drafts__mock_metadata.csv"
    if metadata_path.exists():
        metadata = pd.read_csv(metadata_path)
        _, picks = dedupe_current_cycle_section(metadata, picks, section="mock-drafts")
    picks["published_dt"] = pd.to_datetime(picks["published_at"], format="%m/%d/%y", errors="coerce")
    picks["pick"] = pd.to_numeric(picks["pick"], errors="coerce")
    picks["round_number"] = pd.to_numeric(picks["round_number"], errors="coerce")
    picks = picks[picks["round_number"] == 1].copy()
    return picks


@st.cache_data(show_spinner=False)
def load_current_team_mock_data() -> tuple[pd.DataFrame, pd.DataFrame]:
    section_frames: list[tuple[str, pd.DataFrame, pd.DataFrame]] = []
    for section in ("team-mock-drafts", "teams"):
        metadata_archive_path, picks_archive_path = current_cycle_archive_paths(section)
        metadata_path = (
            metadata_archive_path
            if metadata_archive_path.exists()
            else PROCESSED_DIR / str(CURRENT_YEAR) / f"{section}__mock_metadata.csv"
        )
        picks_path = (
            picks_archive_path
            if picks_archive_path.exists()
            else PROCESSED_DIR / str(CURRENT_YEAR) / f"{section}__mock_picks.csv"
        )
        if not metadata_path.exists() or not picks_path.exists():
            continue

        metadata = pd.read_csv(metadata_path)
        picks = pd.read_csv(picks_path)
        metadata, picks = ensure_team_mock_source_fields(metadata, picks, section=section)
        metadata["source_section"] = section
        picks["source_section"] = section
        section_frames.append((section, metadata, picks))

    if not section_frames:
        return pd.DataFrame(), pd.DataFrame()

    metadata = pd.concat([frame[1] for frame in section_frames], ignore_index=True)
    picks = pd.concat([frame[2] for frame in section_frames], ignore_index=True)

    if "mock_relative_url" in metadata.columns:
        metadata = (
            metadata.sort_values(
                by=["source_section", "published_at", "mock_relative_url"],
                ascending=[True, False, True],
                na_position="last",
            )
            .drop_duplicates(subset=["mock_relative_url"], keep="first")
            .copy()
        )
    kept_urls = set(metadata["mock_relative_url"].dropna().astype(str))
    if kept_urls:
        picks = picks[picks["mock_relative_url"].astype(str).isin(kept_urls)].copy()
    else:
        picks = picks.iloc[0:0].copy()

    metadata, picks = dedupe_current_cycle_section(metadata, picks, section="team-mock-drafts")
    for frame in (metadata, picks):
        frame["published_dt"] = pd.to_datetime(frame["published_at"], format="%m/%d/%y", errors="coerce")
    picks["round_number"] = pd.to_numeric(picks["round_number"], errors="coerce")
    return metadata, picks


@st.cache_data(show_spinner=False)
def load_team_specialists() -> pd.DataFrame:
    matches = sorted(PROCESSED_DIR.glob("historical_team_author_accuracy_*.csv"))
    if not matches:
        return pd.DataFrame()

    team_author = pd.read_csv(matches[-1])
    if "team_specialist_weight" not in team_author.columns:
        team_author["team_specialist_weight"] = team_author["team_specific_score"] / 100.0
    team_author["author_name_norm"] = team_author["author_name"].map(normalize_author)
    return team_author


def build_qualified_authors(
    historical: pd.DataFrame,
    *,
    min_years: int,
    min_edge: float,
    min_above_avg_years: int,
    require_all_years_above: bool,
) -> pd.DataFrame:
    summary = (
        historical.groupby("author_name", dropna=False)
        .agg(
            years_covered=("year", "nunique"),
            mocks_scraped=("mocks_scraped", "sum"),
            avg_historical_score=("avg_custom_accuracy_score", "mean"),
            avg_season_edge=("season_score_edge", "mean"),
            median_season_edge=("season_score_edge", "median"),
            min_season_edge=("season_score_edge", "min"),
            max_season_edge=("season_score_edge", "max"),
            seasons_above_avg=("season_above_average", "sum"),
        )
        .reset_index()
    )
    summary["author_name_norm"] = summary["author_name"].map(normalize_author)
    summary["above_avg_rate"] = summary["seasons_above_avg"] / summary["years_covered"]
    summary["qualified"] = (
        (summary["years_covered"] >= min_years)
        & (summary["avg_season_edge"] >= min_edge)
        & (summary["seasons_above_avg"] >= min_above_avg_years)
    )
    if require_all_years_above:
        summary["qualified"] = summary["qualified"] & (summary["min_season_edge"] >= min_edge)

    summary["author_weight"] = 1.0 + summary["avg_season_edge"].clip(lower=0.0) / 10.0
    summary["author_weight"] = summary["author_weight"] * (
        summary["avg_historical_score"] / summary["avg_historical_score"].max()
    )
    return summary.sort_values(
        by=["qualified", "avg_season_edge", "avg_historical_score", "years_covered"],
        ascending=[False, False, False, False],
    )


def build_qualified_team_author_pairs(
    historical: pd.DataFrame,
    *,
    min_years: int,
    min_edge: float,
    min_above_avg_years: int,
    require_all_years_above: bool,
) -> pd.DataFrame:
    if historical.empty:
        return pd.DataFrame()

    summary = (
        historical.groupby(
            ["author_team_key", "author_name", "author_name_norm", "source_team_slug", "source_team_name"],
            dropna=False,
        )
        .agg(
            years_covered=("year", "nunique"),
            mocks_scraped=("mocks_scraped", "sum"),
            avg_historical_score=("avg_custom_accuracy_score", "mean"),
            avg_season_edge=("season_score_edge", "mean"),
            median_season_edge=("season_score_edge", "median"),
            min_season_edge=("season_score_edge", "min"),
            max_season_edge=("season_score_edge", "max"),
            seasons_above_avg=("season_above_average", "sum"),
            player_team_round_matches=("correct_player_in_round_matches", "sum"),
            position_plus_minus_one_round_matches=("same_position_plus_minus_one_round_matches", "sum"),
        )
        .reset_index()
    )
    summary["above_avg_rate"] = summary["seasons_above_avg"] / summary["years_covered"]
    summary["qualified"] = (
        (summary["years_covered"] >= min_years)
        & (summary["avg_season_edge"] >= min_edge)
        & (summary["seasons_above_avg"] >= min_above_avg_years)
    )
    if require_all_years_above:
        summary["qualified"] = summary["qualified"] & (summary["min_season_edge"] >= min_edge)

    summary["author_weight"] = 1.0 + summary["avg_season_edge"].clip(lower=0.0) / 10.0
    max_score = summary["avg_historical_score"].max()
    if pd.notna(max_score) and max_score > 0:
        summary["author_weight"] = summary["author_weight"] * (
            summary["avg_historical_score"] / max_score
        )
    summary["author_team_label"] = (
        summary["author_name"].fillna("").astype(str) + " | " + summary["source_team_name"].fillna("").astype(str)
    )
    return summary.sort_values(
        by=["qualified", "avg_season_edge", "avg_historical_score", "years_covered"],
        ascending=[False, False, False, False],
    )


def apply_manual_include_overrides(
    qualified_authors: pd.DataFrame,
    *,
    state_key: str = "manual_author_include",
    key_column: str = "author_name_norm",
) -> pd.DataFrame:
    overrides = st.session_state.get(state_key, {})
    qualified_authors = qualified_authors.copy()
    qualified_authors["manual_include"] = qualified_authors[key_column].map(
        lambda key: bool(overrides.get(key, False))
    )
    qualified_authors["effective_qualified"] = (
        qualified_authors["qualified"] | qualified_authors["manual_include"]
    )
    return qualified_authors


def build_current_view(
    current_picks: pd.DataFrame,
    qualified_authors: pd.DataFrame,
    team_specialists: pd.DataFrame,
) -> pd.DataFrame:
    qualified = qualified_authors[qualified_authors["effective_qualified"]].copy()
    if qualified.empty:
        return pd.DataFrame()

    qualified_lookup = qualified[
        ["author_name", "author_name_norm", "author_weight", "avg_season_edge", "avg_historical_score", "years_covered"]
    ].copy()
    current = current_picks.copy()
    current["author_name_norm"] = current["author_name"].map(normalize_author)
    current = current.merge(
        qualified_lookup,
        on=["author_name", "author_name_norm"],
        how="inner",
    )

    if not team_specialists.empty:
        current = current.merge(
            team_specialists[
                [
                    "team_slug",
                    "author_name_norm",
                    "team_specialist_weight",
                    "team_specific_score",
                    "attempts",
                    "years_covered",
                ]
            ].rename(
                columns={
                    "attempts": "team_specialist_attempts",
                    "years_covered": "team_specialist_years",
                }
            ),
            on=["team_slug", "author_name_norm"],
            how="left",
        )
    else:
        current["team_specialist_weight"] = pd.NA
        current["team_specific_score"] = pd.NA
        current["team_specialist_attempts"] = pd.NA
        current["team_specialist_years"] = pd.NA

    current["overall_weight"] = current["author_weight"]
    current["team_weight"] = current["overall_weight"] * current["team_specialist_weight"].fillna(1.0)
    return current


def build_pick_candidates(current: pd.DataFrame, weight_column: str) -> pd.DataFrame:
    slot_teams = (
        current.groupby("pick", dropna=False)
        .agg(
            slot_team_name=("team_name", mode_or_first),
            slot_team_color=("team_color", mode_or_first),
        )
        .reset_index()
    )
    pick_summary = (
        current.groupby(["pick", "player_name", "player_position"], dropna=False)
        .agg(
            weighted_score=(weight_column, "sum"),
            raw_count=("mock_relative_url", "count"),
            unique_authors=("author_name", "nunique"),
            avg_author_edge=("avg_season_edge", "mean"),
            college_name=("college_name", mode_or_first),
        )
        .reset_index()
    )
    pick_totals = current.groupby("pick", dropna=False).agg(total_weight=(weight_column, "sum")).reset_index()
    pick_summary = pick_summary.merge(pick_totals, on="pick", how="left")
    pick_summary = pick_summary.merge(slot_teams, on="pick", how="left")
    pick_summary["pick_share"] = pick_summary["weighted_score"] / pick_summary["total_weight"]
    return pick_summary.sort_values(
        by=["pick", "weighted_score", "raw_count", "avg_author_edge"],
        ascending=[True, False, False, False],
    )


def build_consensus_first_round(pick_candidates: pd.DataFrame) -> pd.DataFrame:
    selected_players: set[str] = set()
    rows: list[dict[str, object]] = []

    for pick in sorted(pick_candidates["pick"].dropna().astype(int).unique()):
        candidates = pick_candidates[pick_candidates["pick"] == pick].copy()
        candidates = candidates.sort_values(
            by=["weighted_score", "raw_count", "avg_author_edge"],
            ascending=[False, False, False],
        )
        selected = None
        for _, candidate in candidates.iterrows():
            if candidate["player_name"] not in selected_players:
                selected = candidate
                break
        if selected is None and not candidates.empty:
            selected = candidates.iloc[0]
        if selected is None:
            continue

        selected_players.add(str(selected["player_name"]))
        runner_up = candidates.iloc[1]["player_name"] if len(candidates) > 1 else None
        rows.append(
            {
                "pick": int(selected["pick"]),
                "team_name": selected["slot_team_name"],
                "player_name": selected["player_name"],
                "player_position": selected["player_position"],
                "weighted_score": selected["weighted_score"],
                "pick_share": selected["pick_share"],
                "raw_count": int(selected["raw_count"]),
                "unique_authors": int(selected["unique_authors"]),
                "runner_up": runner_up,
            }
        )

    return pd.DataFrame(rows)


def build_consensus_board_rows(pick_candidates: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for pick in sorted(pick_candidates["pick"].dropna().astype(int).unique()):
        candidates = (
            pick_candidates[pick_candidates["pick"] == pick]
            .sort_values(
                by=["weighted_score", "pick_share", "raw_count", "avg_author_edge"],
                ascending=[False, False, False, False],
            )
            .head(3)
            .reset_index(drop=True)
        )
        if candidates.empty:
            continue

        row: dict[str, object] = {
            "pick": pick,
            "team_name": candidates.iloc[0]["slot_team_name"],
            "team_color": candidates.iloc[0]["slot_team_color"],
        }
        for idx in range(3):
            prefix = f"choice_{idx + 1}"
            if idx < len(candidates):
                candidate = candidates.iloc[idx]
                row[f"{prefix}_player_name"] = candidate["player_name"]
                row[f"{prefix}_player_position"] = candidate["player_position"]
                row[f"{prefix}_college_name"] = candidate["college_name"]
                row[f"{prefix}_share"] = candidate["pick_share"]
                row[f"{prefix}_weighted_score"] = candidate["weighted_score"]
            else:
                row[f"{prefix}_player_name"] = None
                row[f"{prefix}_player_position"] = None
                row[f"{prefix}_college_name"] = None
                row[f"{prefix}_share"] = None
                row[f"{prefix}_weighted_score"] = None
        rows.append(row)
    return pd.DataFrame(rows)


def render_choice_cell(
    player_name: object,
    player_position: object,
    college_name: object,
    share: object,
    weighted_score: object,
) -> str:
    if pd.isna(player_name) or player_name is None:
        return '<div style="color:#6b7280;font-size:13px;font-weight:600;">N/A</div>'

    position_text = html.escape(str(player_position or ""))
    college_text = html.escape(str(college_name or ""))
    detail_text = ", ".join(part for part in [position_text, college_text] if part)
    share_text = f"{float(share) * 100:.0f}%" if pd.notna(share) else "N/A"
    score_text = f"Score {float(weighted_score):.1f}" if pd.notna(weighted_score) else "Score N/A"
    return (
        '<div style="display:flex;justify-content:space-between;gap:12px;align-items:flex-start;">'
        '<div>'
        f'<div style="font-weight:700;color:#2563eb;margin-bottom:2px;">{html.escape(str(player_name))}</div>'
        f'<div style="font-size:12px;color:#111827;margin-bottom:3px;">{detail_text}</div>'
        f'<div style="font-size:11px;color:#4b5563;font-weight:600;">{score_text}</div>'
        "</div>"
        f'<div style="font-weight:800;font-size:15px;white-space:nowrap;color:#111827;">{share_text}</div>'
        "</div>"
    )


def render_consensus_board(board_rows: pd.DataFrame) -> None:
    if board_rows.empty:
        st.info("No consensus board rows are available for the current filter.")
        return

    html_rows: list[str] = []
    for _, row in board_rows.iterrows():
        team_color = row["team_color"] if pd.notna(row["team_color"]) and row["team_color"] else "#d1d5db"
        html_rows.append(
            "<tr>"
            f'<td style="padding:14px 10px;border-bottom:1px solid #e5e7eb;background:{html.escape(str(team_color))};color:white;font-weight:800;text-align:center;width:60px;">{int(row["pick"])}</td>'
            f'<td style="padding:14px 16px;border-bottom:1px solid #e5e7eb;font-weight:700;width:170px;color:#111827;background:#ffffff;">{html.escape(str(row["team_name"]))}</td>'
            f'<td style="padding:14px 16px;border-bottom:1px solid #e5e7eb;background:#ffffff;color:#111827;">{render_choice_cell(row["choice_1_player_name"], row["choice_1_player_position"], row["choice_1_college_name"], row["choice_1_share"], row["choice_1_weighted_score"])}</td>'
            f'<td style="padding:14px 16px;border-bottom:1px solid #e5e7eb;background:#ffffff;color:#111827;">{render_choice_cell(row["choice_2_player_name"], row["choice_2_player_position"], row["choice_2_college_name"], row["choice_2_share"], row["choice_2_weighted_score"])}</td>'
            f'<td style="padding:14px 16px;border-bottom:1px solid #e5e7eb;background:#ffffff;color:#111827;">{render_choice_cell(row["choice_3_player_name"], row["choice_3_player_position"], row["choice_3_college_name"], row["choice_3_share"], row["choice_3_weighted_score"])}</td>'
            "</tr>"
        )

    board_html = (
        '<div style="border:1px solid #d1d5db;border-radius:10px;overflow:hidden;background:#ffffff;">'
        '<table style="width:100%;border-collapse:collapse;font-size:14px;background:#ffffff;color:#111827;">'
        "<thead>"
        '<tr style="background:#e5e7eb;text-align:left;color:#111827;">'
        '<th style="padding:12px 10px;width:60px;color:#111827;">PICK</th>'
        '<th style="padding:12px 16px;width:170px;color:#111827;">TEAM</th>'
        '<th style="padding:12px 16px;color:#111827;">CONSENSUS</th>'
        '<th style="padding:12px 16px;color:#111827;">2ND CHOICE</th>'
        '<th style="padding:12px 16px;color:#111827;">3RD CHOICE</th>'
        "</tr>"
        "</thead>"
        f"<tbody>{''.join(html_rows)}</tbody>"
        "</table>"
        "</div>"
    )
    st.markdown(board_html, unsafe_allow_html=True)


def build_team_candidates(current: pd.DataFrame) -> pd.DataFrame:
    team_summary = (
        current.groupby(["team_slug", "team_name", "player_name", "player_position"], dropna=False)
        .agg(
            weighted_score=("team_weight", "sum"),
            raw_count=("mock_relative_url", "count"),
            unique_authors=("author_name", "nunique"),
            avg_pick=("pick", "mean"),
            median_pick=("pick", "median"),
            avg_author_edge=("avg_season_edge", "mean"),
        )
        .reset_index()
    )
    team_totals = current.groupby(["team_slug", "team_name"], dropna=False).agg(
        team_total_weight=("team_weight", "sum")
    ).reset_index()
    team_summary = team_summary.merge(team_totals, on=["team_slug", "team_name"], how="left")
    team_summary["team_share"] = team_summary["weighted_score"] / team_summary["team_total_weight"]
    return team_summary.sort_values(
        by=["team_name", "weighted_score", "raw_count", "avg_author_edge"],
        ascending=[True, False, False, False],
    )


def build_team_consensus(team_candidates: pd.DataFrame) -> pd.DataFrame:
    return (
        team_candidates.sort_values(
            by=["team_slug", "weighted_score", "raw_count", "avg_author_edge"],
            ascending=[True, False, False, False],
        )
        .drop_duplicates(subset=["team_slug"], keep="first")
        .sort_values(by=["team_name"])
        .reset_index(drop=True)
    )


def build_team_historical_mocker_view(
    team_specialists: pd.DataFrame,
    current_picks: pd.DataFrame,
) -> pd.DataFrame:
    expected_columns = [
        "team_slug",
        "team_name",
        "author_name",
        "team_specific_score",
        "attempts",
        "years_covered",
        "team_match_rate",
        "has_current_2026_projection",
        "current_2026_player",
        "current_2026_position",
        "current_2026_pick",
        "current_2026_published_at",
        "current_2026_mock_name",
    ]
    if team_specialists.empty:
        return pd.DataFrame(columns=expected_columns)

    specialists = team_specialists.copy()
    specialists = specialists[
        ~specialists["author_name"].fillna("").str.strip().str.lower().isin(GENERIC_AUTHORS)
    ].copy()
    specialists = specialists[specialists["years_covered"] >= 2].copy()

    current_lookup = current_picks.copy()
    current_lookup["author_name_norm"] = current_lookup["author_name"].map(normalize_author)
    current_lookup = (
        current_lookup.sort_values(
            by=["published_dt", "mock_relative_url", "pick"],
            ascending=[False, True, True],
        )
        .groupby(["team_slug", "author_name_norm"], dropna=False)
        .agg(
            current_2026_player=("player_name", "first"),
            current_2026_position=("player_position", "first"),
            current_2026_pick=("pick", "first"),
            current_2026_published_at=("published_at", "first"),
            current_2026_mock_name=("mock_name", "first"),
        )
        .reset_index()
    )

    specialists = specialists.merge(
        current_lookup,
        on=["team_slug", "author_name_norm"],
        how="left",
    )
    specialists["has_current_2026_projection"] = specialists["current_2026_player"].notna()
    specialists = specialists.sort_values(
        by=[
            "team_name",
            "team_specific_score",
            "team_match_rate",
            "attempts",
            "has_current_2026_projection",
        ],
        ascending=[True, False, False, False, False],
    )
    return specialists


def build_player_team_candidates(current: pd.DataFrame) -> pd.DataFrame:
    player_team = (
        current.groupby(["player_name", "player_position", "team_name", "team_slug"], dropna=False)
        .agg(
            weighted_score=("team_weight", "sum"),
            raw_count=("mock_relative_url", "count"),
            unique_authors=("author_name", "nunique"),
            avg_pick=("pick", "mean"),
            median_pick=("pick", "median"),
        )
        .reset_index()
    )
    player_totals = (
        current.groupby(["player_name", "player_position"], dropna=False)
        .agg(player_total_weight=("team_weight", "sum"))
        .reset_index()
    )
    player_team = player_team.merge(
        player_totals,
        on=["player_name", "player_position"],
        how="left",
    )
    player_team["player_team_share"] = player_team["weighted_score"] / player_team["player_total_weight"]
    return player_team.sort_values(
        by=["player_name", "weighted_score", "raw_count", "median_pick"],
        ascending=[True, False, False, True],
    )


def build_player_pick_candidates(current: pd.DataFrame, weight_column: str) -> pd.DataFrame:
    player_pick = (
        current.groupby(["player_name", "player_position", "pick"], dropna=False)
        .agg(
            weighted_score=(weight_column, "sum"),
            raw_count=("mock_relative_url", "count"),
            unique_authors=("author_name", "nunique"),
        )
        .reset_index()
    )
    player_totals = (
        current.groupby(["player_name", "player_position"], dropna=False)
        .agg(player_total_weight=(weight_column, "sum"))
        .reset_index()
    )
    player_pick = player_pick.merge(
        player_totals,
        on=["player_name", "player_position"],
        how="left",
    )
    player_pick["player_pick_share"] = player_pick["weighted_score"] / player_pick["player_total_weight"]
    return player_pick.sort_values(
        by=["player_name", "weighted_score", "raw_count", "pick"],
        ascending=[True, False, False, True],
    )


def build_position_summary(current: pd.DataFrame, weight_column: str) -> pd.DataFrame:
    position_summary = (
        current.groupby(["player_position"], dropna=False)
        .agg(
            weighted_score=(weight_column, "sum"),
            raw_count=("mock_relative_url", "count"),
            unique_authors=("author_name", "nunique"),
            unique_players=("player_name", "nunique"),
            avg_pick=("pick", "mean"),
            median_pick=("pick", "median"),
            earliest_pick=("pick", "min"),
            latest_pick=("pick", "max"),
        )
        .reset_index()
    )
    total_weight = float(current[weight_column].sum()) if not current.empty else 0.0
    total_rows = int(len(current))
    position_summary["round_one_share"] = (
        position_summary["weighted_score"] / total_weight if total_weight > 0 else 0.0
    )
    position_summary["raw_round_one_rate"] = (
        position_summary["raw_count"] / total_rows if total_rows > 0 else 0.0
    )
    return position_summary.sort_values(
        by=["weighted_score", "raw_count", "avg_pick", "player_position"],
        ascending=[False, False, True, True],
    )


def build_position_player_candidates(current: pd.DataFrame, weight_column: str) -> pd.DataFrame:
    position_player = (
        current.groupby(["player_position", "player_name"], dropna=False)
        .agg(
            weighted_score=(weight_column, "sum"),
            raw_count=("mock_relative_url", "count"),
            unique_authors=("author_name", "nunique"),
            avg_pick=("pick", "mean"),
            median_pick=("pick", "median"),
            earliest_pick=("pick", "min"),
            latest_pick=("pick", "max"),
            college_name=("college_name", mode_or_first),
            top_team=("team_name", mode_or_first),
        )
        .reset_index()
    )
    position_totals = (
        current.groupby(["player_position"], dropna=False)
        .agg(position_total_weight=(weight_column, "sum"))
        .reset_index()
    )
    position_player = position_player.merge(position_totals, on=["player_position"], how="left")
    position_player["position_player_share"] = (
        position_player["weighted_score"] / position_player["position_total_weight"]
    )
    return position_player.sort_values(
        by=["player_position", "weighted_score", "raw_count", "avg_pick", "player_name"],
        ascending=[True, False, False, True, True],
    )


def build_position_pick_candidates(current: pd.DataFrame, weight_column: str) -> pd.DataFrame:
    position_pick = (
        current.groupby(["player_position", "pick"], dropna=False)
        .agg(
            weighted_score=(weight_column, "sum"),
            raw_count=("mock_relative_url", "count"),
            unique_authors=("author_name", "nunique"),
            unique_players=("player_name", "nunique"),
            top_player=("player_name", mode_or_first),
            slot_team_name=("team_name", mode_or_first),
        )
        .reset_index()
    )
    position_totals = (
        current.groupby(["player_position"], dropna=False)
        .agg(position_total_weight=(weight_column, "sum"))
        .reset_index()
    )
    position_pick = position_pick.merge(position_totals, on=["player_position"], how="left")
    position_pick["position_pick_share"] = (
        position_pick["weighted_score"] / position_pick["position_total_weight"]
    )
    return position_pick.sort_values(
        by=["player_position", "weighted_score", "raw_count", "pick"],
        ascending=[True, False, False, True],
    )


def weighted_pick_average(frame: pd.DataFrame, weight_column: str) -> float:
    picks = pd.to_numeric(frame.get("pick"), errors="coerce")
    weights = pd.to_numeric(frame.get(weight_column), errors="coerce").fillna(0.0)
    valid = picks.notna()
    picks = picks[valid]
    weights = weights[valid]
    if picks.empty:
        return float("nan")
    total_weight = float(weights.sum())
    if total_weight > 0:
        return float((picks * weights).sum() / total_weight)
    return float(picks.mean())


def get_trend_window_dates(current: pd.DataFrame) -> tuple[list[pd.Timestamp], list[pd.Timestamp]]:
    if current.empty or "published_dt" not in current.columns:
        return [], []
    normalized_dates = (
        pd.to_datetime(current["published_dt"], errors="coerce")
        .dropna()
        .dt.normalize()
        .drop_duplicates()
        .sort_values()
        .tolist()
    )
    if not normalized_dates:
        return [], []
    window_size = max(1, len(normalized_dates) // 2)
    early_dates = normalized_dates[:window_size]
    late_dates = normalized_dates[-window_size:]
    return early_dates, late_dates


def build_player_trend_summary(current: pd.DataFrame, weight_column: str) -> pd.DataFrame:
    if current.empty:
        return pd.DataFrame()

    early_dates, late_dates = get_trend_window_dates(current)
    current = current.copy()
    current["published_dt_norm"] = pd.to_datetime(current["published_dt"], errors="coerce").dt.normalize()

    rows: list[dict[str, object]] = []
    grouped = current.groupby(["player_name", "player_position"], dropna=False)
    early_window_days = max(1, len(early_dates))
    late_window_days = max(1, len(late_dates))
    for (player_name, player_position), group in grouped:
        early_group = group[group["published_dt_norm"].isin(early_dates)].copy()
        late_group = group[group["published_dt_norm"].isin(late_dates)].copy()
        current_avg_pick = weighted_pick_average(group, weight_column)
        early_avg_pick = weighted_pick_average(early_group, weight_column)
        late_avg_pick = weighted_pick_average(late_group, weight_column)
        early_appearance_rate = (
            float(early_group["published_dt_norm"].nunique()) / early_window_days if early_window_days > 0 else 0.0
        )
        late_appearance_rate = (
            float(late_group["published_dt_norm"].nunique()) / late_window_days if late_window_days > 0 else 0.0
        )
        appearance_rate_change = late_appearance_rate - early_appearance_rate
        trend_pick_change = (
            early_avg_pick - late_avg_pick
            if pd.notna(early_avg_pick) and pd.notna(late_avg_pick)
            else float("nan")
        )
        if appearance_rate_change >= 0.15 and (pd.isna(trend_pick_change) or trend_pick_change >= -0.5):
            trend_direction = "Rising"
        elif appearance_rate_change <= -0.15 and (pd.isna(trend_pick_change) or trend_pick_change <= 0.5):
            trend_direction = "Falling"
        elif pd.isna(trend_pick_change):
            trend_direction = "Flat"
        elif trend_pick_change >= 0.5:
            trend_direction = "Rising"
        elif trend_pick_change <= -0.5:
            trend_direction = "Falling"
        else:
            trend_direction = "Flat"
        rows.append(
            {
                "player_name": player_name,
                "player_position": player_position,
                "dates_covered": int(group["published_dt_norm"].nunique()),
                "raw_count": int(len(group)),
                "unique_authors": int(group["author_name"].nunique()),
                "current_weighted_avg_pick": current_avg_pick,
                "early_window_avg_pick": early_avg_pick,
                "late_window_avg_pick": late_avg_pick,
                "trend_pick_change": trend_pick_change,
                "early_appearance_rate": early_appearance_rate,
                "late_appearance_rate": late_appearance_rate,
                "appearance_rate_change": appearance_rate_change,
                "trend_direction": trend_direction,
                "earliest_pick": pd.to_numeric(group["pick"], errors="coerce").min(),
                "latest_pick": pd.to_numeric(group["pick"], errors="coerce").max(),
                "college_name": mode_or_first(group["college_name"]),
                "latest_mock_date": group["published_dt_norm"].max(),
            }
        )

    trend_summary = pd.DataFrame(rows)
    return trend_summary.sort_values(
        by=["appearance_rate_change", "trend_pick_change", "late_window_avg_pick", "raw_count", "player_name"],
        ascending=[False, False, True, False, True],
        na_position="last",
    )


def build_player_daily_trends(current: pd.DataFrame, weight_column: str) -> pd.DataFrame:
    if current.empty:
        return pd.DataFrame()

    current = current.copy()
    current["published_dt_norm"] = pd.to_datetime(current["published_dt"], errors="coerce").dt.normalize()
    rows: list[dict[str, object]] = []
    grouped = current.groupby(["player_name", "player_position", "published_dt_norm"], dropna=False)
    for (player_name, player_position, published_dt_norm), group in grouped:
        rows.append(
            {
                "player_name": player_name,
                "player_position": player_position,
                "published_dt": published_dt_norm,
                "published_at": published_dt_norm.strftime("%Y-%m-%d") if pd.notna(published_dt_norm) else "",
                "weighted_avg_pick": weighted_pick_average(group, weight_column),
                "raw_count": int(len(group)),
                "unique_authors": int(group["author_name"].nunique()),
                "top_team": mode_or_first(group["team_name"]),
            }
        )

    return pd.DataFrame(rows).sort_values(
        by=["player_name", "published_dt"],
        ascending=[True, True],
        na_position="last",
    )


def build_team_full_mock_summary(
    team_metadata: pd.DataFrame,
    team_picks: pd.DataFrame,
    qualified_authors: pd.DataFrame,
) -> pd.DataFrame:
    if team_metadata.empty or team_picks.empty:
        return pd.DataFrame()

    summary = team_metadata.copy()
    summary["author_name_norm"] = summary["author_name"].map(normalize_author)
    qualified_lookup = qualified_authors[
        [
            "author_name_norm",
            "source_team_slug",
            "qualified",
            "manual_include",
            "effective_qualified",
            "years_covered",
            "avg_season_edge",
            "avg_historical_score",
        ]
    ].drop_duplicates(subset=["author_name_norm", "source_team_slug"])
    summary = summary.merge(
        qualified_lookup,
        on=["author_name_norm", "source_team_slug"],
        how="left",
    )
    for column_name in ("qualified", "manual_include", "effective_qualified"):
        summary[column_name] = summary[column_name].fillna(False)

    round_rows = team_picks.copy()
    round_rows["round_number"] = pd.to_numeric(round_rows["round_number"], errors="coerce")
    round_rows = round_rows[round_rows["round_number"].notna()].copy()
    round_rows["round_number"] = round_rows["round_number"].astype(int)
    round_rows["player_cell"] = round_rows.apply(
        lambda row: (
            f"{row['player_name']} ({row['player_position']})"
            if pd.notna(row["player_position"]) and str(row["player_position"]).strip()
            else str(row["player_name"] or "")
        ),
        axis=1,
    )
    round_summary = (
        round_rows.groupby(["mock_relative_url", "round_number"], dropna=False)
        .agg(round_players=("player_cell", lambda values: " | ".join(value for value in values if value)))
        .reset_index()
    )
    if round_summary.empty:
        return summary

    round_summary["round_column"] = round_summary["round_number"].map(lambda value: f"round_{value}")
    round_pivot = (
        round_summary.pivot(index="mock_relative_url", columns="round_column", values="round_players")
        .reset_index()
    )
    round_pivot.columns.name = None
    summary = summary.merge(round_pivot, on="mock_relative_url", how="left")

    for round_number in range(1, 8):
        column_name = f"round_{round_number}"
        if column_name not in summary.columns:
            summary[column_name] = pd.NA

    return summary.sort_values(
        by=["effective_qualified", "published_dt", "author_name", "mock_name"],
        ascending=[False, False, True, True],
    )


def build_team_full_mock_pick_view(
    team_metadata: pd.DataFrame,
    team_picks: pd.DataFrame,
    qualified_authors: pd.DataFrame,
) -> pd.DataFrame:
    if team_metadata.empty or team_picks.empty:
        return pd.DataFrame()

    summary = team_metadata.copy()
    summary["author_name_norm"] = summary["author_name"].map(normalize_author)
    qualified_lookup = qualified_authors[
        ["author_name_norm", "source_team_slug", "qualified", "manual_include", "effective_qualified"]
    ].drop_duplicates(subset=["author_name_norm", "source_team_slug"])
    summary = summary.merge(
        qualified_lookup,
        on=["author_name_norm", "source_team_slug"],
        how="left",
    )
    for column_name in ("qualified", "manual_include", "effective_qualified"):
        summary[column_name] = summary[column_name].fillna(False)

    picks = team_picks.copy()
    picks = picks.merge(
        summary[
            [
                "mock_relative_url",
                "published_dt",
                "qualified",
                "manual_include",
                "effective_qualified",
            ]
        ],
        on="mock_relative_url",
        how="left",
    )
    for column_name in ("qualified", "manual_include", "effective_qualified"):
        picks[column_name] = picks[column_name].fillna(False)
    return picks


def build_team_position_summaries(team_picks: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    if team_picks.empty:
        return pd.DataFrame(), pd.DataFrame()

    team_picks = team_picks.copy()
    team_picks["capital_weight"] = team_picks["round_number"].map(round_capital_weight)

    overall = (
        team_picks.groupby("player_position", dropna=False)
        .agg(
            pick_count=("player_name", "count"),
            unique_mocks=("mock_relative_url", "nunique"),
            unique_authors=("author_name", "nunique"),
            draft_capital_score=("capital_weight", "sum"),
        )
        .reset_index()
    )
    total_picks = overall["pick_count"].sum()
    total_capital = overall["draft_capital_score"].sum()
    overall["overall_share"] = overall["pick_count"] / total_picks if total_picks else 0.0
    overall["capital_share"] = (
        overall["draft_capital_score"] / total_capital if total_capital else 0.0
    )
    overall = overall.sort_values(
        by=["draft_capital_score", "pick_count", "unique_mocks", "player_position"],
        ascending=[False, False, False, True],
        na_position="last",
    )

    by_round = (
        team_picks.groupby(["round_number", "player_position"], dropna=False)
        .agg(
            pick_count=("player_name", "count"),
            unique_mocks=("mock_relative_url", "nunique"),
            unique_authors=("author_name", "nunique"),
            draft_capital_score=("capital_weight", "sum"),
        )
        .reset_index()
    )
    round_totals = (
        team_picks.groupby("round_number", dropna=False)
        .agg(
            round_total_picks=("player_name", "count"),
            round_total_capital=("capital_weight", "sum"),
        )
        .reset_index()
    )
    by_round = by_round.merge(round_totals, on="round_number", how="left")
    by_round["round_share"] = by_round["pick_count"] / by_round["round_total_picks"]
    by_round["capital_share"] = by_round["draft_capital_score"] / by_round["round_total_capital"]
    by_round["capital_share"] = by_round["capital_share"].fillna(0.0)
    by_round = by_round.sort_values(
        by=["round_number", "draft_capital_score", "pick_count", "unique_mocks", "player_position"],
        ascending=[True, False, False, False, True],
        na_position="last",
    )
    return overall, by_round


def build_team_round_player_summary(team_picks: pd.DataFrame) -> pd.DataFrame:
    if team_picks.empty:
        return pd.DataFrame()

    round_player = (
        team_picks.groupby(["round_number", "player_name", "player_position"], dropna=False)
        .agg(
            pick_count=("player_name", "count"),
            unique_mocks=("mock_relative_url", "nunique"),
            unique_authors=("author_name", "nunique"),
            college_name=("college_name", mode_or_first),
        )
        .reset_index()
    )
    round_totals = (
        team_picks.groupby("round_number", dropna=False)
        .agg(round_total_picks=("player_name", "count"))
        .reset_index()
    )
    round_player = round_player.merge(round_totals, on="round_number", how="left")
    round_player["round_share"] = round_player["pick_count"] / round_player["round_total_picks"]
    return round_player.sort_values(
        by=["round_number", "pick_count", "unique_mocks", "player_name"],
        ascending=[True, False, False, True],
        na_position="last",
    )


def team_full_mock_summary_column_config() -> dict[str, object]:
    config: dict[str, object] = {
        "published_at": st.column_config.TextColumn("Date", help="Publish date of the team-specific mock."),
        "author_name": st.column_config.TextColumn("Author", help="Author of the team-specific mock."),
        "avg_historical_score": st.column_config.NumberColumn(
            "Hist.\nScore",
            help="Historical full-team author-team score for this exact author and team pairing, based on the generous scoring model. Blank means we do not have a qualifying historical score for that pairing.",
            format="%.1f",
        ),
        "mock_name": st.column_config.TextColumn("Source", help="Outlet or source name for the team-specific mock."),
        "effective_qualified": st.column_config.CheckboxColumn("Used In\nPool", help="Whether this author is currently in the app's qualified consensus pool after historical filters and any manual include override."),
        "qualified": st.column_config.CheckboxColumn("Auto\nQualified", help="Whether this author automatically passes the current historical best-mocker rules."),
        "selection_count": st.column_config.NumberColumn("Total\nPicks", help="How many picks were captured from this team-specific mock page.", format="%d"),
    }
    for round_number in range(1, 8):
        config[f"round_{round_number}"] = st.column_config.TextColumn(
            f"R{round_number}",
            help=f"Player or players mocked to this team in round {round_number}. Multiple same-round picks are separated by '|'.",
        )
    return config


def team_mock_detail_column_config() -> dict[str, object]:
    return {
        "round_number": st.column_config.NumberColumn("Round", help="Draft round for this pick.", format="%d"),
        "pick_label": st.column_config.TextColumn("Pick\nLabel", help="Round label from the source page, such as R1 or R3."),
        "player_name": st.column_config.TextColumn("Player", help="Prospect selected in this full team mock."),
        "player_position": st.column_config.TextColumn("Pos", help="That player's listed position."),
        "college_name": st.column_config.TextColumn("College", help="College program listed for the player."),
        "traded": st.column_config.TextColumn("Trade", help="Trade annotation from the source when present."),
    }


def team_position_overall_column_config() -> dict[str, object]:
    return {
        "player_position": st.column_config.TextColumn("Pos", help="Position drafted in the selected team's full mock sample."),
        "pick_count": st.column_config.NumberColumn("Total\nPicks", help="How many total picks in the filtered team-mock sample were spent on this position.", format="%d"),
        "overall_share": st.column_config.NumberColumn("Overall\nShare", help="Share of all filtered team-mock picks that went to this position.", format="%.3f"),
        "draft_capital_score": st.column_config.NumberColumn("Capital\nScore", help="Custom draft-capital score for this position using round weights: R1=100, R2=55, R3=30, R4=16, R5=9, R6=5, R7=3. Higher means more early-round investment.", format="%.1f"),
        "capital_share": st.column_config.NumberColumn("Capital\nShare", help="This position's share of the team's total weighted draft capital in the filtered sample.", format="%.3f"),
        "unique_mocks": st.column_config.NumberColumn("Unique\nMocks", help="How many distinct team-mock articles included at least one pick at this position.", format="%d"),
        "unique_authors": st.column_config.NumberColumn("Unique\nAuthors", help="How many different authors used this position at least once in the filtered sample.", format="%d"),
    }


def team_position_by_round_column_config() -> dict[str, object]:
    return {
        "round_number": st.column_config.NumberColumn("Round", help="Draft round for the selected team.", format="%d"),
        "player_position": st.column_config.TextColumn("Pos", help="Position drafted in that round."),
        "pick_count": st.column_config.NumberColumn("Round\nPicks", help="How many filtered pick slots in this round went to this position.", format="%d"),
        "round_share": st.column_config.NumberColumn("Round\nShare", help="Share of the filtered pick slots in this round that went to this position.", format="%.3f"),
        "draft_capital_score": st.column_config.NumberColumn("Capital\nScore", help="Draft-capital score for this position in this round. Because all picks in the same round share the same round weight, this mostly reflects how often the position appears in the round.", format="%.1f"),
        "capital_share": st.column_config.NumberColumn("Capital\nShare", help="Share of that round's weighted capital that went to this position.", format="%.3f"),
        "unique_mocks": st.column_config.NumberColumn("Unique\nMocks", help="How many distinct team-mock articles used this position in this round.", format="%d"),
        "unique_authors": st.column_config.NumberColumn("Unique\nAuthors", help="How many different authors used this position in this round.", format="%d"),
    }


def team_round_player_column_config() -> dict[str, object]:
    return {
        "player_name": st.column_config.TextColumn("Player", help="Prospect mocked to the selected team in this round."),
        "player_position": st.column_config.TextColumn("Pos", help="That player's listed position."),
        "college_name": st.column_config.TextColumn("College", help="College listed for the player."),
        "pick_count": st.column_config.NumberColumn("Round\nHits", help="How many filtered pick slots in this round used this player.", format="%d"),
        "round_share": st.column_config.NumberColumn("Round\nShare", help="Share of all filtered pick slots in this round that went to this player.", format="%.3f"),
        "unique_mocks": st.column_config.NumberColumn("Unique\nMocks", help="How many distinct team-mock articles used this player in this round.", format="%d"),
        "unique_authors": st.column_config.NumberColumn("Unique\nAuthors", help="How many different authors used this player in this round.", format="%d"),
    }


def favorite_picks_by_team_column_config() -> dict[str, object]:
    return {
        "player_name": st.column_config.TextColumn("Player", help="The prospect most often mocked to this team in the current qualified sample."),
        "player_position": st.column_config.TextColumn("Pos", help="That prospect's listed position."),
        "weighted_score": st.column_config.NumberColumn("Weighted\nScore", help="Total weighted support this player gets from the qualified current mockers for this team. Higher means stronger consensus."),
        "team_share": st.column_config.NumberColumn("Team\nShare", help="This player's share of the team's total weighted support. A value of 0.40 means 40% of the weighted team consensus went to this player.", format="%.3f"),
        "raw_count": st.column_config.NumberColumn("Raw\nMocks", help="How many qualified current mocks gave this team this player.", format="%d"),
        "unique_authors": st.column_config.NumberColumn("Unique\nAuthors", help="How many different qualified authors mocked this player to this team.", format="%d"),
        "avg_pick": st.column_config.NumberColumn("Avg\nPick", help="Average draft slot where this player was mocked to this team.", format="%.2f"),
        "median_pick": st.column_config.NumberColumn("Median\nPick", help="Median draft slot where this player was mocked to this team.", format="%.1f"),
    }


def best_team_mockers_column_config() -> dict[str, object]:
    return {
        "author_name": st.column_config.TextColumn("Author", help="The mock drafter."),
        "team_specific_score": st.column_config.NumberColumn("Team\nScore %", help="Historical score for this team using the custom system: 1 point for mocking a round-one player and 2 points for matching the correct player to the team, shown as a percent of max possible points.", format="%.1f"),
        "attempts": st.column_config.NumberColumn("Team\nAttempts", help="How many historical first-round team picks from this author are in the sample for this team.", format="%d"),
        "years_covered": st.column_config.NumberColumn("Years\nCovered", help="How many draft years those historical team picks cover.", format="%d"),
        "team_match_rate": st.column_config.NumberColumn("Team Match\nRate", help="Share of historical attempts where this author matched the actual player to this team.", format="%.3f"),
        "has_current_2026_projection": st.column_config.CheckboxColumn("Has 2026\nProjection", help="Whether this author currently has a recent 2026 mock that gives this team a first-round pick."),
        "current_2026_player": st.column_config.TextColumn("2026\nPlayer", help="The player this author currently mocks to the team in the recent 2026 sample."),
        "current_2026_position": st.column_config.TextColumn("2026\nPos", help="That player's position in the current 2026 mock."),
        "current_2026_pick": st.column_config.NumberColumn("2026\nPick", help="The draft slot where this author currently mocks that player to the team.", format="%.0f"),
        "current_2026_published_at": st.column_config.TextColumn("2026\nDate", help="Publish date of that author's current 2026 mock."),
        "current_2026_mock_name": st.column_config.TextColumn("2026\nSource", help="Outlet or source name for that author's current 2026 mock."),
    }


def consensus_mock_column_config() -> dict[str, object]:
    return {
        "pick": st.column_config.NumberColumn("Pick", help="Draft slot in the consensus first round.", format="%d"),
        "team_name": st.column_config.TextColumn("Team", help="Team currently holding this pick slot in the consensus view."),
        "player_name": st.column_config.TextColumn("Player", help="Consensus player at this slot after resolving duplicate players across the round."),
        "player_position": st.column_config.TextColumn("Pos", help="The player's listed position."),
        "weighted_score": st.column_config.NumberColumn("Weighted\nScore", help="Total weighted support this player received at this pick slot from qualified mockers."),
        "pick_share": st.column_config.NumberColumn("Pick\nShare", help="This player's share of total weighted support at this pick slot.", format="%.3f"),
        "raw_count": st.column_config.NumberColumn("Raw\nMocks", help="How many qualified current mocks placed this player at this pick slot.", format="%d"),
        "unique_authors": st.column_config.NumberColumn("Unique\nAuthors", help="How many different qualified authors placed this player at this pick slot.", format="%d"),
        "runner_up": st.column_config.TextColumn("Runner\nUp", help="The next-best candidate at this pick slot before the unique-player round board was finalized."),
    }


def team_consensus_column_config() -> dict[str, object]:
    return {
        "team_name": st.column_config.TextColumn("Team", help="NFL team in the current consensus sample."),
        "player_name": st.column_config.TextColumn("Player", help="Top weighted player for this team among qualified current mockers."),
        "player_position": st.column_config.TextColumn("Pos", help="That player's listed position."),
        "weighted_score": st.column_config.NumberColumn("Weighted\nScore", help="Total weighted support this player gets for this team."),
        "team_share": st.column_config.NumberColumn("Team\nShare", help="This player's share of the team's weighted support.", format="%.3f"),
        "raw_count": st.column_config.NumberColumn("Raw\nMocks", help="How many qualified current mocks gave this team this player.", format="%d"),
        "avg_pick": st.column_config.NumberColumn("Avg\nPick", help="Average pick slot where this player was mocked to this team.", format="%.2f"),
    }


def by_pick_column_config() -> dict[str, object]:
    return {
        "slot_team_name": st.column_config.TextColumn("Slot\nTeam", help="Team most commonly attached to this pick slot in the current sample."),
        "player_name": st.column_config.TextColumn("Player", help="Prospect mocked at this pick slot."),
        "player_position": st.column_config.TextColumn("Pos", help="That player's listed position."),
        "weighted_score": st.column_config.NumberColumn("Weighted\nScore", help="Total weighted support for this player at the selected pick slot."),
        "pick_share": st.column_config.NumberColumn("Pick\nShare", help="This player's share of weighted support at the selected pick slot.", format="%.3f"),
        "raw_count": st.column_config.NumberColumn("Raw\nMocks", help="How many qualified current mocks placed this player at the selected pick slot.", format="%d"),
        "unique_authors": st.column_config.NumberColumn("Unique\nAuthors", help="How many different qualified authors placed this player at the selected pick slot.", format="%d"),
    }


def player_team_column_config() -> dict[str, object]:
    return {
        "team_name": st.column_config.TextColumn("Team", help="Team this player is being mocked to."),
        "weighted_score": st.column_config.NumberColumn("Weighted\nScore", help="Total team-weighted support this player gets for this team."),
        "player_team_share": st.column_config.NumberColumn("Player-Team\nShare", help="This team's share of the player's total weighted support across all teams.", format="%.3f"),
        "raw_count": st.column_config.NumberColumn("Raw\nMocks", help="How many qualified current mocks send this player to this team.", format="%d"),
        "unique_authors": st.column_config.NumberColumn("Unique\nAuthors", help="How many different qualified authors send this player to this team.", format="%d"),
        "avg_pick": st.column_config.NumberColumn("Avg\nPick", help="Average draft slot where this player goes to this team.", format="%.2f"),
        "median_pick": st.column_config.NumberColumn("Median\nPick", help="Median draft slot where this player goes to this team.", format="%.1f"),
    }


def player_pick_column_config() -> dict[str, object]:
    return {
        "pick": st.column_config.NumberColumn("Pick", help="Draft slot where this player is being mocked.", format="%d"),
        "weighted_score": st.column_config.NumberColumn("Weighted\nScore", help="Total weighted support this player gets at this pick slot."),
        "player_pick_share": st.column_config.NumberColumn("Player-Pick\nShare", help="This pick slot's share of the player's total weighted support across all pick slots.", format="%.3f"),
        "raw_count": st.column_config.NumberColumn("Raw\nMocks", help="How many qualified current mocks place this player at this pick.", format="%d"),
        "unique_authors": st.column_config.NumberColumn("Unique\nAuthors", help="How many different qualified authors place this player at this pick.", format="%d"),
    }


def player_detail_column_config() -> dict[str, object]:
    return {
        "published_at": st.column_config.TextColumn("Date", help="Publish date of the recent current-year mock."),
        "author_name": st.column_config.TextColumn("Author", help="Author of the mock."),
        "mock_name": st.column_config.TextColumn("Source", help="Outlet or source name for the mock."),
        "team_name": st.column_config.TextColumn("Team", help="Team this mock sends the player to."),
        "pick": st.column_config.NumberColumn("Pick", help="Pick slot used in that mock.", format="%.0f"),
        "player_position": st.column_config.TextColumn("Pos", help="The player's listed position."),
        "overall_weight": st.column_config.NumberColumn("Overall\nWeight", help="Author weight from historical above-average performance.", format="%.3f"),
        "team_weight": st.column_config.NumberColumn("Team\nWeight", help="Author weight after applying team-specific specialist history when available.", format="%.3f"),
    }


def pick_detail_column_config() -> dict[str, object]:
    return {
        "published_at": st.column_config.TextColumn("Date", help="Publish date of the recent current-year mock."),
        "author_name": st.column_config.TextColumn("Author", help="Author of the mock."),
        "mock_name": st.column_config.TextColumn("Source", help="Outlet or source name for the mock."),
        "player_name": st.column_config.TextColumn("Player", help="Player this mock drafter has at the selected pick."),
        "player_position": st.column_config.TextColumn("Pos", help="That player's listed position."),
        "team_name": st.column_config.TextColumn("Team", help="Team holding the selected pick in that mock."),
        "pick": st.column_config.NumberColumn("Pick", help="Pick slot used in that mock.", format="%.0f"),
        "active_weight": st.column_config.NumberColumn("Current\nWeight", help="Weight currently used by the app for this mock under the selected first-round consensus weighting mode.", format="%.3f"),
    }


def position_summary_column_config() -> dict[str, object]:
    return {
        "player_position": st.column_config.TextColumn("Pos", help="Position group in the current qualified first-round mock sample."),
        "weighted_score": st.column_config.NumberColumn("Weighted\nScore", help="Total weighted support this position gets across all qualified first-round mocks.", format="%.3f"),
        "round_one_share": st.column_config.NumberColumn("Weighted\nShare", help="This position's share of total weighted first-round support across the sample.", format="%.3f"),
        "raw_count": st.column_config.NumberColumn("Raw\nHits", help="How many qualified first-round mock rows use this position.", format="%d"),
        "raw_round_one_rate": st.column_config.NumberColumn("Raw\nRate", help="Share of all qualified first-round mock rows that use this position.", format="%.3f"),
        "unique_authors": st.column_config.NumberColumn("Unique\nAuthors", help="How many different qualified authors mock at least one player from this position in round one.", format="%d"),
        "unique_players": st.column_config.NumberColumn("Unique\nPlayers", help="How many different players from this position appear in the round-one sample.", format="%d"),
        "avg_pick": st.column_config.NumberColumn("Avg\nPick", help="Average pick slot where this position is mocked in the first round.", format="%.2f"),
        "median_pick": st.column_config.NumberColumn("Median\nPick", help="Median pick slot where this position is mocked in the first round.", format="%.1f"),
        "earliest_pick": st.column_config.NumberColumn("Earliest\nPick", help="Earliest first-round slot where this position appears in the sample.", format="%d"),
        "latest_pick": st.column_config.NumberColumn("Latest\nPick", help="Latest first-round slot where this position appears in the sample.", format="%d"),
    }


def position_player_column_config() -> dict[str, object]:
    return {
        "player_name": st.column_config.TextColumn("Player", help="Player from the selected position appearing in the current qualified first-round sample."),
        "college_name": st.column_config.TextColumn("College", help="College listed for that player."),
        "top_team": st.column_config.TextColumn("Most Common\nTeam", help="Team this player is most commonly mocked to in the current sample."),
        "weighted_score": st.column_config.NumberColumn("Weighted\nScore", help="Total weighted support for this player within the selected position group.", format="%.3f"),
        "position_player_share": st.column_config.NumberColumn("Pos\nShare", help="This player's share of the selected position's total weighted support.", format="%.3f"),
        "raw_count": st.column_config.NumberColumn("Raw\nHits", help="How many qualified first-round mocks include this player.", format="%d"),
        "unique_authors": st.column_config.NumberColumn("Unique\nAuthors", help="How many different qualified authors include this player.", format="%d"),
        "avg_pick": st.column_config.NumberColumn("Avg\nPick", help="Average pick slot where this player is mocked.", format="%.2f"),
        "median_pick": st.column_config.NumberColumn("Median\nPick", help="Median pick slot where this player is mocked.", format="%.1f"),
        "earliest_pick": st.column_config.NumberColumn("Earliest\nPick", help="Earliest pick where this player appears in the sample.", format="%d"),
        "latest_pick": st.column_config.NumberColumn("Latest\nPick", help="Latest pick where this player appears in the sample.", format="%d"),
    }


def position_pick_column_config() -> dict[str, object]:
    return {
        "pick": st.column_config.NumberColumn("Pick", help="First-round draft slot for the selected position.", format="%d"),
        "slot_team_name": st.column_config.TextColumn("Slot\nTeam", help="Team most commonly attached to this pick slot in the current sample."),
        "top_player": st.column_config.TextColumn("Top\nPlayer", help="Most common player from the selected position at this pick slot."),
        "weighted_score": st.column_config.NumberColumn("Weighted\nScore", help="Total weighted support for the selected position at this pick slot.", format="%.3f"),
        "position_pick_share": st.column_config.NumberColumn("Pos-Pick\nShare", help="This pick slot's share of the selected position's total weighted support.", format="%.3f"),
        "raw_count": st.column_config.NumberColumn("Raw\nHits", help="How many qualified first-round mocks place the selected position at this pick slot.", format="%d"),
        "unique_authors": st.column_config.NumberColumn("Unique\nAuthors", help="How many different qualified authors place the selected position at this pick slot.", format="%d"),
        "unique_players": st.column_config.NumberColumn("Unique\nPlayers", help="How many different players from the selected position appear at this pick slot.", format="%d"),
    }


def position_detail_column_config() -> dict[str, object]:
    return {
        "published_at": st.column_config.TextColumn("Date", help="Publish date of the recent current-year mock."),
        "author_name": st.column_config.TextColumn("Author", help="Author of the mock."),
        "mock_name": st.column_config.TextColumn("Source", help="Outlet or source name for the mock."),
        "player_name": st.column_config.TextColumn("Player", help="Player from the selected position mocked in round one."),
        "team_name": st.column_config.TextColumn("Team", help="Team this mock sends the player to."),
        "pick": st.column_config.NumberColumn("Pick", help="Pick slot used in that mock.", format="%.0f"),
        "overall_weight": st.column_config.NumberColumn("Overall\nWeight", help="Author weight from historical above-average performance.", format="%.3f"),
        "team_weight": st.column_config.NumberColumn("Team\nWeight", help="Author weight after applying team-specific specialist history when available.", format="%.3f"),
    }


def player_trend_summary_column_config() -> dict[str, object]:
    return {
        "player_name": st.column_config.TextColumn("Player", help="Player appearing in the current qualified first-round sample."),
        "player_position": st.column_config.TextColumn("Pos", help="That player's listed position."),
        "college_name": st.column_config.TextColumn("College", help="College listed for the player."),
        "trend_direction": st.column_config.TextColumn("Trend", help="Rising means the player is being mocked earlier and/or appearing in round one more often; falling means later and/or less often."),
        "trend_pick_change": st.column_config.NumberColumn("Pick\nChange", help="Early-window avg pick minus late-window avg pick. Positive means moving earlier in mocks.", format="%.2f"),
        "early_window_avg_pick": st.column_config.NumberColumn("Early Avg\nPick", help="Weighted average pick for this player in the early part of the selected date window.", format="%.2f"),
        "late_window_avg_pick": st.column_config.NumberColumn("Late Avg\nPick", help="Weighted average pick for this player in the late part of the selected date window.", format="%.2f"),
        "early_appearance_rate": st.column_config.NumberColumn("Early R1\nRate", help="Share of early-window dates where this player appears in the qualified round-one sample.", format="%.3f"),
        "late_appearance_rate": st.column_config.NumberColumn("Late R1\nRate", help="Share of late-window dates where this player appears in the qualified round-one sample.", format="%.3f"),
        "appearance_rate_change": st.column_config.NumberColumn("R1 Rate\nChange", help="Late-window round-one appearance rate minus early-window rate. Positive means the player is showing up in round one more often.", format="%.3f"),
        "current_weighted_avg_pick": st.column_config.NumberColumn("Current Avg\nPick", help="Weighted average pick for this player across the whole selected date window.", format="%.2f"),
        "dates_covered": st.column_config.NumberColumn("Dates\nCovered", help="How many unique publish dates this player appears on in the selected window.", format="%d"),
        "raw_count": st.column_config.NumberColumn("Raw\nHits", help="How many qualified round-one mock rows include this player.", format="%d"),
        "unique_authors": st.column_config.NumberColumn("Unique\nAuthors", help="How many different qualified authors include this player.", format="%d"),
        "earliest_pick": st.column_config.NumberColumn("Earliest\nPick", help="Earliest pick slot where this player appears in the selected window.", format="%d"),
        "latest_pick": st.column_config.NumberColumn("Latest\nPick", help="Latest pick slot where this player appears in the selected window.", format="%d"),
    }


def player_daily_trend_column_config() -> dict[str, object]:
    return {
        "published_at": st.column_config.TextColumn("Date", help="Publish date in the current selected window."),
        "weighted_avg_pick": st.column_config.NumberColumn("Weighted Avg\nPick", help="Weighted average pick for this player on that date. Lower means earlier in the first round.", format="%.2f"),
        "raw_count": st.column_config.NumberColumn("Raw\nHits", help="How many qualified mock rows included this player on that date.", format="%d"),
        "unique_authors": st.column_config.NumberColumn("Unique\nAuthors", help="How many different qualified authors included this player on that date.", format="%d"),
        "top_team": st.column_config.TextColumn("Most Common\nTeam", help="Team this player was most commonly mocked to on that date."),
    }


def best_mockers_column_config() -> dict[str, object]:
    return {
        "qualified": st.column_config.CheckboxColumn("Auto\nQualified", help="Whether this author automatically passes the current sidebar historical filter."),
        "manual_include": st.column_config.CheckboxColumn("Manual\nInclude", help="Manual override. Check this to force the author into the current consensus pool even if they do not auto-qualify."),
        "effective_qualified": st.column_config.CheckboxColumn("Used In\nPool", help="Final inclusion status used by the current consensus views after combining auto-qualification and any manual include override."),
        "has_recent_2026_mock": st.column_config.CheckboxColumn("Has Recent\n2026 Mock", help="Whether this author has at least one 2026 mock available in the active date window, regardless of qualification."),
        "currently_used_in_2026_consensus": st.column_config.CheckboxColumn("Used In\n2026 Consensus", help="Whether this author is actually being used in the current 2026 consensus after qualification and manual-include rules are applied."),
        "author_name": st.column_config.TextColumn("Author", help="Mock drafter name."),
        "years_covered": st.column_config.NumberColumn("Years\nCovered", help="How many historical seasons this author appears in.", format="%d"),
        "seasons_above_avg": st.column_config.NumberColumn("Above-Avg\nSeasons", help="How many historical seasons this author finished above that season's average score.", format="%d"),
        "above_avg_rate": st.column_config.NumberColumn("Above-Avg\nRate", help="Share of covered seasons where the author finished above average.", format="%.3f"),
        "avg_historical_score": st.column_config.NumberColumn("Avg Hist.\nScore", help="Average historical mock score across covered seasons.", format="%.2f"),
        "avg_season_edge": st.column_config.NumberColumn("Avg Season\nEdge", help="Average score gap above or below each season's average mocker.", format="%.2f"),
        "min_season_edge": st.column_config.NumberColumn("Min Season\nEdge", help="Worst season-level gap relative to that season's average.", format="%.2f"),
        "author_weight": st.column_config.NumberColumn("Author\nWeight", help="Weight used in the app's consensus views, based on historical edge and score.", format="%.3f"),
    }


def author_latest_mock_column_config() -> dict[str, object]:
    return {
        "pick": st.column_config.NumberColumn("Pick", help="Draft slot in the author's latest 2026 first-round mock.", format="%d"),
        "team_name": st.column_config.TextColumn("Team", help="Team assigned to this pick in the author's latest 2026 mock."),
        "player_name": st.column_config.TextColumn("Player", help="Player mocked at this pick."),
        "player_position": st.column_config.TextColumn("Pos", help="That player's listed position."),
        "college_name": st.column_config.TextColumn("College", help="College listed for the player."),
        "published_at": st.column_config.TextColumn("Date", help="Publish date for the author's latest 2026 mock."),
        "mock_name": st.column_config.TextColumn("Source", help="Outlet/source for the author's latest 2026 mock."),
    }


def ingestion_history_column_config() -> dict[str, object]:
    return {
        "ingested_at": st.column_config.TextColumn("Logged At", help="When this ingest attempt was recorded."),
        "method": st.column_config.TextColumn("Method", help="How the mock was ingested, such as manual URL or pasted HTML."),
        "status": st.column_config.TextColumn("Status", help="Outcome of the ingest event, such as ingested, duplicate, replaced, or failed."),
        "section": st.column_config.TextColumn("Section", help="Which local current-cycle dataset this mock belongs to."),
        "mock_relative_url": st.column_config.TextColumn("Mock URL", help="Relative mock URL recorded for the ingest event."),
        "detail": st.column_config.TextColumn("Detail", help="Extra context about the ingest event."),
    }


def recent_local_mocks_column_config() -> dict[str, object]:
    return {
        "published_at": st.column_config.TextColumn("Date", help="Publish date of the current local mock."),
        "section": st.column_config.TextColumn("Section", help="Whether this is a first-round mock or team mock entry."),
        "author_name": st.column_config.TextColumn("Author", help="Author of the local mock entry."),
        "mock_name": st.column_config.TextColumn("Source", help="Outlet or source name."),
        "source_team_name": st.column_config.TextColumn("Team", help="For team-mock entries, the source team page tied to the mock."),
        "mock_relative_url": st.column_config.TextColumn("Mock URL", help="Relative URL of the mock stored locally."),
    }


def ingestion_status_column_config() -> dict[str, object]:
    return {
        "mock_type": st.column_config.TextColumn("Mock Type", help="Local dataset group shown in this summary."),
        "current_local_mocks": st.column_config.NumberColumn("Current Local\nMocks", help="How many current local mock records are on disk for this mock type.", format="%d"),
        "latest_mock_published_at": st.column_config.TextColumn("Latest Mock\nDate", help="Most recent publish date among locally stored mocks for this type."),
        "latest_mock_author": st.column_config.TextColumn("Latest Mock\nAuthor", help="Author of the most recently published local mock for this type."),
        "latest_mock_source": st.column_config.TextColumn("Latest Mock\nSource", help="Outlet/source of the most recently published local mock for this type."),
        "last_ingested_at": st.column_config.TextColumn("Last\nIngested", help="Most recent local ingest timestamp recorded for this mock type."),
        "last_ingest_method": st.column_config.TextColumn("Last Ingest\nMethod", help="How the most recent ingest for this mock type was added locally."),
        "last_ingest_status": st.column_config.TextColumn("Last Ingest\nStatus", help="Outcome of the most recent ingest event for this mock type."),
    }


def best_team_full_mockers_column_config() -> dict[str, object]:
    return {
        "qualified": st.column_config.CheckboxColumn("Auto\nQual", help="Whether this author automatically passes the current historical full-team-mock filter."),
        "manual_include": st.column_config.CheckboxColumn("Manual\nIn", help="Manual override. Check this to force the author into the current full-team-mock pool even if they do not auto-qualify."),
        "effective_qualified": st.column_config.CheckboxColumn("Team\nPool", help="Final inclusion status used by the current full-team-mock views after combining auto-qualification and any manual include override."),
        "has_recent_2026_team_mock": st.column_config.CheckboxColumn("Recent\n2026", help="Whether this author has at least one 2026 team-specific mock available in the active date window."),
        "currently_used_in_team_full_mock_view": st.column_config.CheckboxColumn("Active\nNow", help="Whether this author is currently contributing to the Team Full Mocks tab after historical qualification and manual include rules are applied."),
        "author_name": st.column_config.TextColumn("Author", help="Mock drafter name."),
        "source_team_name": st.column_config.TextColumn("Team", help="Specific team this historical full-team-mocker row is tied to."),
        "years_covered": st.column_config.NumberColumn("Years", help="How many historical seasons this author appears in for team full mocks.", format="%d"),
        "seasons_above_avg": st.column_config.NumberColumn("Above\nAvg", help="How many historical seasons this author finished above that season's average team-mock score.", format="%d"),
        "player_team_round_matches": st.column_config.NumberColumn("Player\nHits", help="Total times this author matched the correct player to this team in the correct round across the historical sample.", format="%d"),
        "position_plus_minus_one_round_matches": st.column_config.NumberColumn("Pos +/-1\nHits", help="Total times this author matched the team's drafted position within one round of the actual round across the historical sample.", format="%d"),
        "above_avg_rate": st.column_config.NumberColumn("Above\nRate", help="Share of covered seasons where the author finished above average in team full mocks.", format="%.3f"),
        "avg_historical_score": st.column_config.NumberColumn("Hist.\nScore", help="Average historical team-mock score across covered seasons.", format="%.2f"),
        "avg_season_edge": st.column_config.NumberColumn("Avg\nEdge", help="Average score gap above or below each season's average team-mocker.", format="%.2f"),
        "author_weight": st.column_config.NumberColumn("Weight", help="Weight derived from historical team-mock edge and score.", format="%.3f"),
    }


def render_app() -> None:
    st.set_page_config(page_title="NFL Mock Consensus", layout="wide")
    st.title("NFL Mock Consensus")
    st.caption("Current 2026 first-round consensus driven by historically above-average mockers.")
    read_only_mode = is_read_only_mode()

    if "uploaded_html_uploader_nonce" not in st.session_state:
        st.session_state["uploaded_html_uploader_nonce"] = 0
    if st.session_state.pop("clear_manual_mock_url_text", False):
        st.session_state["manual_mock_url_text"] = ""
    if st.session_state.pop("clear_pasted_mock_inputs", False):
        st.session_state["pasted_mock_url"] = ""
        st.session_state["pasted_mock_html"] = ""

    st.sidebar.header("Current Data")
    if read_only_mode:
        st.sidebar.info(
            "Read-only mode is on. Refresh, manual ingest, and manual include editing are hidden."
        )
    last_refresh_started_at_text = st.session_state.get("last_refresh_started_at")
    refresh_cooldown_until: datetime | None = None
    refresh_disabled = False
    if last_refresh_started_at_text:
        try:
            last_refresh_started_at = datetime.fromisoformat(last_refresh_started_at_text)
            refresh_cooldown_until = last_refresh_started_at + timedelta(minutes=REFRESH_COOLDOWN_MINUTES)
            refresh_disabled = datetime.now() < refresh_cooldown_until
        except ValueError:
            st.session_state.pop("last_refresh_started_at", None)
            refresh_cooldown_until = None
            refresh_disabled = False

    st.sidebar.caption(
        f"Refresh cooldown: {REFRESH_COOLDOWN_MINUTES} minutes. "
        f"Refresh scraper pace: {REFRESH_SLEEP_SECONDS:.1f}s between mock-page requests."
    )
    if refresh_disabled and refresh_cooldown_until is not None:
        st.sidebar.info(
            "Refresh is cooling down until "
            + refresh_cooldown_until.strftime("%Y-%m-%d %I:%M %p")
            + "."
        )

    last_refresh = st.session_state.get("last_refresh_result")
    if last_refresh:
        if last_refresh.get("ok", False):
            st.sidebar.success(last_refresh.get("title", "Refresh complete"))
        else:
            st.sidebar.error(last_refresh.get("title", "Refresh failed"))
        if last_refresh.get("highlights"):
            for line in last_refresh["highlights"]:
                st.sidebar.caption(line)
        if last_refresh.get("message"):
            with st.sidebar.expander("Last refresh details", expanded=False):
                st.write(last_refresh["message"])
        if st.sidebar.button("Clear Refresh Result", use_container_width=True):
            st.session_state.pop("last_refresh_result", None)
            st.rerun()
    if not read_only_mode:
        if st.sidebar.button(
            "Pull Fresh 14-Day Mocks",
            use_container_width=True,
            disabled=refresh_disabled,
        ):
            st.session_state["last_refresh_started_at"] = datetime.now().isoformat()
            with st.sidebar:
                with st.status("Checking for new first-round mocks and team mocks...", expanded=True) as status:
                    ok, message = refresh_current_cycle_data(status.write)
                    if ok:
                        highlight_lines = build_refresh_highlight_lines(message)
                        if highlight_lines:
                            status.write("Refresh summary: " + " ".join(highlight_lines))
                        status.update(
                            label="Refresh complete",
                            state="complete",
                            expanded=True,
                        )
                    else:
                        status.update(
                            label="Refresh failed",
                            state="error",
                            expanded=True,
                        )
            st.session_state["last_refresh_result"] = {
                "ok": ok,
                "title": "Refresh complete" if ok else "Refresh failed",
                "highlights": summarize_result_for_sidebar(message, ok=ok),
                "message": message,
            }
            st.rerun()

        with st.sidebar.expander("Add Specific Mock URLs", expanded=False):
            st.caption(
                "Paste one NFL Mock Draft Database URL per line. "
                "This is lighter than a full refresh and is useful when you spot a new mock in your browser."
            )
            manual_url_text = st.text_area(
                "Mock URLs",
                key="manual_mock_url_text",
                height=120,
                placeholder=(
                    "https://www.nflmockdraftdatabase.com/mock-drafts/2026/...\n"
                    "https://www.nflmockdraftdatabase.com/team-mock-drafts/2026/..."
                ),
            )
            manual_urls = extract_manual_mock_urls(manual_url_text)
            if manual_urls:
                st.caption(f"{len(manual_urls)} URL(s) ready to ingest.")
            if st.button(
                "Pull Entered Mock URLs",
                use_container_width=True,
                disabled=not manual_urls,
            ):
                st.session_state["last_refresh_started_at"] = datetime.now().isoformat()
                with st.status("Fetching the manually entered mock URLs...", expanded=True) as status:
                    ok, message = ingest_manual_mock_urls(manual_urls, status.write)
                    if ok:
                        highlight_lines = build_refresh_highlight_lines(message)
                        if highlight_lines:
                            status.write("Refresh summary: " + " ".join(highlight_lines))
                        status.update(
                            label="Manual URL ingest complete",
                            state="complete",
                            expanded=True,
                        )
                    else:
                        status.update(
                            label="Manual URL ingest failed",
                            state="error",
                            expanded=True,
                        )
                st.session_state["last_refresh_result"] = {
                    "ok": ok,
                    "title": "Manual URL ingest complete" if ok else "Manual URL ingest failed",
                    "highlights": summarize_result_for_sidebar(message, ok=ok),
                    "message": message,
                }
                if ok:
                    st.session_state["clear_manual_mock_url_text"] = True
                st.rerun()

        with st.sidebar.expander("Paste Or Upload Mock Page Source", expanded=False):
            st.caption(
                "Fastest fallback when the site blocks scripted requests: paste page source HTML or upload saved .html files. "
                "The app will try to auto-detect the mock URL from the HTML."
            )
            pasted_mock_url = st.text_input(
                "Mock URL For Pasted HTML",
                key="pasted_mock_url",
                placeholder="Optional if the page source contains a canonical mock URL...",
            )
            pasted_html = st.text_area(
                "Page Source HTML",
                key="pasted_mock_html",
                height=220,
                placeholder="Paste the page source HTML from your browser here...",
            )
            detected_pasted_url = extract_mock_url_from_html(pasted_html) if pasted_html.strip() else None
            if pasted_mock_url.strip():
                st.caption(f"Using entered mock URL: {pasted_mock_url.strip()}")
            elif detected_pasted_url:
                st.caption(f"Detected mock URL from HTML: {detected_pasted_url}")
            elif pasted_html.strip():
                st.caption("Could not auto-detect a mock URL from the HTML. Paste the URL above if needed.")
            pasted_ready = bool((pasted_mock_url.strip() or detected_pasted_url) and pasted_html.strip())
            if pasted_ready:
                st.caption("HTML source is ready to ingest.")
            if st.button(
                "Ingest Pasted HTML",
                use_container_width=True,
                disabled=not pasted_ready,
            ):
                with st.status("Parsing pasted mock HTML...", expanded=True) as status:
                    ok, message = ingest_pasted_mock_html(
                        mock_url_text=pasted_mock_url,
                        html_text=pasted_html,
                        status_callback=status.write,
                    )
                    status.update(
                        label="Pasted HTML ingest complete" if ok else "Pasted HTML ingest failed",
                        state="complete" if ok else "error",
                        expanded=True,
                    )
                st.session_state["last_refresh_result"] = {
                    "ok": ok,
                    "title": "Pasted HTML ingest complete" if ok else "Pasted HTML ingest failed",
                    "highlights": summarize_result_for_sidebar(message, ok=ok),
                    "message": message,
                }
                if ok:
                    st.session_state["clear_pasted_mock_inputs"] = True
                st.rerun()

            uploaded_html_files = st.file_uploader(
                "Upload Saved Page Source HTML File(s)",
                type=["html", "htm"],
                accept_multiple_files=True,
                key=f"uploaded_mock_html_files_{st.session_state['uploaded_html_uploader_nonce']}",
            )
            if uploaded_html_files:
                detected_count = 0
                undetected_files: list[str] = []
                for uploaded_file in uploaded_html_files:
                    html_text = uploaded_file.getvalue().decode("utf-8", errors="replace")
                    if extract_mock_url_from_html(html_text):
                        detected_count += 1
                    else:
                        undetected_files.append(uploaded_file.name)
                st.caption(
                    f"{len(uploaded_html_files)} uploaded HTML file(s), {detected_count} with auto-detected mock URLs."
                )
                if undetected_files:
                    st.caption("Need manual attention: " + ", ".join(undetected_files[:5]))
            if st.button(
                "Ingest Uploaded HTML Files",
                use_container_width=True,
                disabled=not uploaded_html_files,
            ):
                with st.status("Parsing uploaded HTML files...", expanded=True) as status:
                    ok, message = ingest_uploaded_html_files(uploaded_html_files, status.write)
                    status.update(
                        label="Uploaded HTML ingest complete" if ok else "Uploaded HTML ingest failed",
                        state="complete" if ok else "error",
                        expanded=True,
                    )
                st.session_state["last_refresh_result"] = {
                    "ok": ok,
                    "title": "Uploaded HTML ingest complete" if ok else "Uploaded HTML ingest failed",
                    "highlights": summarize_result_for_sidebar(message, ok=ok),
                    "message": message,
                }
                if ok:
                    st.session_state["uploaded_html_uploader_nonce"] += 1
                st.rerun()

    historical = load_historical_author_seasons()
    historical_team = load_historical_team_author_team_seasons()
    current_picks = load_current_picks()
    current_team_metadata, current_team_picks = load_current_team_mock_data()
    team_specialists = load_team_specialists()
    all_current_picks = current_picks.copy()
    all_current_team_metadata = current_team_metadata.copy()

    available_dates = sorted(
        set(current_picks["published_dt"].dropna().dt.date.unique()).union(
            set(current_team_metadata["published_dt"].dropna().dt.date.unique())
        )
    )
    if not available_dates:
        st.error("No valid publish dates were found in the current 2026 mock dataset.")
        return
    selected_date_range = st.sidebar.slider(
        "Published date window",
        min_value=available_dates[0],
        max_value=available_dates[-1],
        value=(
            max(available_dates[0], available_dates[-1] - timedelta(days=13)),
            available_dates[-1],
        ),
        format="YYYY-MM-DD",
    )
    selected_start_date, selected_end_date = selected_date_range
    current_picks = current_picks[
        current_picks["published_dt"].dt.date.between(selected_start_date, selected_end_date)
    ].copy()
    if not current_team_metadata.empty:
        current_team_metadata = current_team_metadata[
            current_team_metadata["published_dt"].dt.date.between(selected_start_date, selected_end_date)
        ].copy()
    if not current_team_picks.empty:
        current_team_picks = current_team_picks[
            current_team_picks["published_dt"].dt.date.between(selected_start_date, selected_end_date)
        ].copy()

    st.sidebar.header("Best Mocker Filter")
    min_years = st.sidebar.slider("Min historical years", min_value=0, max_value=6, value=3)
    min_edge = st.sidebar.slider("Min avg season edge", min_value=-10.0, max_value=10.0, value=0.0, step=0.5)
    min_above_avg_years = st.sidebar.slider(
        "Min above-average seasons",
        min_value=0,
        max_value=6,
        value=3,
    )
    require_all_years_above = st.sidebar.checkbox("Require every covered season to beat threshold", value=False)
    pick_weight_mode = st.sidebar.selectbox(
        "First-round consensus weighting",
        options=[
            ("Historical edge", "overall_weight"),
            ("Equal qualified authors", "equal_weight"),
        ],
        index=1,
        format_func=lambda option: option[0],
    )

    qualified_authors = build_qualified_authors(
        historical,
        min_years=min_years,
        min_edge=min_edge,
        min_above_avg_years=min_above_avg_years,
        require_all_years_above=require_all_years_above,
    )
    qualified_authors = apply_manual_include_overrides(qualified_authors, state_key="manual_author_include")
    team_qualified_authors = build_qualified_team_author_pairs(
        historical_team,
        min_years=min_years,
        min_edge=min_edge,
        min_above_avg_years=min_above_avg_years,
        require_all_years_above=require_all_years_above,
    )
    team_qualified_authors = apply_manual_include_overrides(
        team_qualified_authors,
        state_key="manual_team_author_include",
        key_column="author_team_key",
    )
    if current_picks.empty:
        st.error("No current mocks matched the selected published date window.")
        return
    current = build_current_view(current_picks, qualified_authors, team_specialists)
    if current.empty:
        st.error("No current 2026 mocks matched the current best-mocker filter. Relax the sidebar thresholds.")
        st.data_editor(
            qualified_authors,
            use_container_width=True,
            column_config=best_mockers_column_config(),
            disabled=True,
            hide_index=True,
        )
        return

    current["equal_weight"] = 1.0
    pick_weight_column = pick_weight_mode[1]

    min_date = current["published_dt"].min()
    max_date = current["published_dt"].max()
    qualified_current_authors = current["author_name"].nunique()
    qualified_current_mocks = current["mock_relative_url"].nunique()

    metric_col_1, metric_col_2, metric_col_3, metric_col_4 = st.columns(4)
    metric_col_1.metric("Qualified Authors", f"{qualified_current_authors}")
    metric_col_2.metric("Qualified Current Mocks", f"{qualified_current_mocks}")
    metric_col_3.metric("Date Window", f"{min_date:%Y-%m-%d} to {max_date:%Y-%m-%d}")
    metric_col_4.metric(
        "Avg Qualified Edge",
        f"{current[['author_name', 'avg_season_edge']].drop_duplicates()['avg_season_edge'].mean():.1f}",
    )

    pick_candidates = build_pick_candidates(current, pick_weight_column)
    consensus_first_round = build_consensus_first_round(pick_candidates)
    consensus_board_rows = build_consensus_board_rows(pick_candidates)
    team_candidates = build_team_candidates(current)
    team_consensus = build_team_consensus(team_candidates)
    team_historical_mockers = build_team_historical_mocker_view(team_specialists, current_picks)
    player_team_candidates = build_player_team_candidates(current)
    player_pick_candidates = build_player_pick_candidates(current, pick_weight_column)
    position_summary = build_position_summary(current, pick_weight_column)
    position_player_candidates = build_position_player_candidates(current, pick_weight_column)
    position_pick_candidates = build_position_pick_candidates(current, pick_weight_column)
    player_trend_summary = build_player_trend_summary(current, pick_weight_column)
    player_daily_trends = build_player_daily_trends(current, pick_weight_column)
    trend_early_dates, trend_late_dates = get_trend_window_dates(current)
    ingestion_history = load_ingestion_history()
    current_cycle_mock_metadata = load_current_cycle_mock_metadata()
    ingestion_status_rows: list[dict[str, object]] = []

    if "section" in ingestion_history.columns:
        first_round_ingestion = ingestion_history[ingestion_history["section"] == "mock-drafts"].copy()
        team_ingestion = ingestion_history[ingestion_history["section"].isin(["team-mock-drafts", "teams"])].copy()
    else:
        first_round_ingestion = ingestion_history.head(0).copy()
        team_ingestion = ingestion_history.head(0).copy()

    if "section" in current_cycle_mock_metadata.columns:
        first_round_metadata = current_cycle_mock_metadata[current_cycle_mock_metadata["section"] == "mock-drafts"].copy()
    else:
        first_round_metadata = current_cycle_mock_metadata.head(0).copy()

    for mock_type, metadata_frame, history_frame in (
        ("First-Round Mocks", first_round_metadata, first_round_ingestion),
        ("Team Mocks", all_current_team_metadata, team_ingestion),
    ):
        latest_mock = metadata_frame.head(0) if metadata_frame.empty else metadata_frame.sort_values(
            by=["published_dt", "author_name", "mock_name"],
            ascending=[False, True, True],
            na_position="last",
        ).head(1)
        latest_ingest = history_frame.head(0) if history_frame.empty else history_frame.sort_values(
            by=["ingested_dt"],
            ascending=[False],
            na_position="last",
        ).head(1)
        ingestion_status_rows.append(
            {
                "mock_type": mock_type,
                "current_local_mocks": int(len(metadata_frame)),
                "latest_mock_published_at": (
                    str(latest_mock["published_at"].iloc[0])
                    if not latest_mock.empty and "published_at" in latest_mock.columns
                    else ""
                ),
                "latest_mock_author": (
                    str(latest_mock["author_name"].iloc[0])
                    if not latest_mock.empty and "author_name" in latest_mock.columns
                    else ""
                ),
                "latest_mock_source": (
                    str(latest_mock["mock_name"].iloc[0])
                    if not latest_mock.empty and "mock_name" in latest_mock.columns
                    else ""
                ),
                "last_ingested_at": (
                    str(latest_ingest["ingested_at"].iloc[0])
                    if not latest_ingest.empty and "ingested_at" in latest_ingest.columns
                    else ""
                ),
                "last_ingest_method": (
                    str(latest_ingest["method"].iloc[0])
                    if not latest_ingest.empty and "method" in latest_ingest.columns
                    else ""
                ),
                "last_ingest_status": (
                    str(latest_ingest["status"].iloc[0])
                    if not latest_ingest.empty and "status" in latest_ingest.columns
                    else ""
                ),
            }
        )
    ingestion_status_summary = pd.DataFrame(ingestion_status_rows)
    team_full_mock_summary = build_team_full_mock_summary(
        current_team_metadata,
        current_team_picks,
        team_qualified_authors,
    )
    team_full_mock_picks = build_team_full_mock_pick_view(
        current_team_metadata,
        current_team_picks,
        team_qualified_authors,
    )

    tab_1, tab_2, tab_3, tab_4, tab_5, tab_6, tab_7, tab_8, tab_9, tab_10 = st.tabs(
        ["Consensus Mock", "By Team", "By Pick", "By Player", "By Position", "Player Trends", "Best Mockers", "Ingestion", "Team Full Mocks", "Best Full Team Mockers"]
    )

    with tab_1:
        st.subheader("Full First-Round Consensus")
        st.caption(
            "A draft-board style view of the current first-round consensus from qualified recent mockers, "
            "including the top alternate options at each pick."
        )
        render_consensus_board(consensus_board_rows)
        with st.expander("Show Raw Consensus Table"):
            st.dataframe(
                consensus_first_round[
                    [
                        "pick",
                        "team_name",
                        "player_name",
                        "player_position",
                        "weighted_score",
                        "pick_share",
                        "raw_count",
                        "runner_up",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
                column_config=consensus_mock_column_config(),
            )

    with tab_2:
        st.subheader("Favorite Picks By Team")
        team_name_options = sorted(team_candidates["team_name"].dropna().unique())
        selected_team = st.selectbox("Team", options=team_name_options)
        team_rows = team_candidates[team_candidates["team_name"] == selected_team].head(12)
        st.dataframe(
            team_rows[
                [
                    "player_name",
                    "player_position",
                    "weighted_score",
                    "team_share",
                    "raw_count",
                    "avg_pick",
                    "median_pick",
                ]
            ],
            use_container_width=True,
            hide_index=True,
            column_config=favorite_picks_by_team_column_config(),
        )
        st.subheader("Historically Best Mockers For This Team")
        historical_team_rows = team_historical_mockers[
            team_historical_mockers["team_name"] == selected_team
        ].head(12)
        st.dataframe(
            historical_team_rows[
                [
                    "author_name",
                    "team_specific_score",
                    "attempts",
                    "years_covered",
                    "team_match_rate",
                    "has_current_2026_projection",
                    "current_2026_player",
                    "current_2026_position",
                    "current_2026_pick",
                    "current_2026_published_at",
                    "current_2026_mock_name",
                ]
            ],
            use_container_width=True,
            hide_index=True,
            column_config=best_team_mockers_column_config(),
        )
        st.subheader("Top Team Favorite For Every Team")
        st.dataframe(
            team_consensus[
                [
                    "team_name",
                    "player_name",
                    "player_position",
                    "weighted_score",
                    "team_share",
                    "raw_count",
                    "avg_pick",
                ]
            ],
            use_container_width=True,
            hide_index=True,
            column_config=team_consensus_column_config(),
        )

    with tab_3:
        st.subheader("Favorite Picks By Slot")
        pick_options = sorted(pick_candidates["pick"].dropna().astype(int).unique())
        selected_pick = st.selectbox("Pick", options=pick_options, index=0)
        pick_rows = pick_candidates[pick_candidates["pick"] == selected_pick].head(12)
        st.dataframe(
            pick_rows[
                [
                    "slot_team_name",
                    "player_name",
                    "player_position",
                    "weighted_score",
                    "pick_share",
                    "raw_count",
                ]
            ],
            use_container_width=True,
            hide_index=True,
            column_config=by_pick_column_config(),
        )

        st.subheader("Mocks Behind This Pick")
        pick_detail_rows = (
            current[current["pick"] == selected_pick]
            .assign(active_weight=lambda frame: frame[pick_weight_column])
            .sort_values(
                by=["player_name", "published_dt", "author_name", "mock_name"],
                ascending=[True, False, True, True],
            )
        )
        st.dataframe(
            pick_detail_rows[
                [
                    "published_at",
                    "author_name",
                    "mock_name",
                    "player_name",
                    "player_position",
                    "team_name",
                    "pick",
                    "active_weight",
                ]
            ],
            use_container_width=True,
            hide_index=True,
            column_config=pick_detail_column_config(),
        )

    with tab_4:
        st.subheader("Player View")
        player_options = sorted(player_team_candidates["player_name"].dropna().unique())
        player_search = st.text_input("Player search", value="", placeholder="Type part of a player name")
        filtered_player_options = [
            player_name
            for player_name in player_options
            if player_search.strip().lower() in player_name.lower()
        ]
        if not filtered_player_options:
            st.warning("No players match that search.")
        else:
            default_player_name = (
                "Francis Mauigoa"
                if "Francis Mauigoa" in filtered_player_options
                else filtered_player_options[0]
            )
            default_player_index = filtered_player_options.index(default_player_name)
            selected_player = st.selectbox("Player", options=filtered_player_options, index=default_player_index)

            player_team_rows = player_team_candidates[
                player_team_candidates["player_name"] == selected_player
            ].head(12)
            player_pick_rows = player_pick_candidates[
                player_pick_candidates["player_name"] == selected_player
            ].head(12)
            player_detail_rows = (
                current[current["player_name"] == selected_player]
                .sort_values(by=["pick", "published_dt", "author_name"], ascending=[True, False, True])
                .copy()
            )

            header_cols = st.columns(4)
            if not player_team_rows.empty:
                header_cols[0].metric("Position", str(player_team_rows["player_position"].iloc[0]))
                header_cols[1].metric("Most Common Team", str(player_team_rows["team_name"].iloc[0]))
                header_cols[2].metric("Median Mock Slot", f"{player_team_rows['median_pick'].median():.1f}")
                header_cols[3].metric("Recent Qualified Mocks", f"{len(player_detail_rows)}")

            subcol_1, subcol_2 = st.columns(2)
            with subcol_1:
                st.caption("Where this player is being mocked by team")
                st.dataframe(
                    player_team_rows[
                        [
                            "team_name",
                            "weighted_score",
                            "player_team_share",
                            "raw_count",
                            "avg_pick",
                            "median_pick",
                        ]
                    ],
                    use_container_width=True,
                    hide_index=True,
                    column_config=player_team_column_config(),
                )
            with subcol_2:
                st.caption("Where this player is being mocked by pick slot")
                st.dataframe(
                    player_pick_rows[
                        [
                            "pick",
                            "weighted_score",
                            "player_pick_share",
                            "raw_count",
                        ]
                    ],
                    use_container_width=True,
                    hide_index=True,
                    column_config=player_pick_column_config(),
                )

            st.caption("Qualified recent mocks behind this player view")
            st.dataframe(
                player_detail_rows[
                    [
                        "published_at",
                        "author_name",
                        "mock_name",
                        "team_name",
                        "pick",
                        "player_position",
                        "overall_weight",
                        "team_weight",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
                column_config=player_detail_column_config(),
            )

    with tab_5:
        st.subheader("Position View")
        if position_summary.empty:
            st.info("No position-level first-round view is available for the current filter.")
        else:
            position_options = position_summary["player_position"].dropna().astype(str).tolist()
            selected_position = st.selectbox("Position", options=position_options)

            selected_position_summary = position_summary[
                position_summary["player_position"] == selected_position
            ].head(1)
            position_player_rows = position_player_candidates[
                position_player_candidates["player_position"] == selected_position
            ].head(15)
            position_pick_rows = position_pick_candidates[
                position_pick_candidates["player_position"] == selected_position
            ].head(15)
            position_detail_rows = (
                current[current["player_position"] == selected_position]
                .sort_values(by=["pick", "published_dt", "author_name", "player_name"], ascending=[True, False, True, True])
                .copy()
            )

            if not selected_position_summary.empty:
                metric_cols = st.columns(5)
                metric_cols[0].metric("Weighted Share", f"{selected_position_summary['round_one_share'].iloc[0] * 100:.1f}%")
                metric_cols[1].metric("Raw Round-One Rate", f"{selected_position_summary['raw_round_one_rate'].iloc[0] * 100:.1f}%")
                metric_cols[2].metric("Avg Pick", f"{selected_position_summary['avg_pick'].iloc[0]:.1f}")
                metric_cols[3].metric("Median Pick", f"{selected_position_summary['median_pick'].iloc[0]:.1f}")
                metric_cols[4].metric(
                    "Pick Range",
                    f"{int(selected_position_summary['earliest_pick'].iloc[0])}-{int(selected_position_summary['latest_pick'].iloc[0])}",
                )

            st.caption("How often each first-round position shows up in the current qualified sample")
            st.dataframe(
                position_summary[
                    [
                        "player_position",
                        "weighted_score",
                        "round_one_share",
                        "raw_count",
                        "raw_round_one_rate",
                        "unique_authors",
                        "unique_players",
                        "avg_pick",
                        "median_pick",
                        "earliest_pick",
                        "latest_pick",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
                column_config=position_summary_column_config(),
            )

            pos_col_1, pos_col_2 = st.columns(2)
            with pos_col_1:
                st.caption("Players from this position mocked in the first round")
                st.dataframe(
                    position_player_rows[
                        [
                            "player_name",
                            "college_name",
                            "top_team",
                            "weighted_score",
                            "position_player_share",
                            "raw_count",
                            "avg_pick",
                            "median_pick",
                            "earliest_pick",
                            "latest_pick",
                        ]
                    ],
                    use_container_width=True,
                    hide_index=True,
                    column_config=position_player_column_config(),
                )
            with pos_col_2:
                st.caption("Where this position is being mocked by pick slot")
                st.dataframe(
                    position_pick_rows[
                        [
                            "pick",
                            "slot_team_name",
                            "top_player",
                            "weighted_score",
                            "position_pick_share",
                            "raw_count",
                            "unique_players",
                        ]
                    ],
                    use_container_width=True,
                    hide_index=True,
                    column_config=position_pick_column_config(),
                )

            st.caption("Qualified recent mocks behind this position view")
            st.dataframe(
                position_detail_rows[
                    [
                        "published_at",
                        "author_name",
                        "mock_name",
                        "player_name",
                        "team_name",
                        "pick",
                        "overall_weight",
                        "team_weight",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
                column_config=position_detail_column_config(),
            )

    with tab_6:
        st.subheader("Player Trends")
        if len(set(current["published_dt"].dropna().dt.date.tolist())) < 2 or player_trend_summary.empty:
            st.info("Player trends need at least two publish dates in the selected window.")
        else:
            early_label = (
                f"{trend_early_dates[0]:%Y-%m-%d} to {trend_early_dates[-1]:%Y-%m-%d}"
                if trend_early_dates
                else "N/A"
            )
            late_label = (
                f"{trend_late_dates[0]:%Y-%m-%d} to {trend_late_dates[-1]:%Y-%m-%d}"
                if trend_late_dates
                else "N/A"
            )
            st.caption(
                f"Rising/falling compares the early window ({early_label}) to the late window ({late_label}). "
                "Positive pick change means the player is being mocked earlier. Positive round-one rate change means the player is showing up in round one more often."
            )

            trend_candidates = player_trend_summary[
                player_trend_summary["dates_covered"] >= 2
            ].copy()
            rising_rows = trend_candidates[
                trend_candidates["trend_pick_change"] >= 0.5
            ].head(15)
            falling_rows = trend_candidates[
                trend_candidates["trend_pick_change"] <= -0.5
            ].sort_values(
                by=["trend_pick_change", "late_window_avg_pick", "raw_count", "player_name"],
                ascending=[True, True, False, True],
                na_position="last",
            ).head(15)

            trend_col_1, trend_col_2 = st.columns(2)
            with trend_col_1:
                st.caption("Players rising in first-round mocks")
                st.dataframe(
                    rising_rows[
                        [
                            "player_name",
                            "player_position",
                            "college_name",
                            "trend_direction",
                            "trend_pick_change",
                            "early_window_avg_pick",
                            "late_window_avg_pick",
                            "appearance_rate_change",
                            "early_appearance_rate",
                            "late_appearance_rate",
                            "dates_covered",
                            "raw_count",
                        ]
                    ],
                    use_container_width=True,
                    hide_index=True,
                    column_config=player_trend_summary_column_config(),
                )
            with trend_col_2:
                st.caption("Players falling in first-round mocks")
                st.dataframe(
                    falling_rows[
                        [
                            "player_name",
                            "player_position",
                            "college_name",
                            "trend_direction",
                            "trend_pick_change",
                            "early_window_avg_pick",
                            "late_window_avg_pick",
                            "appearance_rate_change",
                            "early_appearance_rate",
                            "late_appearance_rate",
                            "dates_covered",
                            "raw_count",
                        ]
                    ],
                    use_container_width=True,
                    hide_index=True,
                    column_config=player_trend_summary_column_config(),
                )

            trend_player_options = trend_candidates["player_name"].dropna().astype(str).tolist()
            if trend_player_options:
                default_trend_player = (
                    rising_rows["player_name"].iloc[0]
                    if not rising_rows.empty
                    else trend_player_options[0]
                )
                default_compare_players = [default_trend_player]
                selected_trend_players = st.multiselect(
                    "Players To Inspect",
                    options=trend_player_options,
                    default=default_compare_players,
                    help="Select one or more players to compare their trend lines together.",
                )
                if selected_trend_players:
                    focus_trend_player = selected_trend_players[0]
                    selected_trend_summary = trend_candidates[
                        trend_candidates["player_name"] == focus_trend_player
                    ].head(1)
                    selected_daily_trends = player_daily_trends[
                        player_daily_trends["player_name"].isin(selected_trend_players)
                    ].copy()
                    if not selected_trend_summary.empty:
                        st.caption(f"Summary for {focus_trend_player}")
                        metric_cols = st.columns(6)
                        metric_cols[0].metric("Trend", str(selected_trend_summary["trend_direction"].iloc[0]))
                        metric_cols[1].metric("Pick Change", f"{selected_trend_summary['trend_pick_change'].iloc[0]:+.2f}")
                        metric_cols[2].metric("R1 Rate Change", f"{selected_trend_summary['appearance_rate_change'].iloc[0]:+.3f}")
                        metric_cols[3].metric("Early Avg Pick", f"{selected_trend_summary['early_window_avg_pick'].iloc[0]:.1f}")
                        metric_cols[4].metric("Late Avg Pick", f"{selected_trend_summary['late_window_avg_pick'].iloc[0]:.1f}")
                        metric_cols[5].metric("Dates Covered", f"{int(selected_trend_summary['dates_covered'].iloc[0])}")

                    if not selected_daily_trends.empty:
                        st.caption("Weighted average pick over time for selected players. Lower means earlier in the first round.")
                        chart_frame = (
                            selected_daily_trends[["published_dt", "player_name", "weighted_avg_pick"]]
                            .copy()
                            .pivot(index="published_dt", columns="player_name", values="weighted_avg_pick")
                            .sort_index()
                        )
                        st.line_chart(chart_frame)
                        detail_rows = selected_daily_trends.sort_values(
                            by=["published_dt", "player_name"],
                            ascending=[True, True],
                            na_position="last",
                        ).copy()
                        st.dataframe(
                            detail_rows[
                                [
                                    "player_name",
                                    "published_at",
                                    "weighted_avg_pick",
                                    "raw_count",
                                    "top_team",
                                ]
                            ],
                            use_container_width=True,
                            hide_index=True,
                            column_config={
                                "player_name": st.column_config.TextColumn("Player", help="Selected player shown in the comparison table."),
                                **player_daily_trend_column_config(),
                            },
                        )

    with tab_7:
        st.subheader("Historically Best Mockers")
        qualified_view = qualified_authors.copy()
        qualified_view["has_recent_2026_mock"] = qualified_view["author_name"].isin(current_picks["author_name"].unique())
        qualified_view["currently_used_in_2026_consensus"] = qualified_view["author_name"].isin(current["author_name"].unique())
        filter_col_1, filter_col_2, filter_col_3, filter_col_4 = st.columns(4)
        author_options = sorted(qualified_view["author_name"].dropna().unique().tolist())
        selected_authors = filter_col_1.multiselect(
            "Authors",
            options=author_options,
            help="Start typing to search and select an author name.",
        )
        min_years_filter, max_years_filter = filter_col_2.slider(
            "Years covered",
            min_value=int(qualified_view["years_covered"].min()),
            max_value=int(qualified_view["years_covered"].max()),
            value=(
                int(qualified_view["years_covered"].min()),
                int(qualified_view["years_covered"].max()),
            ),
        )
        qualification_filter = filter_col_3.selectbox(
            "Qualification",
            options=["All", "Qualified only", "Not qualified"],
        )
        current_window_filter = filter_col_4.selectbox(
            "Current 2026 window",
            options=["All", "Has recent 2026 mock", "No recent 2026 mock"],
        )

        if selected_authors:
            qualified_view = qualified_view[
                qualified_view["author_name"].isin(selected_authors)
            ]
        qualified_view = qualified_view[
            qualified_view["years_covered"].between(min_years_filter, max_years_filter)
        ]
        if qualification_filter == "Qualified only":
            qualified_view = qualified_view[qualified_view["qualified"]]
        elif qualification_filter == "Not qualified":
            qualified_view = qualified_view[~qualified_view["qualified"]]

        if current_window_filter == "Has recent 2026 mock":
            qualified_view = qualified_view[qualified_view["has_recent_2026_mock"]]
        elif current_window_filter == "No recent 2026 mock":
            qualified_view = qualified_view[~qualified_view["has_recent_2026_mock"]]
        editor_columns = [
            "qualified",
            "manual_include",
            "effective_qualified",
            "has_recent_2026_mock",
            "currently_used_in_2026_consensus",
            "author_name",
            "years_covered",
            "seasons_above_avg",
            "above_avg_rate",
            "avg_historical_score",
            "avg_season_edge",
            "min_season_edge",
            "author_weight",
            "author_name_norm",
        ]
        editor_view = qualified_view[editor_columns].copy()
        best_mocker_column_order = [
            "qualified",
            "manual_include",
            "effective_qualified",
            "has_recent_2026_mock",
            "currently_used_in_2026_consensus",
            "author_name",
            "years_covered",
            "seasons_above_avg",
            "above_avg_rate",
            "avg_historical_score",
            "avg_season_edge",
            "min_season_edge",
            "author_weight",
        ]
        if read_only_mode:
            st.dataframe(
                editor_view[best_mocker_column_order],
                use_container_width=True,
                hide_index=True,
                column_config=best_mockers_column_config(),
            )
        else:
            edited_view = st.data_editor(
                editor_view,
                use_container_width=True,
                hide_index=True,
                column_config=best_mockers_column_config(),
                disabled=[
                    "qualified",
                    "effective_qualified",
                    "has_recent_2026_mock",
                    "currently_used_in_2026_consensus",
                    "author_name",
                    "years_covered",
                    "seasons_above_avg",
                    "above_avg_rate",
                    "avg_historical_score",
                    "avg_season_edge",
                    "min_season_edge",
                    "author_weight",
                    "author_name_norm",
                ],
                column_order=best_mocker_column_order,
            )
            current_overrides = dict(st.session_state.get("manual_author_include", {}))
            new_overrides = dict(current_overrides)
            for _, row in edited_view.iterrows():
                author_key = row["author_name_norm"]
                if not author_key:
                    continue
                new_overrides[author_key] = bool(row["manual_include"])
            if new_overrides != current_overrides:
                st.session_state["manual_author_include"] = new_overrides
                st.rerun()

        st.subheader("Latest 2026 Mock By Author")
        latest_author_options = sorted(current_picks["author_name"].dropna().unique().tolist())
        selected_latest_author = st.selectbox(
            "Author Latest 2026 Mock",
            options=latest_author_options,
            help="Pick an author to inspect their most recent 2026 first-round mock currently in the local dataset.",
        )
        latest_author_rows = current_picks[current_picks["author_name"] == selected_latest_author].copy()
        latest_author_rows = latest_author_rows.sort_values(
            by=["published_dt", "mock_relative_url", "pick"],
            ascending=[False, True, True],
            na_position="last",
        )
        if latest_author_rows.empty:
            st.info("No 2026 mock is available for that author in the current local dataset.")
        else:
            latest_mock_url = str(latest_author_rows.iloc[0]["mock_relative_url"])
            latest_mock_rows = (
                latest_author_rows[latest_author_rows["mock_relative_url"] == latest_mock_url]
                .sort_values(by=["pick", "team_name", "player_name"], ascending=[True, True, True])
                .copy()
            )
            latest_meta = latest_mock_rows.iloc[0]
            latest_meta_cols = st.columns(4)
            latest_meta_cols[0].metric("Author", str(latest_meta["author_name"]))
            latest_meta_cols[1].metric("Publish Date", str(latest_meta["published_at"]))
            latest_meta_cols[2].metric("Source", str(latest_meta["mock_name"]))
            latest_meta_cols[3].metric("Picks Shown", f"{len(latest_mock_rows)}")
            st.dataframe(
                latest_mock_rows[
                    [
                        "pick",
                        "team_name",
                        "player_name",
                        "player_position",
                        "college_name",
                        "published_at",
                        "mock_name",
                    ]
                ],
                use_container_width=True,
                hide_index=True,
                column_config=author_latest_mock_column_config(),
            )

    with tab_8:
        st.subheader("Ingestion")
        st.caption(
            "Track local additions, duplicate skips, pasted HTML imports, and the most recent current-cycle mocks stored on disk."
        )
        st.caption("Latest local mock date and last ingest timestamp by mock type")
        st.dataframe(
            ingestion_status_summary,
            use_container_width=True,
            hide_index=True,
            column_config=ingestion_status_column_config(),
        )
        ingest_col_1, ingest_col_2 = st.columns(2)
        with ingest_col_1:
            st.caption("Recent local mocks")
            if current_cycle_mock_metadata.empty:
                st.info("No current local mock metadata is available yet.")
            else:
                recent_rows = current_cycle_mock_metadata.head(25).copy()
                if "source_team_name" not in recent_rows.columns:
                    recent_rows["source_team_name"] = pd.NA
                st.dataframe(
                    recent_rows[
                        [
                            "published_at",
                            "section",
                            "author_name",
                            "mock_name",
                            "source_team_name",
                            "mock_relative_url",
                        ]
                    ],
                    use_container_width=True,
                    hide_index=True,
                    column_config=recent_local_mocks_column_config(),
                )
        with ingest_col_2:
            st.caption("Ingestion history")
            if ingestion_history.empty:
                st.info("No ingestion history has been logged yet.")
            else:
                st.dataframe(
                    ingestion_history.head(30)[
                        [
                            "ingested_at",
                            "method",
                            "status",
                            "section",
                            "mock_relative_url",
                            "detail",
                        ]
                    ],
                    use_container_width=True,
                    hide_index=True,
                    column_config=ingestion_history_column_config(),
                )

    with tab_9:
        st.subheader("Team Full Mocks")
        if team_full_mock_summary.empty or team_full_mock_picks.empty:
            st.info("No team-specific mocks are available for the current date window.")
        else:
            available_team_names = sorted(team_full_mock_summary["source_team_name"].dropna().unique())
            selected_full_mock_team = st.selectbox(
                "Team For Full Mocks",
                options=available_team_names,
            )
            only_pool_authors = st.checkbox(
                "Only show authors currently in the qualified pool",
                value=False,
                help="Turn this on to restrict the team full-mock list to authors who are currently auto-qualified or manually included in the team full-mock historical pool.",
            )
            team_mock_rows = team_full_mock_summary[
                team_full_mock_summary["source_team_name"] == selected_full_mock_team
            ].copy()
            if only_pool_authors:
                team_mock_rows = team_mock_rows[team_mock_rows["effective_qualified"]].copy()

            if team_mock_rows.empty:
                st.info("No full team mocks matched the current team and author filters.")
            else:
                filtered_team_picks = team_full_mock_picks[
                    team_full_mock_picks["source_team_name"] == selected_full_mock_team
                ].copy()
                if only_pool_authors:
                    filtered_team_picks = filtered_team_picks[filtered_team_picks["effective_qualified"]].copy()

                if filtered_team_picks.empty:
                    st.info("No team-mock pick rows matched the current team and author filters.")
                else:
                    position_overall, position_by_round = build_team_position_summaries(filtered_team_picks)
                    round_player_summary = build_team_round_player_summary(filtered_team_picks)

                    metric_col_1, metric_col_2, metric_col_3 = st.columns(3)
                    metric_col_1.metric("Mocks In View", f"{team_mock_rows['mock_relative_url'].nunique()}")
                    metric_col_2.metric("Authors In View", f"{team_mock_rows['author_name'].nunique()}")
                    metric_col_3.metric("Total Pick Slots", f"{len(filtered_team_picks)}")

                    st.subheader("Position Summary")
                    st.caption(
                        "Draft capital score uses a custom round curve to emphasize early-round investment: "
                        "R1=100, R2=55, R3=30, R4=16, R5=9, R6=5, R7=3."
                    )
                    summary_col_1, summary_col_2 = st.columns(2)
                    with summary_col_1:
                        st.caption("Overall position mix across all filtered full-team picks")
                        st.dataframe(
                            position_overall[
                                [
                                    "player_position",
                                    "pick_count",
                                    "overall_share",
                                    "draft_capital_score",
                                    "capital_share",
                                    "unique_mocks",
                                    "unique_authors",
                                ]
                            ],
                            use_container_width=True,
                            hide_index=True,
                            column_config=team_position_overall_column_config(),
                        )
                    with summary_col_2:
                        st.caption("Position mix by round")
                        st.dataframe(
                            position_by_round[
                                [
                                    "round_number",
                                    "player_position",
                                    "pick_count",
                                    "round_share",
                                    "draft_capital_score",
                                    "capital_share",
                                    "unique_mocks",
                                    "unique_authors",
                                ]
                            ],
                            use_container_width=True,
                            hide_index=True,
                            column_config=team_position_by_round_column_config(),
                        )

                    st.subheader("Player Percentages By Round")
                    available_rounds = sorted(
                        round_player_summary["round_number"].dropna().astype(int).unique().tolist()
                    )
                    if available_rounds:
                        round_tabs = st.tabs([f"Round {round_number}" for round_number in available_rounds])
                        for round_tab, round_number in zip(round_tabs, available_rounds, strict=False):
                            with round_tab:
                                round_rows = round_player_summary[
                                    round_player_summary["round_number"] == round_number
                                ].copy()
                                st.dataframe(
                                    round_rows[
                                        [
                                            "player_name",
                                            "player_position",
                                            "college_name",
                                            "pick_count",
                                            "round_share",
                                            "unique_mocks",
                                            "unique_authors",
                                        ]
                                    ],
                                    use_container_width=True,
                                    hide_index=True,
                                    column_config=team_round_player_column_config(),
                                )

                    with st.expander("Show Underlying Team Mocks"):
                        st.dataframe(
                            team_mock_rows[
                                [
                                    "published_at",
                                    "author_name",
                                    "avg_historical_score",
                                    "mock_name",
                                    "effective_qualified",
                                    "qualified",
                                    "selection_count",
                                    "round_1",
                                    "round_2",
                                    "round_3",
                                    "round_4",
                                    "round_5",
                                    "round_6",
                                    "round_7",
                                ]
                            ],
                            use_container_width=True,
                            hide_index=True,
                            column_config=team_full_mock_summary_column_config(),
                        )

                        st.subheader("Mock Pick Detail")
                        detail_options = team_mock_rows["mock_relative_url"].tolist()
                        selected_mock_url = st.selectbox(
                            "Mock To Inspect",
                            options=detail_options,
                            format_func=lambda value: next(
                                (
                                    f"{row['published_at']} | {row['author_name']} | {row['mock_name']}"
                                    for _, row in team_mock_rows.iterrows()
                                    if row["mock_relative_url"] == value
                                ),
                                str(value),
                            ),
                        )
                        detail_rows = filtered_team_picks[
                            filtered_team_picks["mock_relative_url"] == selected_mock_url
                        ].copy()
                        if not detail_rows.empty:
                            detail_rows = detail_rows.drop_duplicates().copy()
                        detail_rows = detail_rows.sort_values(
                            by=["round_number", "pick_label", "player_name"],
                            ascending=[True, True, True],
                            na_position="last",
                        )
                        st.dataframe(
                            detail_rows[
                                [
                                    "round_number",
                                    "pick_label",
                                    "player_name",
                                    "player_position",
                                    "college_name",
                                    "traded",
                                ]
                            ],
                            use_container_width=True,
                            hide_index=True,
                            column_config=team_mock_detail_column_config(),
                        )

    with tab_10:
        st.subheader("Historically Best Full Team Mockers")
        team_qualified_view = team_qualified_authors.copy()
        recent_pairs = (
            current_team_metadata[["author_name", "source_team_slug"]]
            .dropna()
            .copy()
            if not current_team_metadata.empty
            else pd.DataFrame(columns=["author_name", "source_team_slug"])
        )
        recent_pair_keys = set()
        if not recent_pairs.empty:
            recent_pairs["author_name_norm"] = recent_pairs["author_name"].map(normalize_author)
            recent_pairs["author_team_key"] = (
                recent_pairs["author_name_norm"].fillna("")
                + "::"
                + recent_pairs["source_team_slug"].fillna("").astype(str).str.strip().str.lower()
            )
            recent_pair_keys = set(recent_pairs["author_team_key"].dropna().astype(str))
        used_pair_keys = set(
            team_full_mock_summary[team_full_mock_summary["effective_qualified"]]
            .assign(
                author_team_key=lambda df: (
                    df["author_name"].map(normalize_author).fillna("")
                    + "::"
                    + df["source_team_slug"].fillna("").astype(str).str.strip().str.lower()
                )
            )["author_team_key"]
            .dropna()
            .astype(str)
            if not team_full_mock_summary.empty
            else []
        )
        team_qualified_view["has_recent_2026_team_mock"] = team_qualified_view["author_team_key"].isin(recent_pair_keys)
        team_qualified_view["currently_used_in_team_full_mock_view"] = team_qualified_view["author_team_key"].isin(
            used_pair_keys
        )
        filter_col_1, filter_col_2, filter_col_3, filter_col_4 = st.columns(4)
        author_options = sorted(team_qualified_view["author_team_label"].dropna().unique().tolist())
        selected_authors = filter_col_1.multiselect(
            "Author-Team",
            options=author_options,
            help="Start typing to search and select a historical author-team combination.",
            key="team_best_mocker_authors",
        )
        min_years_filter, max_years_filter = filter_col_2.slider(
            "Years covered",
            min_value=int(team_qualified_view["years_covered"].min()),
            max_value=int(team_qualified_view["years_covered"].max()),
            value=(
                int(team_qualified_view["years_covered"].min()),
                int(team_qualified_view["years_covered"].max()),
            ),
            key="team_best_mocker_years",
        )
        qualification_filter = filter_col_3.selectbox(
            "Qualification",
            options=["All", "Qualified only", "Not qualified"],
            key="team_best_mocker_qualification",
        )
        current_window_filter = filter_col_4.selectbox(
            "Current 2026 team mocks",
            options=["All", "Has recent 2026 team mock", "No recent 2026 team mock"],
            key="team_best_mocker_recent",
        )

        if selected_authors:
            team_qualified_view = team_qualified_view[
                team_qualified_view["author_team_label"].isin(selected_authors)
            ]
        team_qualified_view = team_qualified_view[
            team_qualified_view["years_covered"].between(min_years_filter, max_years_filter)
        ]
        if qualification_filter == "Qualified only":
            team_qualified_view = team_qualified_view[team_qualified_view["qualified"]]
        elif qualification_filter == "Not qualified":
            team_qualified_view = team_qualified_view[~team_qualified_view["qualified"]]

        if current_window_filter == "Has recent 2026 team mock":
            team_qualified_view = team_qualified_view[team_qualified_view["has_recent_2026_team_mock"]]
        elif current_window_filter == "No recent 2026 team mock":
            team_qualified_view = team_qualified_view[~team_qualified_view["has_recent_2026_team_mock"]]

        editor_columns = [
            "qualified",
            "manual_include",
            "effective_qualified",
            "has_recent_2026_team_mock",
            "currently_used_in_team_full_mock_view",
            "author_name",
            "source_team_name",
            "years_covered",
            "seasons_above_avg",
            "player_team_round_matches",
            "position_plus_minus_one_round_matches",
            "above_avg_rate",
            "avg_historical_score",
            "avg_season_edge",
            "author_weight",
            "author_name_norm",
            "author_team_key",
        ]
        editor_view = team_qualified_view[editor_columns].copy()
        best_team_mocker_column_order = [
            "qualified",
            "manual_include",
            "effective_qualified",
            "has_recent_2026_team_mock",
            "currently_used_in_team_full_mock_view",
            "author_name",
            "source_team_name",
            "years_covered",
            "seasons_above_avg",
            "player_team_round_matches",
            "position_plus_minus_one_round_matches",
            "above_avg_rate",
            "avg_historical_score",
            "avg_season_edge",
            "author_weight",
        ]
        if read_only_mode:
            st.dataframe(
                editor_view[best_team_mocker_column_order],
                use_container_width=True,
                hide_index=True,
                column_config=best_team_full_mockers_column_config(),
            )
        else:
            edited_view = st.data_editor(
                editor_view,
                use_container_width=True,
                hide_index=True,
                column_config=best_team_full_mockers_column_config(),
                disabled=[
                    "qualified",
                    "effective_qualified",
                    "has_recent_2026_team_mock",
                    "currently_used_in_team_full_mock_view",
                    "author_name",
                    "source_team_name",
                    "years_covered",
                    "seasons_above_avg",
                    "player_team_round_matches",
                    "position_plus_minus_one_round_matches",
                    "above_avg_rate",
                    "avg_historical_score",
                    "avg_season_edge",
                    "author_weight",
                    "author_name_norm",
                    "author_team_key",
                ],
                column_order=best_team_mocker_column_order,
            )
            current_overrides = dict(st.session_state.get("manual_team_author_include", {}))
            new_overrides = dict(current_overrides)
            for _, row in edited_view.iterrows():
                author_key = row["author_team_key"]
                if not author_key:
                    continue
                new_overrides[author_key] = bool(row["manual_include"])
            if new_overrides != current_overrides:
                st.session_state["manual_team_author_include"] = new_overrides
                st.rerun()


if __name__ == "__main__":
    render_app()
