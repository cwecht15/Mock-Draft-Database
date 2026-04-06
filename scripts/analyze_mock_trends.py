#!/usr/bin/env python
from __future__ import annotations

import argparse
import math
from pathlib import Path

import pandas as pd


SUPPORTED_SECTIONS = ("mock-drafts", "team-mock-drafts", "teams")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Analyze scraped NFL mock-draft picks for analyst accuracy and current-cycle trends."
    )
    parser.add_argument("--year", type=int, required=True, help="Draft year, for example 2025 or 2026.")
    parser.add_argument(
        "--section",
        action="append",
        dest="sections",
        choices=SUPPORTED_SECTIONS,
        help="Processed section to analyze. Repeat the flag to analyze multiple sections.",
    )
    parser.add_argument(
        "--processed-dir",
        default="data/processed",
        help="Directory that contains the processed CSVs written by scrape_nflmockdraftdatabase.py.",
    )
    parser.add_argument(
        "--final-author-mock-only",
        action="store_true",
        help="Keep only the latest mock per author within the chosen year. Generic bylines like Staff are deduped by author plus outlet.",
    )
    return parser.parse_args()


def ensure_dir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def normalize_name(value: str | None) -> str | None:
    if value is None or (isinstance(value, float) and math.isnan(value)):
        return None
    text = str(value).strip().lower()
    if not text:
        return None
    return "".join(ch for ch in text if ch.isalnum())


def load_section_data(processed_dir: Path, year: int, section: str) -> tuple[pd.DataFrame, pd.DataFrame]:
    year_dir = processed_dir / str(year)
    metadata = pd.read_csv(year_dir / f"{section}__mock_metadata.csv")
    picks = pd.read_csv(year_dir / f"{section}__mock_picks.csv")
    return metadata, picks


def load_actual_results(processed_dir: Path, year: int) -> pd.DataFrame | None:
    path = processed_dir / str(year) / f"actual_draft_results_{year}.csv"
    if not path.exists():
        return None
    return pd.read_csv(path)


def build_author_dedupe_key(author_name: str | None, mock_name: str | None) -> str:
    author = (author_name or "").strip()
    outlet = (mock_name or "").strip()
    generic_authors = {"", "staff", "media", "editors", "editorial staff"}
    if author.lower() in generic_authors:
        return f"{author.lower()}::{outlet.lower()}"
    return author.lower()


def dedupe_to_latest_author_mock(
    metadata: pd.DataFrame,
    picks: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    metadata = metadata.copy()
    picks = picks.copy()

    metadata["published_dt"] = pd.to_datetime(metadata["published_at"], format="%m/%d/%y", errors="coerce")
    if "list_rank" not in metadata.columns:
        metadata["list_rank"] = range(1, len(metadata) + 1)
    metadata["author_dedupe_key"] = [
        build_author_dedupe_key(author, mock)
        for author, mock in zip(metadata["author_name"], metadata["mock_name"], strict=False)
    ]

    metadata = metadata.sort_values(
        by=["author_dedupe_key", "published_dt", "list_rank", "mock_relative_url"],
        ascending=[True, False, True, True],
        na_position="last",
    )
    kept_metadata = metadata.drop_duplicates(subset=["author_dedupe_key"], keep="first").copy()
    kept_urls = set(kept_metadata["mock_relative_url"].dropna().astype(str))
    kept_picks = picks[picks["mock_relative_url"].astype(str).isin(kept_urls)].copy()
    return kept_metadata, kept_picks


def build_accuracy_outputs(
    picks: pd.DataFrame,
    metadata: pd.DataFrame,
    actual: pd.DataFrame,
    *,
    section: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    actual = actual.copy()
    picks = picks.copy()

    actual["player_norm"] = actual["player_name"].map(normalize_name)
    picks["player_norm"] = picks["player_name"].map(normalize_name)

    actual_by_pick = actual[
        [
            "pick",
            "player_norm",
            "player_name",
            "player_position",
            "team_slug",
            "team_name",
            "round_number",
        ]
    ].rename(
        columns={
            "player_norm": "actual_player_norm",
            "player_name": "actual_player_name",
            "player_position": "actual_player_position",
            "team_slug": "actual_team_slug",
            "team_name": "actual_team_name",
            "round_number": "actual_round_number",
        }
    )
    actual_by_player = actual[
        ["player_norm", "pick", "player_name", "team_slug", "team_name", "round_number"]
    ].drop_duplicates(subset=["player_norm"])
    actual_by_player = actual_by_player.rename(
        columns={
            "pick": "actual_pick",
            "player_name": "actual_player_name",
            "team_slug": "actual_team_slug",
            "team_name": "actual_team_name",
            "round_number": "actual_round_number",
        }
    )

    by_pick_df = picks.merge(actual_by_pick, on="pick", how="left")
    by_pick_df["player_to_pick_match"] = (
        by_pick_df["player_norm"].notna()
        & by_pick_df["actual_player_norm"].notna()
        & (by_pick_df["player_norm"] == by_pick_df["actual_player_norm"])
    )
    by_pick_df["slot_team_match"] = (
        by_pick_df["team_slug"].notna()
        & by_pick_df["actual_team_slug"].notna()
        & (by_pick_df["team_slug"] == by_pick_df["actual_team_slug"])
    )
    by_pick_df["team_to_position_match"] = (
        by_pick_df["slot_team_match"]
        & by_pick_df["player_position"].notna()
        & by_pick_df["actual_player_position"].notna()
        & (by_pick_df["player_position"] == by_pick_df["actual_player_position"])
    )

    by_player_df = picks.merge(actual_by_player, on="player_norm", how="left")
    by_player_df["player_was_drafted"] = by_player_df["actual_pick"].notna()
    by_player_df["player_in_round_one"] = by_player_df["actual_round_number"] == 1
    by_player_df["player_to_team_match"] = (
        by_player_df["team_slug"].notna()
        & by_player_df["actual_team_slug"].notna()
        & (by_player_df["team_slug"] == by_player_df["actual_team_slug"])
    )
    by_player_df["round_match"] = (
        by_player_df["round_number"].notna()
        & by_player_df["actual_round_number"].notna()
        & (by_player_df["round_number"] == by_player_df["actual_round_number"])
    )
    by_player_df["pick_abs_error"] = (by_player_df["pick"] - by_player_df["actual_pick"]).abs()

    actual_team_round_positions: dict[tuple[str, int], set[str]] = {}
    for row in actual[["team_slug", "round_number", "player_position"]].dropna().itertuples(index=False):
        team_slug = str(row.team_slug)
        round_number = int(row.round_number)
        player_position = str(row.player_position)
        actual_team_round_positions.setdefault((team_slug, round_number), set()).add(player_position)

    by_player_df["correct_player_in_round"] = (
        by_player_df["player_to_team_match"] & by_player_df["round_match"]
    )

    def has_same_position_in_round_offset(row: pd.Series, offset: int) -> bool:
        team_slug = row.get("team_slug")
        round_number = row.get("round_number")
        player_position = row.get("player_position")
        if pd.isna(team_slug) or pd.isna(round_number) or pd.isna(player_position):
            return False
        try:
            lookup_key = (str(team_slug), int(round_number) + offset)
        except (TypeError, ValueError):
            return False
        return str(player_position) in actual_team_round_positions.get(lookup_key, set())

    by_player_df["same_position_same_round"] = by_player_df.apply(
        lambda row: has_same_position_in_round_offset(row, 0),
        axis=1,
    )
    by_player_df["same_position_plus_minus_one_round"] = by_player_df.apply(
        lambda row: has_same_position_in_round_offset(row, -1) or has_same_position_in_round_offset(row, 1),
        axis=1,
    )
    by_player_df["same_position_same_round_only"] = (
        by_player_df["same_position_same_round"] & ~by_player_df["correct_player_in_round"]
    )
    by_player_df["same_position_plus_minus_one_round_only"] = (
        by_player_df["same_position_plus_minus_one_round"]
        & ~by_player_df["correct_player_in_round"]
        & ~by_player_df["same_position_same_round"]
    )

    accuracy = (
        by_player_df.groupby("mock_relative_url", dropna=False)
        .agg(
            selection_count=("player_name", "count"),
            player_drafted_matches=("player_was_drafted", "sum"),
            round_one_player_matches=("player_in_round_one", "sum"),
            player_to_team_matches=("player_to_team_match", "sum"),
            round_matches=("round_match", "sum"),
            correct_player_in_round_matches=("correct_player_in_round", "sum"),
            same_position_same_round_matches=("same_position_same_round_only", "sum"),
            same_position_plus_minus_one_round_matches=("same_position_plus_minus_one_round_only", "sum"),
            mean_pick_abs_error=("pick_abs_error", "mean"),
            median_pick_abs_error=("pick_abs_error", "median"),
        )
        .reset_index()
    )
    pick_summary = (
        by_pick_df.groupby("mock_relative_url", dropna=False)
        .agg(
            player_to_pick_matches=("player_to_pick_match", "sum"),
            team_to_position_matches=("team_to_position_match", "sum"),
        )
        .reset_index()
    )
    accuracy = accuracy.merge(pick_summary, on="mock_relative_url", how="left")
    accuracy["player_to_pick_match_rate"] = (
        accuracy["player_to_pick_matches"] / accuracy["selection_count"]
    )
    accuracy["player_to_team_match_rate"] = (
        accuracy["player_to_team_matches"] / accuracy["selection_count"]
    )
    accuracy["round_one_player_match_rate"] = (
        accuracy["round_one_player_matches"] / accuracy["selection_count"]
    )
    accuracy["team_to_position_match_rate"] = (
        accuracy["team_to_position_matches"] / accuracy["selection_count"]
    )
    accuracy["round_match_rate"] = accuracy["round_matches"] / accuracy["selection_count"]
    accuracy["player_drafted_match_rate"] = (
        accuracy["player_drafted_matches"] / accuracy["selection_count"]
    )
    accuracy["correct_player_in_round_rate"] = (
        accuracy["correct_player_in_round_matches"] / accuracy["selection_count"]
    )
    accuracy["same_position_same_round_rate"] = (
        accuracy["same_position_same_round_matches"] / accuracy["selection_count"]
    )
    accuracy["same_position_plus_minus_one_round_rate"] = (
        accuracy["same_position_plus_minus_one_round_matches"] / accuracy["selection_count"]
    )
    accuracy["generous_score_points"] = (
        (3 * accuracy["correct_player_in_round_matches"])
        + (2 * accuracy["same_position_same_round_matches"])
        + accuracy["same_position_plus_minus_one_round_matches"]
    )
    accuracy["generous_score_max_points"] = accuracy["selection_count"] * 3
    accuracy["generous_score_rate"] = (
        accuracy["generous_score_points"] / accuracy["generous_score_max_points"]
    )
    accuracy["simple_score_points"] = (
        accuracy["round_one_player_matches"] + (2 * accuracy["player_to_team_matches"])
    )
    accuracy["simple_score_max_points"] = accuracy["selection_count"] * 3
    accuracy["simple_score_rate"] = (
        accuracy["simple_score_points"] / accuracy["simple_score_max_points"]
    )
    if section in {"team-mock-drafts", "teams"}:
        accuracy["simple_score_points"] = accuracy["generous_score_points"]
        accuracy["simple_score_max_points"] = accuracy["generous_score_max_points"]
        accuracy["simple_score_rate"] = accuracy["generous_score_rate"]
    accuracy["custom_accuracy_score"] = 100.0 * accuracy["simple_score_rate"]

    accuracy = accuracy.merge(
        metadata[
            [
                "mock_relative_url",
                "mock_name",
                "author_name",
                "published_at",
                "external_url",
                "selection_count",
            ]
        ].rename(
            columns={
                "selection_count": "metadata_selection_count",
            }
        ),
        on="mock_relative_url",
        how="left",
    )

    author_accuracy = (
        accuracy.groupby(["author_name", "mock_name"], dropna=False)
        .agg(
            mocks_scraped=("mock_relative_url", "count"),
            avg_custom_accuracy_score=("custom_accuracy_score", "mean"),
            avg_simple_score_points=("simple_score_points", "mean"),
            avg_simple_score_rate=("simple_score_rate", "mean"),
            avg_round_one_player_match_rate=("round_one_player_match_rate", "mean"),
            avg_player_to_pick_match_rate=("player_to_pick_match_rate", "mean"),
            avg_player_to_team_match_rate=("player_to_team_match_rate", "mean"),
            avg_team_to_position_match_rate=("team_to_position_match_rate", "mean"),
            avg_round_match_rate=("round_match_rate", "mean"),
            avg_player_drafted_match_rate=("player_drafted_match_rate", "mean"),
            avg_correct_player_in_round_rate=("correct_player_in_round_rate", "mean"),
            avg_same_position_same_round_rate=("same_position_same_round_rate", "mean"),
            avg_same_position_plus_minus_one_round_rate=("same_position_plus_minus_one_round_rate", "mean"),
            avg_generous_score_points=("generous_score_points", "mean"),
            avg_generous_score_rate=("generous_score_rate", "mean"),
            avg_mean_pick_abs_error=("mean_pick_abs_error", "mean"),
        )
        .reset_index()
        .sort_values(
            by=[
                "avg_custom_accuracy_score",
                "avg_player_to_team_match_rate",
                "avg_round_one_player_match_rate",
            ],
            ascending=[False, False, False],
        )
    )

    return accuracy.sort_values(
        by=["custom_accuracy_score", "player_to_pick_match_rate", "player_to_team_match_rate"],
        ascending=[False, False, False],
    ), author_accuracy


def mode_or_none(series: pd.Series) -> str | None:
    modes = series.dropna().mode()
    if modes.empty:
        return None
    return modes.iloc[0]


def build_trend_outputs(picks: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    picks = picks.copy()
    picks["mock_id"] = picks["mock_relative_url"]

    player_trends = (
        picks.groupby(["player_name", "player_position"], dropna=False)
        .agg(
            mock_count=("mock_id", "count"),
            unique_mocks=("mock_id", "nunique"),
            unique_authors=("author_name", "nunique"),
            avg_round=("round_number", "mean"),
            median_round=("round_number", "median"),
            min_round=("round_number", "min"),
            max_round=("round_number", "max"),
            avg_pick=("pick", "mean"),
            median_pick=("pick", "median"),
            min_pick=("pick", "min"),
            max_pick=("pick", "max"),
            team_count=("team_slug", "nunique"),
        )
        .reset_index()
    )
    player_modes = (
        picks.groupby(["player_name", "player_position"], dropna=False)
        .agg(
            most_common_team=("team_name", mode_or_none),
            most_common_team_slug=("team_slug", mode_or_none),
        )
        .reset_index()
    )
    player_trends = player_trends.merge(
        player_modes, on=["player_name", "player_position"], how="left"
    ).sort_values(by=["mock_count", "median_pick"], ascending=[False, True])

    team_player_trends = (
        picks.groupby(["team_slug", "team_name", "player_name", "player_position"], dropna=False)
        .agg(
            mock_count=("mock_id", "count"),
            unique_mocks=("mock_id", "nunique"),
            unique_authors=("author_name", "nunique"),
            avg_round=("round_number", "mean"),
            median_round=("round_number", "median"),
            min_round=("round_number", "min"),
            max_round=("round_number", "max"),
            avg_pick=("pick", "mean"),
            median_pick=("pick", "median"),
            min_pick=("pick", "min"),
            max_pick=("pick", "max"),
        )
        .reset_index()
    )
    team_totals = (
        picks.groupby(["team_slug", "team_name"], dropna=False)
        .agg(team_mock_count=("mock_id", "count"))
        .reset_index()
    )
    team_player_trends = team_player_trends.merge(
        team_totals, on=["team_slug", "team_name"], how="left"
    )
    team_player_trends["share_of_team_picks"] = (
        team_player_trends["mock_count"] / team_player_trends["team_mock_count"]
    )
    team_player_trends = team_player_trends.sort_values(
        by=["team_name", "mock_count", "median_pick"], ascending=[True, False, True]
    )

    team_position_trends = (
        picks.groupby(["team_slug", "team_name", "player_position"], dropna=False)
        .agg(
            mock_count=("mock_id", "count"),
            unique_players=("player_name", "nunique"),
            avg_round=("round_number", "mean"),
            median_round=("round_number", "median"),
            min_round=("round_number", "min"),
            max_round=("round_number", "max"),
            avg_pick=("pick", "mean"),
            median_pick=("pick", "median"),
            min_pick=("pick", "min"),
            max_pick=("pick", "max"),
        )
        .reset_index()
        .sort_values(by=["team_name", "mock_count", "median_pick"], ascending=[True, False, True])
    )

    return player_trends, team_player_trends, team_position_trends


def analyze_section(processed_dir: Path, year: int, section: str) -> None:
    metadata, picks = load_section_data(processed_dir, year, section)
    actual = load_actual_results(processed_dir, year)
    year_dir = processed_dir / str(year)
    ensure_dir(year_dir)

    player_trends, team_player_trends, team_position_trends = build_trend_outputs(picks)
    player_trends.to_csv(year_dir / f"{section}__player_trends.csv", index=False)
    team_player_trends.to_csv(year_dir / f"{section}__team_player_trends.csv", index=False)
    team_position_trends.to_csv(year_dir / f"{section}__team_position_trends.csv", index=False)

    if actual is not None:
        mock_accuracy, author_accuracy = build_accuracy_outputs(
            picks,
            metadata,
            actual,
            section=section,
        )
        mock_accuracy.to_csv(year_dir / f"{section}__mock_accuracy.csv", index=False)
        author_accuracy.to_csv(year_dir / f"{section}__author_accuracy.csv", index=False)
        print(
            f"{section}: wrote {len(mock_accuracy)} mock accuracy rows and {len(author_accuracy)} author summary rows"
        )

    print(
        f"{section}: wrote {len(player_trends)} player trends, "
        f"{len(team_player_trends)} team-player trends, and "
        f"{len(team_position_trends)} team-position trends"
    )


def main() -> None:
    args = parse_args()
    processed_dir = Path(args.processed_dir)
    sections = args.sections or list(SUPPORTED_SECTIONS)

    for section in sections:
        if args.final_author_mock_only:
            metadata, picks = load_section_data(processed_dir, args.year, section)
            deduped_metadata, deduped_picks = dedupe_to_latest_author_mock(metadata, picks)
            year_dir = processed_dir / str(args.year)
            deduped_metadata.to_csv(
                year_dir / f"{section}__mock_metadata.final_author_only.csv",
                index=False,
            )
            deduped_picks.to_csv(
                year_dir / f"{section}__mock_picks.final_author_only.csv",
                index=False,
            )
            temp_metadata = year_dir / f"{section}__mock_metadata.csv"
            temp_picks = year_dir / f"{section}__mock_picks.csv"
            backup_metadata = year_dir / f"{section}__mock_metadata.full_backup.csv"
            backup_picks = year_dir / f"{section}__mock_picks.full_backup.csv"
            metadata.to_csv(backup_metadata, index=False)
            picks.to_csv(backup_picks, index=False)
            deduped_metadata.to_csv(temp_metadata, index=False)
            deduped_picks.to_csv(temp_picks, index=False)
            try:
                analyze_section(processed_dir, args.year, section)
            finally:
                metadata.to_csv(temp_metadata, index=False)
                picks.to_csv(temp_picks, index=False)
        else:
            analyze_section(processed_dir, args.year, section)


if __name__ == "__main__":
    main()
