#!/usr/bin/env python
from __future__ import annotations

import argparse
import math
from pathlib import Path

import pandas as pd


GENERIC_AUTHORS = {"", "staff", "media", "editors", "editorial staff"}


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build historical author-by-team accuracy tables and optional weighted trend outputs "
            "for a future mock-draft year."
        )
    )
    parser.add_argument(
        "--processed-dir",
        default="data/processed",
        help="Directory containing per-year processed CSVs.",
    )
    parser.add_argument(
        "--history-start-year",
        type=int,
        default=2020,
        help="First historical year to include.",
    )
    parser.add_argument(
        "--history-end-year",
        type=int,
        default=2025,
        help="Last historical year to include.",
    )
    parser.add_argument(
        "--target-year",
        type=int,
        default=None,
        help="Optional future year whose trends should be weighted by historical team-specialist scores.",
    )
    parser.add_argument(
        "--min-attempts",
        type=int,
        default=3,
        help="Minimum author-team attempts for a pair to appear in the best-authors-by-team output.",
    )
    parser.add_argument(
        "--min-years-covered",
        type=int,
        default=2,
        help="Minimum historical years covered for a pair to appear in the best-authors-by-team output.",
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


def is_generic_author(value: str | None) -> bool:
    return (value or "").strip().lower() in GENERIC_AUTHORS


def load_historical_pick_results(processed_dir: Path, start_year: int, end_year: int) -> pd.DataFrame:
    frames: list[pd.DataFrame] = []
    for year in range(start_year, end_year + 1):
        picks_path = processed_dir / str(year) / "mock-drafts__mock_picks.csv"
        actual_path = processed_dir / str(year) / f"actual_draft_results_{year}.csv"
        if not picks_path.exists() or not actual_path.exists():
            continue

        picks = pd.read_csv(picks_path)
        actual = pd.read_csv(actual_path)
        picks["player_norm"] = picks["player_name"].map(normalize_name)
        actual["player_norm"] = actual["player_name"].map(normalize_name)

        actual_by_player = actual[
            ["player_norm", "team_slug", "team_name", "round_number", "pick"]
        ].drop_duplicates(subset=["player_norm"])
        actual_by_player = actual_by_player.rename(
            columns={
                "team_slug": "actual_team_slug",
                "team_name": "actual_team_name",
                "round_number": "actual_round_number",
                "pick": "actual_pick",
            }
        )

        merged = picks.merge(actual_by_player, on="player_norm", how="left")
        merged["year"] = year
        merged["player_to_team_match"] = (
            merged["team_slug"].notna()
            & merged["actual_team_slug"].notna()
            & (merged["team_slug"] == merged["actual_team_slug"])
        )
        merged["player_in_round_one"] = merged["actual_round_number"] == 1
        frames.append(merged)

    if not frames:
        raise FileNotFoundError("No historical processed mock picks plus actual results were found.")
    return pd.concat(frames, ignore_index=True)


def build_team_specialist_tables(
    historical_results: pd.DataFrame,
    *,
    min_attempts: int,
    min_years_covered: int,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    first_round = historical_results[
        (historical_results["round_number"] == 1) & historical_results["team_slug"].notna()
    ].copy()

    team_author = (
        first_round.groupby(["team_slug", "team_name", "author_name"], dropna=False)
        .agg(
            attempts=("player_name", "count"),
            years_covered=("year", "nunique"),
            team_hits=("player_to_team_match", "sum"),
            round_one_player_hits=("player_in_round_one", "sum"),
            mean_mock_pick=("pick", "mean"),
            mean_actual_pick=("actual_pick", "mean"),
        )
        .reset_index()
    )
    team_author["team_match_rate"] = team_author["team_hits"] / team_author["attempts"]
    team_author["round_one_player_rate"] = (
        team_author["round_one_player_hits"] / team_author["attempts"]
    )
    team_author["team_specific_points"] = (
        team_author["round_one_player_hits"] + (2 * team_author["team_hits"])
    )
    team_author["team_specific_max_points"] = team_author["attempts"] * 3
    team_author["team_specific_score"] = (
        100.0 * team_author["team_specific_points"] / team_author["team_specific_max_points"]
    )
    team_author["team_specialist_weight"] = team_author["team_specific_score"] / 100.0
    team_author = team_author.sort_values(
        by=["team_specific_score", "team_match_rate", "attempts"],
        ascending=[False, False, False],
    )

    stable_pairs = team_author[
        (team_author["attempts"] >= min_attempts)
        & (team_author["years_covered"] >= min_years_covered)
    ].copy()
    stable_pairs = stable_pairs[
        ~stable_pairs["author_name"].map(is_generic_author)
    ].copy()

    best_by_team = (
        stable_pairs.sort_values(
            by=["team_slug", "team_specific_score", "team_match_rate", "attempts"],
            ascending=[True, False, False, False],
        )
        .drop_duplicates(subset=["team_slug"], keep="first")
        .sort_values(by=["team_name", "team_specific_score"], ascending=[True, False])
    )

    author_overall = (
        historical_results.groupby("author_name", dropna=False)
        .agg(
            total_mocked_picks=("player_name", "count"),
            historical_years_covered=("year", "nunique"),
            overall_team_hits=("player_to_team_match", "sum"),
            overall_round_one_player_hits=("player_in_round_one", "sum"),
        )
        .reset_index()
    )
    author_overall["overall_points"] = (
        author_overall["overall_round_one_player_hits"] + (2 * author_overall["overall_team_hits"])
    )
    author_overall["overall_max_points"] = author_overall["total_mocked_picks"] * 3
    author_overall["overall_score"] = (
        100.0 * author_overall["overall_points"] / author_overall["overall_max_points"]
    )
    author_overall["overall_weight"] = author_overall["overall_score"] / 100.0
    author_overall = author_overall.sort_values(
        by=["overall_score", "overall_team_hits", "total_mocked_picks"],
        ascending=[False, False, False],
    )

    best_by_team["team_specialist_weight"] = best_by_team["team_specific_score"] / 100.0
    return team_author, best_by_team, author_overall


def build_weighted_team_player_trends(
    picks: pd.DataFrame,
    team_author: pd.DataFrame,
    author_overall: pd.DataFrame,
) -> pd.DataFrame:
    weight_columns = team_author[
        ["team_slug", "author_name", "team_specific_score", "team_specialist_weight", "attempts", "years_covered"]
    ].copy()
    weight_columns = weight_columns.rename(
        columns={
            "attempts": "historical_team_attempts",
            "years_covered": "historical_team_years_covered",
        }
    )
    overall_columns = author_overall[
        ["author_name", "overall_score", "overall_weight", "total_mocked_picks", "historical_years_covered"]
    ].copy()

    weighted = picks.merge(weight_columns, on=["team_slug", "author_name"], how="left")
    weighted = weighted.merge(overall_columns, on="author_name", how="left")
    weighted["applied_weight"] = weighted["team_specialist_weight"].fillna(weighted["overall_weight"])
    weighted["applied_weight"] = weighted["applied_weight"].fillna(1.0 / 3.0)
    weighted["used_team_specific_weight"] = weighted["team_specialist_weight"].notna()
    weighted["mock_id"] = weighted["mock_relative_url"]

    team_player_weighted = (
        weighted.groupby(["team_slug", "team_name", "player_name", "player_position"], dropna=False)
        .agg(
            weighted_mock_score=("applied_weight", "sum"),
            raw_mock_count=("mock_id", "count"),
            unique_mocks=("mock_id", "nunique"),
            unique_authors=("author_name", "nunique"),
            avg_pick=("pick", "mean"),
            median_pick=("pick", "median"),
            min_pick=("pick", "min"),
            max_pick=("pick", "max"),
            avg_author_weight=("applied_weight", "mean"),
            team_specific_weight_uses=("used_team_specific_weight", "sum"),
        )
        .reset_index()
    )

    team_totals = (
        weighted.groupby(["team_slug", "team_name"], dropna=False)
        .agg(
            weighted_team_total=("applied_weight", "sum"),
            raw_team_total=("mock_id", "count"),
        )
        .reset_index()
    )
    team_player_weighted = team_player_weighted.merge(
        team_totals, on=["team_slug", "team_name"], how="left"
    )
    team_player_weighted["weighted_share_of_team_picks"] = (
        team_player_weighted["weighted_mock_score"] / team_player_weighted["weighted_team_total"]
    )
    return team_player_weighted.sort_values(
        by=["team_name", "weighted_mock_score", "weighted_share_of_team_picks", "median_pick"],
        ascending=[True, False, False, True],
    )


def main() -> None:
    args = parse_args()
    processed_dir = Path(args.processed_dir)
    ensure_dir(processed_dir)

    historical_results = load_historical_pick_results(
        processed_dir, args.history_start_year, args.history_end_year
    )
    team_author, best_by_team, author_overall = build_team_specialist_tables(
        historical_results,
        min_attempts=args.min_attempts,
        min_years_covered=args.min_years_covered,
    )

    suffix = (
        f"{args.history_start_year}_{args.history_end_year}_"
        f"min{args.min_attempts}attempts_{args.min_years_covered}years"
    )
    team_author_path = processed_dir / f"historical_team_author_accuracy_{suffix}.csv"
    best_by_team_path = processed_dir / f"historical_best_authors_by_team_{suffix}.csv"
    author_overall_path = processed_dir / f"historical_author_overall_weights_{suffix}.csv"
    team_author.to_csv(team_author_path, index=False)
    best_by_team.to_csv(best_by_team_path, index=False)
    author_overall.to_csv(author_overall_path, index=False)

    print(f"Wrote {len(team_author)} team-author rows to {team_author_path.name}")
    print(f"Wrote {len(best_by_team)} best-team rows to {best_by_team_path.name}")
    print(f"Wrote {len(author_overall)} author-overall rows to {author_overall_path.name}")

    if args.target_year is None:
        return

    target_picks_path = processed_dir / str(args.target_year) / "mock-drafts__mock_picks.csv"
    if not target_picks_path.exists():
        print(
            f"Skipped weighted target-year trends because {target_picks_path} does not exist."
        )
        return

    target_picks = pd.read_csv(target_picks_path)
    target_first_round = target_picks[target_picks["round_number"] == 1].copy()
    weighted_team_player = build_weighted_team_player_trends(
        target_first_round,
        team_author,
        author_overall,
    )
    output_path = (
        processed_dir
        / str(args.target_year)
        / "mock-drafts__team_player_trends.weighted_by_team_specialists.csv"
    )
    ensure_dir(output_path.parent)
    weighted_team_player.to_csv(output_path, index=False)
    print(f"Wrote {len(weighted_team_player)} weighted team-player rows to {output_path.name}")


if __name__ == "__main__":
    main()
