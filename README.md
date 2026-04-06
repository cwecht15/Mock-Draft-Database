# NFL Mock Draft Scraper

This project scrapes NFL Mock Draft Database pages so we can:

- store historical and current mock-draft picks in a normalized format
- evaluate analyst accuracy on past draft classes
- identify player, team, and position trends for current draft cycles

The first implementation is centered on NFL Mock Draft Database because its individual mock pages already expose structured metadata like outlet, author, publish date, accuracy, and per-pick selections.

## Scripts

- `app.py`: local interactive viewer for qualified mockers, current first-round consensus, and current favorites by team and pick
- `scripts/scrape_nflmockdraftdatabase.py`: downloads list pages, individual mock pages, and optional actual draft results into normalized CSVs
- `scripts/analyze_mock_trends.py`: builds analyst accuracy summaries and mock-draft trend outputs from the scraped CSVs
- `scripts/build_team_specialist_weights.py`: builds historical author-by-team tables and, when a future year exists, weighted team-player trend files based on team-specific author history

## Site Coverage

The scraper supports:

- `mock-drafts`: national mock drafts, usually first-round mocks
- `team-mock-drafts`: team-based full mocks from the same site
- `teams`: per-team aggregator pages like `/teams/2026/philadelphia-eagles`, which list team-specific mock articles and recent team consensus data

It can also optionally scrape the actual draft results page at `https://www.nflmockdraftdatabase.com/nfl-draft-results-<year>` for historical scoring.

## Why This Source Works

NFL Mock Draft Database embeds a structured JSON payload in each page's `data-react-props` attribute. That makes it much more stable than trying to scrape visual card markup.

## Example Workflow

Historical analyst accuracy first:

```powershell
python scripts\scrape_nflmockdraftdatabase.py --year 2025 --section mock-drafts --published-month 4 --published-day-min 10 --latest-author-mock-only --resume --checkpoint-every 25 --include-actual-results
python scripts\analyze_mock_trends.py --year 2025 --section mock-drafts
```

Current-cycle first-round and team-based trend tracking:

```powershell
python scripts\scrape_nflmockdraftdatabase.py --year 2026 --section mock-drafts --section teams --published-days-back 14 --latest-author-mock-only --resume
python scripts\analyze_mock_trends.py --year 2026 --section mock-drafts --section teams
```

Pull just one team page and its linked team mocks:

```powershell
python scripts\scrape_nflmockdraftdatabase.py --year 2026 --section teams --team-slug philadelphia-eagles --published-days-back 14 --latest-author-mock-only --resume
python scripts\analyze_mock_trends.py --year 2026 --section teams
```

Historical April-only run across multiple years:

```powershell
$years = 2025,2024,2023,2022,2021,2020
foreach ($year in $years) {
  python scripts\scrape_nflmockdraftdatabase.py --year $year --section mock-drafts --published-month 4 --published-day-min 10 --latest-author-mock-only --max-pages 25 --resume --checkpoint-every 25 --include-actual-results
  python scripts\analyze_mock_trends.py --year $year --section mock-drafts
}
```

Build stable best-authors-by-team outputs from the historical sample:

```powershell
python scripts\build_team_specialist_weights.py --history-start-year 2020 --history-end-year 2025 --min-attempts 5 --min-years-covered 4
```

Once `2026` mocks exist, build weighted team trends:

```powershell
python scripts\build_team_specialist_weights.py --history-start-year 2020 --history-end-year 2025 --min-attempts 5 --min-years-covered 4 --target-year 2026
```

Launch the interactive viewer:

```powershell
streamlit run app.py
```

## Outputs

For each year and section the scraper writes:

- `data/raw/<year>/<section>/index/*.html`
- `data/raw/<year>/<section>/mocks/*.html`
- `data/processed/<year>/<section>__mock_metadata.csv`
- `data/processed/<year>/<section>__mock_picks.csv`

For `teams`, it also writes:

- `data/raw/<year>/teams/team_pages/<team-slug>__page_<n>.html`
- `data/processed/<year>/teams__team_pages.csv`
- `data/processed/<year>/teams__team_consensus.csv`

If `--include-actual-results` is used, it also writes:

- `data/raw/<year>/actual/nfl_draft_results_<year>.html`
- `data/processed/<year>/actual_draft_results_<year>.csv`

While the scraper is running, it also writes:

- `data/processed/<year>/<section>__mock_metadata.checkpoint.csv`
- `data/processed/<year>/<section>__mock_picks.checkpoint.csv`
- `data/processed/<year>/<section>__progress.json`

The analyzer writes:

- `data/processed/<year>/<section>__player_trends.csv`
- `data/processed/<year>/<section>__team_player_trends.csv`
- `data/processed/<year>/<section>__team_position_trends.csv`
- `data/processed/<year>/<section>__mock_accuracy.csv` when actual results are available
- `data/processed/<year>/<section>__author_accuracy.csv` when actual results are available

The team-specialist script writes:

- `data/processed/historical_team_author_accuracy_<start>_<end>_min<attempts>attempts_<years>years.csv`
- `data/processed/historical_best_authors_by_team_<start>_<end>_min<attempts>attempts_<years>years.csv`
- `data/processed/historical_author_overall_weights_<start>_<end>_min<attempts>attempts_<years>years.csv`
- `data/processed/<target-year>/mock-drafts__team_player_trends.weighted_by_team_specialists.csv` when the target year exists

The app reads the processed historical author accuracy files plus the current-year `mock-drafts__mock_picks.csv` file. It lets you:

- pull a fresh rolling 14-day current-year scrape from inside the sidebar
- filter the current-year views by published date in the sidebar
- filter to historically above-average mockers with at least a chosen number of years
- build a full first-round consensus board from only those qualified authors
- inspect favorite picks by team and by pick slot

## Notes

- The site appears to redirect-loop on standard Python `requests` in this environment, so the scraper includes a PowerShell `Invoke-WebRequest` fallback for Windows.
- Historical analyst accuracy is now driven by custom metrics built from actual draft results:
  - `1` point for mocking a player into round one
  - `2` points for mocking the correct player-to-team match
  - `custom_accuracy_score` is the percentage of total possible points earned
- `--resume` reuses raw HTML files and checkpoint CSVs instead of re-fetching the same pages.
- `--published-month 4` is a good default for historical accuracy pulls because it focuses the dataset on the final pre-draft stretch instead of the whole draft cycle.
- `--published-day-min 10` is useful for narrowing the sample to the final run-up to draft week.
- `--published-days-back 14` is useful for a rolling recent-mocks workflow that keeps updating with the newest mock drafts.
- `--latest-author-mock-only` drops older mocks from authors who already have a later mock in the scrape window, which keeps the historical dataset focused on final takes.
- For `--section teams`, latest-author dedupe is done per team so an author can still appear once for the Eagles, once for the Bears, and so on within the same scrape window.
