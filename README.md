# PBOR-Lite

PBOR-Lite is a personal and educational PBOR-style month-end reporting workflow simulation built in Python. It uses bundled sample inputs and optional public market data to show how return measurement, attribution, reconciliation, QA, and reporting fit together in a single portfolio analytics process.

The GitHub repository slug is `PBOR`; PBOR-Lite is the project name used in the codebase and documentation.

## Business Workflow

- Start from holdings, transactions, benchmark weights, prices, and FX inputs.
- Rebuild daily positions and calculate month-end performance using TWR and Modified Dietz.
- Compare portfolio results with a benchmark and compute sector-level attribution.
- Check whether attribution ties back to active return and flag QA breaks when it does not.
- Generate a month-end review pack in SQLite, Excel, Markdown, PDF, and PNG formats.

## Why This Project Matters

Investment operations and portfolio analytics work is not just about calculating a return. It also requires clean inputs, benchmark alignment, reconciliation checks, and an output pack that someone can review quickly at month-end.

PBOR-Lite keeps that workflow small enough to inspect end to end while still reflecting the control and review steps that matter in performance reporting.

## Sample Output

![PBOR-Lite tear sheet sample](docs/tearsheet-sample.png)

Sample tear sheet generated from the repo's bundled sample dataset. It is included to show the reporting format of this personal project, not live client reporting.

## What It Does

- Loads portfolio, benchmark, price, FX, and transaction inputs into a reproducible month-end workflow.
- Rebuilds daily positions and calculates monthly Time-Weighted Return and Modified Dietz return.
- Computes Brinson-Fachler sector attribution and reconciles it to reported active return.
- Runs QA checks for missing data, return outliers, holdings mismatches, and attribution control breaks.
- Produces a SQLite-backed dataset, a one-page tear sheet, an Excel report pack, and supporting CSV outputs.
- Supports both bundled demo inputs and an optional public-market-data build path.

## Tech Stack

Python, pandas, NumPy, SQLite, Streamlit, matplotlib, openpyxl, PyYAML, requests, `yfinance`, FRED, `exchange_calendars`, and `pandas_market_calendars`.

## Quick Start

Use the bundled synthetic dataset for a reproducible local run:

```bash
python -m pip install -r requirements.txt
python -m src.run_month_end --asof 2026-01-10
python -m src.show_results --month 2026-01
streamlit run app/dashboard.py
```

Optional: build a public-market-data input set, then run month-end against it:

```bash
python scripts/build_real_data.py --out-dir ./data_real/market_real
python scripts/last_month_end.py
python -m src.run_month_end --asof YYYY-MM-DD --data-dir ./data_real/market_real
```

## Project Outputs

Each month-end run writes a dated output folder under `outputs/YYYY-MM/` plus an updated SQLite database at `pbor_lite.db`.

- `report.xlsx`: workbook with performance, attribution, reconciliation, and break details
- `onepager.pdf`: one-page summary tear sheet for quick review
- `tearsheet.png`: image export of the tear sheet
- `controls_table.png`: QA and control snapshot
- `onepager.md`: text summary of the month-end pack
- `summary.json`: run metadata and file manifest
- `attribution_reconciliation.csv`: attribution-to-active-return control output

## Methodology

- [METHODOLOGY.md](METHODOLOGY.md): detailed notes on return methodology, attribution, reconciliation, and controls
- [docs/Methodology.md](docs/Methodology.md): concise formula reference
- [docs/HowItWorks.md](docs/HowItWorks.md): workflow walkthrough

## What I Learned / Limitations

- Attribution only becomes useful when it ties back to reported active return, so the workflow treats failed reconciliations as under review rather than presenting them as final.
- This is a personal and educational project, not a production accounting or reporting platform.
- The optional live-data path uses public market data sources (`yfinance` and FRED), and the bundled `data/` folder uses synthetic sample inputs for deterministic local testing.
- The workflow simulates PBOR-style month-end reporting, return measurement, attribution, and QA review in a smaller-scale environment.
- Controls, benchmark construction, and portfolio coverage are intentionally simplified to keep the project transparent and runnable from a public repo.

## Testing

```bash
pytest -q
```

## Repository Structure

- `src/`: month-end pipeline modules for returns, attribution, reconciliation, QA, export, and reporting
- `pbor/`: date-context and market-calendar helpers
- `app/`: Streamlit dashboard over the SQLite output
- `data/`: bundled synthetic input files for local runs
- `docs/`: methodology notes and sample tear sheet image
- `scripts/`: public-data builder and helper utilities
- `sql/`: SQLite DDL and reporting views
- `tests/`: pytest coverage for return logic, attribution, reporting, and date/calendar behavior
