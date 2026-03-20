# PBOR-Lite

Performance Book of Record workflow for return measurement, attribution, controls, and reporting.

## What It Does

- Builds PBOR input data from live prices, dividends, benchmark series, and SOFR policy data.
- Computes daily and monthly returns, Brinson-Fachler attribution, QA breaks, and reconciliation controls.
- Publishes a SQLite-backed output pack and Streamlit dashboard for review.

## Architecture

```text
yfinance + FRED
       |
       v
scripts/build_real_data.py
       |
       v
src/run_month_end.py
       |
       +--> pbor_lite.db
       |
       +--> outputs/YYYY-MM/
       |
       v
app/dashboard.py
```

## Quick Start

```powershell
python -m pip install -r requirements.txt
python scripts/build_real_data.py
python -m src.run_month_end --asof $(python scripts/last_month_end.py) --data-dir .\data_real\market_real
streamlit run app/dashboard.py
```

## Output Pack

Each month-end run writes to `outputs/YYYY-MM/`.

- `summary.json`: machine-readable run metadata and file inventory
- `report.xlsx`: workbook with returns, attribution, and QA tabs
- `onepager.pdf`: month-end tear sheet
- `tearsheet.png`: presentation-ready image export
- `controls_table.png`: controls snapshot used by the report pack
- `onepager.md`: narrative summary
- `attribution_reconciliation.csv`: reconciliation evidence

## Methodology

See [METHODOLOGY.md](METHODOLOGY.md).

## Project Structure

```text
PBOR-Lite/
|-- app/
|   `-- dashboard.py
|-- data/
|-- data_real/
|   |-- market_dummy/
|   `-- market_real/
|-- outputs/
|-- scripts/
|   |-- build_market_dummy_data.py
|   |-- build_real_data.py
|   `-- last_month_end.py
|-- sql/
|-- src/
|   |-- attribution.py
|   |-- export.py
|   |-- ingest.py
|   |-- qa.py
|   |-- reconciliation.py
|   |-- returns.py
|   `-- run_month_end.py
|-- tests/
|-- .github/
|   `-- workflows/
|       `-- monthly_run.yml
|-- METHODOLOGY.md
|-- pbor_lite.db
|-- policy.yaml
`-- requirements.txt
```

## Tech Stack

Python, pandas, NumPy, yfinance, FRED, PyYAML, requests, Streamlit, SQLite, matplotlib, openpyxl.
