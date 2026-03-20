# PBOR-Lite

Performance book of record for monthly returns, attribution, QA, and reporting.

## What It Does

- Builds PBOR input files from market data.
- Runs return, attribution, and control workflows.
- Stores results in SQLite and publishes monthly reports and a dashboard.

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

Each run writes to `outputs/YYYY-MM/`.

- `report.xlsx`: full workbook with returns, attribution, and breaks
- `onepager.pdf`: monthly tear sheet
- `tearsheet.png`: image export
- `controls_table.png`: controls snapshot
- `onepager.md`: written summary
- `summary.json`: run metadata
- `attribution_reconciliation.csv`: reconciliation output

## Methodology

Methodology notes are in [METHODOLOGY.md](METHODOLOGY.md).

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

Python, pandas, NumPy, yfinance, FRED, requests, PyYAML, Streamlit, SQLite, matplotlib, and openpyxl.
