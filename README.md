# PBOR-Lite

PBOR-Lite is an end-to-end Performance Book of Record workflow for monthly performance measurement, attribution, reconciliation, and reporting.

## Core capabilities

- CSV ingestion with schema validation and policy-driven controls
- SQL PBOR data model (`dim_*`, `fact_*`, `pbor_*`) in SQLite
- Daily positions, market values, and daily return series
- Monthly Time-Weighted Return (TWR) and Modified Dietz returns
- Brinson-Fachler sector attribution (allocation, selection, interaction)
- Attribution-to-active reconciliation gate (<5 bps tolerance)
- Automated break detection and auditable exception logging
- Interview-ready reporting pack (CSV, Excel, PDF, PNG, Markdown, JSON)

## Project structure

```text
PBOR-Lite/
  data/
  docs/
  outputs/
  scripts/
  sql/
  src/
  tests/
  policy.yaml
  requirements.txt
```

## Run the full workflow

```powershell
cd C:\Users\perso\OneDrive\Documents\PBOR-Lite
powershell -ExecutionPolicy Bypass -File .\scripts\run_demo.ps1 -AsOf 2026-01-10
```

## Run commands directly

```bash
python -m src.run_month_end --asof 2026-01-10
python -m src.show_results --month 2026-01
```

## Convert real GSMIF data and run

```powershell
cd C:\Users\perso\OneDrive\Documents\PBOR-Lite
python .\scripts\convert_gsmif_to_pbor.py `
  --input "C:\Users\perso\Downloads\gsmif (1).csv" `
  --out-dir ".\data_real\gsmif" `
  --portfolio-id PF_GSMIF `
  --benchmark-id BM1 `
  --benchmark-source stooq `
  --benchmark-symbol SPY.US

python -m src.run_month_end --asof 2026-02-18 --data-dir .\data_real\gsmif
python -m src.show_results --month 2026-02
```

If external market download is unavailable, the converter automatically falls back to internal NAV-derived benchmark returns.

## Build market-based dummy stock data and run

```powershell
cd C:\Users\perso\OneDrive\Documents\PBOR-Lite
python .\scripts\build_market_dummy_data.py `
  --out-dir ".\data_real\market_dummy" `
  --start 2025-08-22 `
  --end 2026-02-18 `
  --portfolio-id PF_MKT `
  --benchmark-id BM1

python -m src.run_month_end --asof 2026-02-18 --data-dir .\data_real\market_dummy
python -m src.show_results --month 2026-02
```

The builder attempts to pull daily prices from Stooq first. If a symbol cannot be downloaded, it automatically falls back to deterministic synthetic price paths and records the source in `dataset_summary.json`.

## Output package

Outputs are written to `outputs/YYYY-MM/`.

- `daily_returns.csv`: daily portfolio value, external flows, and benchmark-linked daily return
- `monthly_returns.csv`: monthly TWR, Modified Dietz, benchmark return, active return
- `attribution.csv`: Brinson-Fachler effects by sector
- `attribution_reconciliation.csv`: attribution sum vs active return reconciliation and tolerance status
- `breaks.csv`: break log with severity and investigation notes
- `qa_ingest_summary.csv`: ingestion controls and validation results
- `report.xlsx`: refreshable workbook with Summary, Returns, Attribution, and Break tabs
- `onepager.pdf`: executive one-page performance and attribution tear sheet
- `tearsheet.png`: chart image for presentations and applications
- `onepager.md`: analyst narrative summary of the month-end cycle
- `summary.json`: machine-readable run metadata and artifact inventory

Publication policy:

- Attribution is published only when reconciliation diff is below the configured tolerance (`< 5 bps` by default).
- Reporting window is month-to-date as-of (`reporting_window: mtd_asof` in `policy.yaml`).

## Technology stack

- Python 3.11+
- pandas, numpy
- matplotlib
- openpyxl
- SQLite
- pytest

## Documentation

- Methodology: `docs/Methodology.md`
- Process flow: `docs/HowItWorks.md`
