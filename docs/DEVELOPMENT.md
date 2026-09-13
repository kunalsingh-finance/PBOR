# Development and operations

Run commands from the repository root using Python 3.11. Install dependencies with `python -m pip install -r requirements.txt`.

## Command-line runs

Run the bundled portfolio inputs through the reporting pipeline:

```bash
python -m src.run_month_end --asof 2026-01-10
```

Include the synthetic custodian and bank records for position and cash reconciliation:

```bash
python scripts/build_recon_demo_data.py
python -m src.run_month_end --asof 2026-01-10 --recon-data-dir data/recon_demo
python -m streamlit run app/dashboard.py
```

The fixed sample date is January 10, 2026. Its results describe the available sample interval, not a complete calendar month or current market performance. The reconciliation scenario deliberately includes unresolved breaks, so a completed pipeline run can correctly produce a blocked reporting sign-off.

The pipeline also accepts `--data-dir` for an alternative input folder and `--db-path` for an alternative SQLite database. Run `python -m src.run_month_end --help` for the full command reference.

## Optional public market data

```bash
python scripts/build_real_data.py --out-dir ./data_real/market_real
python scripts/last_month_end.py
```

Use the date returned by the second command as `YYYY-MM-DD` below:

```bash
python -m src.run_month_end --asof YYYY-MM-DD --data-dir ./data_real/market_real
```

The data builder uses public price history and a configured cash-return source. This path needs network access and depends on provider availability. It is separate from the offline sample verification path. Review the generated inputs and [methodology](../METHODOLOGY.md) before interpreting results.

## Generated files

Runs write reporting artifacts under `outputs/YYYY-MM/` and update the SQLite database at `pbor_lite.db` by default. These files are local generated results and are excluded from Git. Re-running the same period replaces its generated artifacts; retain a separate copy if a prior run is needed for comparison.

| File | Contents |
| --- | --- |
| `report.xlsx` | Performance, attribution, QA, reconciliation, and sign-off workbook, including `AutoReconExceptions` and `SignOffSummary` sheets. |
| `daily_returns.csv` | Daily portfolio performance series. |
| `monthly_returns.csv` | Period TWR, Modified Dietz, benchmark, and active returns. |
| `attribution.csv` | Brinson-Fachler sector attribution. |
| `attribution_reconciliation.csv` | Attribution-to-active-return control results. |
| `breaks.csv` | QA and control findings. |
| `qa_ingest_summary.csv` | Input-validation summary. |
| `recon_exceptions.csv` | Position and cash reconciliation queue. |
| `signoff_summary.csv` | Control-area status and final reporting readiness. |
| `onepager.pdf` | Summary tear sheet. |
| `tearsheet.png` | Image of the summary tear sheet. |
| `controls_table.png` | QA and controls image. |
| `summary.json` | Run metadata, generated-file manifest, and sign-off metrics. |

The checked-in [sample tear sheet](tearsheet-sample.png) illustrates the report format. Regenerate outputs to inspect results from the current code and inputs.

## Validation

```bash
python -m pytest -q
python scripts/run_full_demo.py
python scripts/verify_demo_outputs.py
python -m py_compile app/dashboard.py
```

The tests cover return calculations, attribution, reconciliation, sign-off, reporting, and calendar behavior. `run_full_demo.py` creates synthetic reconciliation inputs and executes the reporting pipeline. `verify_demo_outputs.py` checks generated files, workbook sheets, summary metadata, and SQLite tables. These checks use bundled synthetic data without external market-data calls.

The [CI workflow](../.github/workflows/ci.yml) installs dependencies, runs tests, builds and verifies the synthetic reporting pack, and compiles the dashboard. It runs on pushes, pull requests, and manual dispatch. To start a manual run, open **Actions → CI → Run workflow** in GitHub.

## Repository layout

| Path | Purpose |
| --- | --- |
| `app/` | Streamlit application. |
| `src/` | Returns, attribution, reconciliation, QA, sign-off, and reporting pipeline. |
| `pbor/` | Date-context and market-calendar helpers. |
| `data/` | Bundled input files and generated sample reconciliation records. |
| `docs/` | Methodology, scenario notes, and development reference. |
| `scripts/` | Data builders, output verification, and utilities. |
| `sql/` | SQLite schema and reporting views. |
| `tests/` | Automated behavioral checks. |
| `policy.yaml` | Return assumptions, benchmarks, and control tolerances. |
