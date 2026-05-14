# How It Works

Portfolio Reconciliation & Reporting Control Engine runs in this sequence:

1. Ingest CSV inputs from `data/`.
2. Validate required columns and basic integrity checks.
3. Load `dim_*` and `fact_*` tables into SQLite.
4. Rebuild daily positions from transactions + prices + FX.
5. Compute daily returns and monthly linked TWR.
6. Compute monthly Modified Dietz using external-flow timing assumptions.
7. Compute Brinson-Fachler attribution (allocation, selection, interaction).
8. Reconcile attribution sum against active return with a <5 bps gate.
9. Run break checks and store exceptions in `pbor_breaks`.
10. Optionally run PBOR-vs-custodian and ledger-vs-bank reconciliation from synthetic operational records.
11. Store reconciliation exceptions in `pbor_recon_exceptions`.
12. Export month-end artifacts to `outputs/YYYY-MM/`.
13. Generate a one-page PDF tear sheet, Excel workbook, and PNG chart for reporting.

Core run command:

```bash
python -m src.run_month_end --asof YYYY-MM-DD
```

Reconciliation demo:

```bash
python scripts/build_recon_demo_data.py
python -m src.run_month_end --asof 2026-01-31 --recon-data-dir data/recon_demo
```

Results walkthrough:

```bash
python -m src.show_results --month YYYY-MM
```
