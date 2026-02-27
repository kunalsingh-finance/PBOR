# How It Works

PBOR-Lite runs in this sequence:

1. Ingest CSV inputs from `data/`.
2. Validate required columns and basic integrity checks.
3. Load `dim_*` and `fact_*` tables into SQLite.
4. Rebuild daily positions from transactions + prices + FX.
5. Compute daily returns and monthly linked TWR.
6. Compute monthly Modified Dietz using external-flow timing assumptions.
7. Compute Brinson-Fachler attribution (allocation, selection, interaction).
8. Reconcile attribution sum against active return with a <5 bps gate.
9. Run break checks and store exceptions in `pbor_breaks`.
10. Export month-end artifacts to `outputs/YYYY-MM/`.
11. Generate a one-page PDF tear sheet, Excel workbook, and PNG chart for reporting.

Core run command:

```bash
python -m src.run_month_end --asof YYYY-MM-DD
```

Results walkthrough:

```bash
python -m src.show_results --month YYYY-MM
```
