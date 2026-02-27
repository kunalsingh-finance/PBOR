CREATE VIEW IF NOT EXISTS vw_monthly_active_check AS
SELECT
    month_end,
    portfolio_id,
    portfolio_return_twr,
    benchmark_return,
    active_return,
    portfolio_return_twr - benchmark_return AS active_recomputed
FROM pbor_monthly_returns;

CREATE VIEW IF NOT EXISTS vw_attribution_reconcile AS
SELECT
    month_end,
    portfolio_id,
    benchmark_id,
    ROUND(SUM(active_effect), 10) AS active_from_effects
FROM pbor_attribution_monthly
GROUP BY month_end, portfolio_id, benchmark_id;

CREATE VIEW IF NOT EXISTS vw_break_counts AS
SELECT
    asof_date,
    severity,
    break_type,
    COUNT(*) AS break_count
FROM pbor_breaks
GROUP BY asof_date, severity, break_type;
