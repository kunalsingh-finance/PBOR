PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS dim_security (
    security_id TEXT PRIMARY KEY,
    ticker TEXT NOT NULL,
    name TEXT NOT NULL,
    asset_class TEXT,
    sector TEXT,
    currency TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_portfolio (
    portfolio_id TEXT PRIMARY KEY,
    portfolio_name TEXT
);

CREATE TABLE IF NOT EXISTS fact_prices (
    date TEXT NOT NULL,
    security_id TEXT NOT NULL,
    price REAL NOT NULL,
    price_currency TEXT NOT NULL,
    source TEXT NOT NULL,
    PRIMARY KEY (date, security_id, source)
);

CREATE TABLE IF NOT EXISTS fact_fx_rates (
    date TEXT NOT NULL,
    ccy_pair TEXT NOT NULL,
    rate REAL NOT NULL,
    source TEXT NOT NULL,
    PRIMARY KEY (date, ccy_pair, source)
);

CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_id INTEGER PRIMARY KEY AUTOINCREMENT,
    date TEXT NOT NULL,
    portfolio_id TEXT NOT NULL,
    security_id TEXT,
    quantity REAL,
    price REAL,
    fees REAL,
    txn_type TEXT NOT NULL,
    cash_amount REAL
);

CREATE TABLE IF NOT EXISTS fact_holdings_reported (
    date TEXT NOT NULL,
    portfolio_id TEXT NOT NULL,
    security_id TEXT NOT NULL,
    quantity REAL NOT NULL,
    market_value_base REAL,
    cash_balance_base REAL,
    PRIMARY KEY (date, portfolio_id, security_id)
);

CREATE TABLE IF NOT EXISTS fact_benchmark_weights (
    date TEXT NOT NULL,
    benchmark_id TEXT NOT NULL,
    sector TEXT NOT NULL,
    weight REAL NOT NULL,
    PRIMARY KEY (date, benchmark_id, sector)
);

CREATE TABLE IF NOT EXISTS fact_benchmark_returns (
    date TEXT NOT NULL,
    benchmark_id TEXT NOT NULL,
    sector TEXT NOT NULL,
    return REAL NOT NULL,
    PRIMARY KEY (date, benchmark_id, sector)
);

CREATE TABLE IF NOT EXISTS pbor_daily_positions (
    date TEXT NOT NULL,
    portfolio_id TEXT NOT NULL,
    security_id TEXT NOT NULL,
    quantity_eod REAL NOT NULL,
    price_local REAL,
    security_currency TEXT,
    fx_to_base REAL,
    market_value_base REAL,
    cash_balance_base REAL,
    PRIMARY KEY (date, portfolio_id, security_id)
);

CREATE TABLE IF NOT EXISTS pbor_daily_returns (
    date TEXT NOT NULL,
    portfolio_id TEXT NOT NULL,
    portfolio_value_base REAL NOT NULL,
    external_flow_base REAL NOT NULL,
    daily_return REAL,
    benchmark_return REAL,
    PRIMARY KEY (date, portfolio_id)
);

CREATE TABLE IF NOT EXISTS pbor_monthly_returns (
    month_end TEXT NOT NULL,
    portfolio_id TEXT NOT NULL,
    portfolio_return_twr REAL,
    portfolio_return_dietz REAL,
    dietz_denominator REAL,
    benchmark_return REAL,
    active_return REAL,
    PRIMARY KEY (month_end, portfolio_id)
);

CREATE TABLE IF NOT EXISTS pbor_attribution_monthly (
    month_end TEXT NOT NULL,
    portfolio_id TEXT NOT NULL,
    benchmark_id TEXT NOT NULL,
    sector TEXT NOT NULL,
    w_p REAL NOT NULL,
    w_b REAL NOT NULL,
    r_p REAL NOT NULL,
    r_b REAL NOT NULL,
    allocation_effect REAL NOT NULL,
    selection_effect REAL NOT NULL,
    interaction_effect REAL NOT NULL,
    active_effect REAL NOT NULL,
    PRIMARY KEY (month_end, portfolio_id, benchmark_id, sector)
);

CREATE TABLE IF NOT EXISTS pbor_breaks (
    break_id INTEGER PRIMARY KEY AUTOINCREMENT,
    asof_date TEXT NOT NULL,
    portfolio_id TEXT,
    break_type TEXT NOT NULL,
    severity TEXT NOT NULL,
    details TEXT NOT NULL,
    root_cause TEXT,
    resolution TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP
);
