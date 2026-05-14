from __future__ import annotations

from pathlib import Path

import pandas as pd

POSITION_KEY = ["asof_date", "portfolio_id", "account_id", "security_id"]
CASH_KEY = ["asof_date", "portfolio_id", "account_id", "currency"]

POSITION_COLUMNS = [
    "asof_date",
    "portfolio_id",
    "account_id",
    "security_id",
    "ticker",
    "quantity",
    "price",
    "market_value_base",
]
CASH_COLUMNS = ["asof_date", "portfolio_id", "account_id", "currency", "cash_balance"]

RECON_OUTPUT_COLUMNS = [
    "asof_date",
    "portfolio_id",
    "account_id",
    "security_id",
    "ticker",
    "currency",
    "break_type",
    "status",
    "workflow_status",
    "severity",
    "sla_bucket",
    "internal_quantity",
    "external_quantity",
    "quantity_diff",
    "internal_price",
    "external_price",
    "price_diff",
    "internal_market_value",
    "external_market_value",
    "market_value_diff",
    "internal_cash",
    "external_cash",
    "cash_diff",
    "root_cause",
    "resolution_note",
    "action_required",
    "owner",
    "age_days",
]

NON_EXCEPTION_STATUSES = {"MATCHED", "WITHIN_TOLERANCE"}

ROOT_CAUSES = {
    "QUANTITY_BREAK": "Pending trade settlement, booking timing issue, or unmatched transaction",
    "PRICE_BREAK": "Pricing source mismatch or stale price",
    "MARKET_VALUE_BREAK": "Valuation difference caused by quantity, price, FX, or timing mismatch",
    "MISSING_IN_CUSTODIAN": "Custodian feed delay, failed settlement, or unmatched security mapping",
    "MISSING_IN_INTERNAL": "Missing PBOR booking or security setup issue",
    "CASH_BREAK": "Settlement, fee, dividend, wire, or interest timing difference",
    "MISSING_IN_BANK": "Bank feed missing internal cash record",
    "MISSING_IN_INTERNAL_CASH": "Cash movement missing from internal ledger",
    "MATCHED": "No break",
    "WITHIN_TOLERANCE": "Difference within configured tolerance",
}

RESOLUTION_NOTES = {
    "QUANTITY_BREAK": "Review trade blotter, settlement date, and custodian booking",
    "PRICE_BREAK": "Validate pricing source and rerun price load",
    "MARKET_VALUE_BREAK": "Review quantity, price, FX, and valuation timing",
    "MISSING_IN_CUSTODIAN": "Confirm custodian feed completeness and settlement status",
    "MISSING_IN_INTERNAL": "Review PBOR booking and security setup",
    "CASH_BREAK": "Review cash ledger, bank activity, fees, dividends, and wires",
    "MISSING_IN_BANK": "Confirm bank feed completeness and cash account mapping",
    "MISSING_IN_INTERNAL_CASH": "Review cash ledger posting and internal account setup",
    "MATCHED": "No action required",
    "WITHIN_TOLERANCE": "Monitor only",
}

ACTION_REQUIRED = {
    "QUANTITY_BREAK": "Review trade blotter, settlement status, and custodian booking",
    "PRICE_BREAK": "Validate pricing source and stale price logic",
    "MARKET_VALUE_BREAK": "Review quantity, price, FX, and valuation timing",
    "MISSING_IN_CUSTODIAN": "Confirm custodian feed completeness and settlement status",
    "MISSING_IN_INTERNAL": "Review PBOR booking and security setup",
    "CASH_BREAK": "Review cash ledger, bank activity, fees, dividends, wires, and interest",
    "MISSING_IN_BANK": "Confirm bank feed completeness and account mapping",
    "MISSING_IN_INTERNAL_CASH": "Review internal cash ledger posting",
    "MATCHED": "No action required",
    "WITHIN_TOLERANCE": "No action required",
}


def _validate_required_columns(frame: pd.DataFrame, required_columns: list[str], source_name: str) -> None:
    missing = [column for column in required_columns if column not in frame.columns]
    if missing:
        missing_text = ", ".join(missing)
        raise ValueError(f"{source_name} is missing required column(s): {missing_text}")


def _load_csv(data_dir: Path, file_name: str, required_columns: list[str]) -> pd.DataFrame:
    path = data_dir / file_name
    if not path.exists():
        raise FileNotFoundError(f"Required reconciliation input not found: {path}")
    frame = pd.read_csv(path)
    _validate_required_columns(frame, required_columns, file_name)
    return frame


def load_recon_inputs(data_dir: Path) -> dict[str, pd.DataFrame]:
    data_dir = Path(data_dir)
    if not data_dir.exists():
        raise FileNotFoundError(f"Reconciliation data directory not found: {data_dir}")

    return {
        "internal_positions": _load_csv(data_dir, "internal_positions.csv", POSITION_COLUMNS),
        "custodian_positions": _load_csv(data_dir, "custodian_positions.csv", POSITION_COLUMNS),
        "internal_cash": _load_csv(data_dir, "internal_cash.csv", CASH_COLUMNS),
        "bank_cash": _load_csv(data_dir, "bank_cash.csv", CASH_COLUMNS),
    }


def _position_recon_policy(policy: dict[str, object]) -> dict[str, object]:
    settings = policy.get("position_recon", policy)
    return settings if isinstance(settings, dict) else {}


def _policy_float(policy: dict[str, object], key: str, default: float) -> float:
    settings = _position_recon_policy(policy)
    try:
        return float(settings.get(key, default))
    except (TypeError, ValueError):
        return default


def _coerce_numeric(frame: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    out = frame.copy()
    for column in columns:
        out[column] = pd.to_numeric(out[column], errors="coerce")
    return out


def _abs_or_zero(value: object) -> float:
    if value is None or pd.isna(value):
        return 0.0
    return abs(float(value))


def _price_diff_pct(row: pd.Series) -> float:
    diff = _abs_or_zero(row.get("price_diff"))
    internal_price = row.get("internal_price")
    external_price = row.get("external_price")
    base = internal_price if internal_price is not None and not pd.isna(internal_price) and float(internal_price) != 0 else external_price
    if base is None or pd.isna(base) or float(base) == 0:
        return diff
    return diff / abs(float(base))


def _has_tiny_difference(row: pd.Series, columns: list[str]) -> bool:
    return any(_abs_or_zero(row.get(column)) > 0 for column in columns)


def reconcile_positions(
    internal_positions: pd.DataFrame,
    custodian_positions: pd.DataFrame,
    policy: dict[str, object],
) -> pd.DataFrame:
    _validate_required_columns(internal_positions, POSITION_COLUMNS, "internal_positions")
    _validate_required_columns(custodian_positions, POSITION_COLUMNS, "custodian_positions")

    quantity_tolerance = _policy_float(policy, "quantity_tolerance", 0.0)
    price_tolerance_pct = _policy_float(policy, "price_tolerance_pct", 0.0005)
    market_value_tolerance = _policy_float(policy, "market_value_tolerance", 100.0)

    internal = _coerce_numeric(internal_positions, ["quantity", "price", "market_value_base"]).rename(
        columns={
            "ticker": "ticker_internal",
            "quantity": "internal_quantity",
            "price": "internal_price",
            "market_value_base": "internal_market_value",
        }
    )
    external = _coerce_numeric(custodian_positions, ["quantity", "price", "market_value_base"]).rename(
        columns={
            "ticker": "ticker_external",
            "quantity": "external_quantity",
            "price": "external_price",
            "market_value_base": "external_market_value",
        }
    )

    merged = internal.merge(external, on=POSITION_KEY, how="outer", indicator=True)
    merged["ticker"] = merged["ticker_internal"].combine_first(merged["ticker_external"])
    merged["quantity_diff"] = merged["external_quantity"] - merged["internal_quantity"]
    merged["price_diff"] = merged["external_price"] - merged["internal_price"]
    merged["market_value_diff"] = merged["external_market_value"] - merged["internal_market_value"]

    def classify(row: pd.Series) -> str:
        if row["_merge"] == "left_only":
            return "MISSING_IN_CUSTODIAN"
        if row["_merge"] == "right_only":
            return "MISSING_IN_INTERNAL"
        if _abs_or_zero(row["quantity_diff"]) > quantity_tolerance:
            return "QUANTITY_BREAK"
        if _price_diff_pct(row) > price_tolerance_pct:
            return "PRICE_BREAK"
        if _abs_or_zero(row["market_value_diff"]) > market_value_tolerance:
            return "MARKET_VALUE_BREAK"
        if _has_tiny_difference(row, ["quantity_diff", "price_diff", "market_value_diff"]):
            return "WITHIN_TOLERANCE"
        return "MATCHED"

    merged["status"] = merged.apply(classify, axis=1)
    merged["currency"] = pd.NA

    return merged[
        [
            "asof_date",
            "portfolio_id",
            "account_id",
            "security_id",
            "ticker",
            "currency",
            "status",
            "internal_quantity",
            "external_quantity",
            "quantity_diff",
            "internal_price",
            "external_price",
            "price_diff",
            "internal_market_value",
            "external_market_value",
            "market_value_diff",
        ]
    ].copy()


def reconcile_cash(
    internal_cash: pd.DataFrame,
    bank_cash: pd.DataFrame,
    policy: dict[str, object],
) -> pd.DataFrame:
    _validate_required_columns(internal_cash, CASH_COLUMNS, "internal_cash")
    _validate_required_columns(bank_cash, CASH_COLUMNS, "bank_cash")

    cash_tolerance = _policy_float(policy, "cash_tolerance", 50.0)

    internal = _coerce_numeric(internal_cash, ["cash_balance"]).rename(
        columns={"cash_balance": "internal_cash"}
    )
    external = _coerce_numeric(bank_cash, ["cash_balance"]).rename(columns={"cash_balance": "external_cash"})

    merged = internal.merge(external, on=CASH_KEY, how="outer", indicator=True)
    merged["cash_diff"] = merged["external_cash"] - merged["internal_cash"]

    def classify(row: pd.Series) -> str:
        if row["_merge"] == "left_only":
            return "MISSING_IN_BANK"
        if row["_merge"] == "right_only":
            return "MISSING_IN_INTERNAL_CASH"
        if _abs_or_zero(row["cash_diff"]) > cash_tolerance:
            return "CASH_BREAK"
        if _abs_or_zero(row["cash_diff"]) > 0:
            return "WITHIN_TOLERANCE"
        return "MATCHED"

    merged["status"] = merged.apply(classify, axis=1)
    merged["security_id"] = pd.NA
    merged["ticker"] = pd.NA

    return merged[
        [
            "asof_date",
            "portfolio_id",
            "account_id",
            "security_id",
            "ticker",
            "currency",
            "status",
            "internal_cash",
            "external_cash",
            "cash_diff",
        ]
    ].copy()


def _break_type(status: str) -> str:
    if status == "MATCHED":
        return "NO_BREAK"
    if status == "WITHIN_TOLERANCE":
        return "WITHIN_TOLERANCE"
    return status


def _severity(row: pd.Series) -> str:
    status = str(row["status"])
    if status == "MATCHED":
        return "INFO"
    if status == "WITHIN_TOLERANCE":
        return "LOW"
    if status in {
        "MISSING_IN_CUSTODIAN",
        "MISSING_IN_INTERNAL",
        "MISSING_IN_BANK",
        "MISSING_IN_INTERNAL_CASH",
        "QUANTITY_BREAK",
        "CASH_BREAK",
    }:
        return "HIGH"
    if status == "MARKET_VALUE_BREAK":
        return "HIGH" if _abs_or_zero(row.get("market_value_diff")) > 10000 else "MEDIUM"
    if status == "PRICE_BREAK":
        return "MEDIUM"
    return "LOW"


def _workflow_status(status: str) -> str:
    return "CLOSED" if status in NON_EXCEPTION_STATUSES else "OPEN"


def _sla_bucket(status: str, age_days: int) -> str:
    if status in NON_EXCEPTION_STATUSES:
        return "N/A"
    if age_days <= 1:
        return "CURRENT"
    if age_days <= 3:
        return "WATCHLIST"
    return "BREACHED"


def _action_required(status: str) -> str:
    return ACTION_REQUIRED.get(status, "Review exception and document resolution")


def _enrich_recon_rows(frame: pd.DataFrame) -> pd.DataFrame:
    out = frame.copy()
    out["break_type"] = out["status"].map(_break_type)
    out["severity"] = out.apply(_severity, axis=1)
    out["root_cause"] = out["status"].map(ROOT_CAUSES).fillna("Review required")
    out["resolution_note"] = out["status"].map(RESOLUTION_NOTES).fillna("Review and document resolution")
    out["owner"] = "Ops Analyst"
    out["age_days"] = 0
    out["workflow_status"] = out["status"].map(_workflow_status)
    out["sla_bucket"] = out.apply(lambda row: _sla_bucket(str(row["status"]), int(row["age_days"])), axis=1)
    out["action_required"] = out["status"].map(_action_required)
    for column in RECON_OUTPUT_COLUMNS:
        if column not in out.columns:
            out[column] = pd.NA
    return out[RECON_OUTPUT_COLUMNS].copy()


def run_auto_recon(data_dir: Path, policy: dict[str, object]) -> pd.DataFrame:
    inputs = load_recon_inputs(data_dir)
    position_recon = reconcile_positions(
        internal_positions=inputs["internal_positions"],
        custodian_positions=inputs["custodian_positions"],
        policy=policy,
    )
    cash_recon = reconcile_cash(
        internal_cash=inputs["internal_cash"],
        bank_cash=inputs["bank_cash"],
        policy=policy,
    )
    combined = pd.concat([position_recon, cash_recon], ignore_index=True, sort=False)
    if combined.empty:
        return pd.DataFrame(columns=RECON_OUTPUT_COLUMNS)
    return _enrich_recon_rows(combined)
