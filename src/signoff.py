from __future__ import annotations

import pandas as pd

NON_EXCEPTION_STATUSES = {"MATCHED", "WITHIN_TOLERANCE"}
SIGNOFF_COLUMNS = [
    "control_area",
    "status",
    "high_severity_count",
    "open_exception_count",
    "ready_for_signoff",
    "review_note",
    "action_required",
]


def _high_severity_count(frame: pd.DataFrame) -> int:
    if frame.empty or "severity" not in frame.columns:
        return 0
    return int(frame["severity"].astype(str).str.upper().eq("HIGH").sum())


def _open_recon_mask(frame: pd.DataFrame) -> pd.Series:
    if frame.empty:
        return pd.Series(dtype=bool)
    if "workflow_status" in frame.columns:
        return frame["workflow_status"].astype(str).str.upper().ne("CLOSED")
    if "status" in frame.columns:
        return ~frame["status"].astype(str).isin(NON_EXCEPTION_STATUSES)
    return pd.Series([False] * len(frame), index=frame.index)


def _is_position_recon(frame: pd.DataFrame) -> pd.Series:
    if frame.empty or "security_id" not in frame.columns:
        return pd.Series(dtype=bool)
    return frame["security_id"].notna() & frame["security_id"].astype(str).ne("")


def _is_cash_recon(frame: pd.DataFrame) -> pd.Series:
    if frame.empty or "currency" not in frame.columns:
        return pd.Series(dtype=bool)
    security_blank = (
        frame["security_id"].isna() | frame["security_id"].astype(str).isin(["", "<NA>", "nan", "None"])
        if "security_id" in frame.columns
        else pd.Series([True] * len(frame), index=frame.index)
    )
    return frame["currency"].notna() & frame["currency"].astype(str).ne("") & security_blank


def _row(
    control_area: str,
    passed: bool,
    high_severity_count: int,
    open_exception_count: int,
    pass_note: str,
    fail_note: str,
    fail_action: str,
) -> dict[str, object]:
    return {
        "control_area": control_area,
        "status": "PASS" if passed else "FAIL",
        "high_severity_count": int(high_severity_count),
        "open_exception_count": int(open_exception_count),
        "ready_for_signoff": bool(passed),
        "review_note": pass_note if passed else fail_note,
        "action_required": "No action required" if passed else fail_action,
    }


def build_signoff_summary(
    breaks: pd.DataFrame,
    recon_exceptions: pd.DataFrame,
    recon_latest: dict,
) -> pd.DataFrame:
    breaks = breaks.copy() if breaks is not None else pd.DataFrame()
    recon = recon_exceptions.copy() if recon_exceptions is not None else pd.DataFrame()
    open_mask = _open_recon_mask(recon)
    high_recon_mask = (
        recon["severity"].astype(str).str.upper().eq("HIGH")
        if "severity" in recon.columns
        else pd.Series([False] * len(recon), index=recon.index)
    )

    attribution_pass = bool(
        recon_latest.get("available")
        and recon_latest.get("within_tolerance")
        and recon_latest.get("weights_ok")
        and recon_latest.get("portfolio_return_ok")
    )
    attribution = _row(
        control_area="Attribution Reconciliation",
        passed=attribution_pass,
        high_severity_count=0 if attribution_pass else 1,
        open_exception_count=0 if attribution_pass else 1,
        pass_note="Attribution reconciles within tolerance",
        fail_note="Attribution reconciliation failed or is unavailable",
        fail_action="Review attribution reconciliation output and active-return tie-out",
    )

    qa_high = _high_severity_count(breaks)
    qa = _row(
        control_area="QA Controls",
        passed=qa_high == 0,
        high_severity_count=qa_high,
        open_exception_count=qa_high,
        pass_note="No high-severity QA breaks remain open",
        fail_note="High-severity QA breaks require review",
        fail_action="Resolve or document high-severity QA breaks",
    )

    position_mask = _is_position_recon(recon)
    position_open = recon[position_mask & open_mask] if not recon.empty else pd.DataFrame()
    position_high_open = recon[position_mask & open_mask & high_recon_mask] if not recon.empty else pd.DataFrame()
    positions = _row(
        control_area="PBOR vs Custodian Positions",
        passed=position_high_open.empty,
        high_severity_count=len(position_high_open),
        open_exception_count=len(position_open),
        pass_note="No high-severity position reconciliation breaks remain open",
        fail_note="High-severity PBOR/custodian position breaks remain open",
        fail_action="Review PBOR/custodian position breaks and document resolution",
    )

    cash_mask = _is_cash_recon(recon)
    cash_open = recon[cash_mask & open_mask] if not recon.empty else pd.DataFrame()
    cash_high_open = recon[cash_mask & open_mask & high_recon_mask] if not recon.empty else pd.DataFrame()
    cash = _row(
        control_area="Cash Reconciliation",
        passed=cash_high_open.empty,
        high_severity_count=len(cash_high_open),
        open_exception_count=len(cash_open),
        pass_note="No high-severity cash reconciliation breaks remain open",
        fail_note="High-severity cash reconciliation breaks remain open",
        fail_action="Review bank/ledger cash break and document resolution",
    )

    previous_rows = [attribution, qa, positions, cash]
    final_pass = all(row["status"] == "PASS" for row in previous_rows)
    final = _row(
        control_area="Final Reporting Sign-Off",
        passed=final_pass,
        high_severity_count=sum(int(row["high_severity_count"]) for row in previous_rows),
        open_exception_count=sum(int(row["open_exception_count"]) for row in previous_rows),
        pass_note="Reporting pack is ready for review sign-off",
        fail_note="Reporting pack remains under review until failed controls are resolved",
        fail_action="Resolve failed control areas before reporting sign-off",
    )

    return pd.DataFrame([*previous_rows, final], columns=SIGNOFF_COLUMNS)
