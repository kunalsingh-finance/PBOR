from __future__ import annotations

import pandas as pd

from src.signoff import build_signoff_summary


ATTRIBUTION_PASS = {
    "available": True,
    "within_tolerance": True,
    "weights_ok": True,
    "portfolio_return_ok": True,
}

ATTRIBUTION_FAIL = {
    "available": True,
    "within_tolerance": False,
    "weights_ok": True,
    "portfolio_return_ok": True,
}


def _final_status(summary: pd.DataFrame) -> str:
    return str(summary.loc[summary["control_area"] == "Final Reporting Sign-Off", "status"].iloc[0])


def _control_status(summary: pd.DataFrame, control_area: str) -> str:
    return str(summary.loc[summary["control_area"] == control_area, "status"].iloc[0])


def test_all_controls_pass_returns_final_signoff_pass() -> None:
    summary = build_signoff_summary(
        breaks=pd.DataFrame(),
        recon_exceptions=pd.DataFrame(),
        recon_latest=ATTRIBUTION_PASS,
    )

    assert _final_status(summary) == "PASS"


def test_attribution_fail_fails_final_signoff() -> None:
    summary = build_signoff_summary(
        breaks=pd.DataFrame(),
        recon_exceptions=pd.DataFrame(),
        recon_latest=ATTRIBUTION_FAIL,
    )

    assert _control_status(summary, "Attribution Reconciliation") == "FAIL"
    assert _final_status(summary) == "FAIL"


def test_high_severity_qa_break_fails_final_signoff() -> None:
    breaks = pd.DataFrame([{"severity": "HIGH", "break_type": "ATTRIBUTION_RECONCILIATION_FAIL"}])

    summary = build_signoff_summary(
        breaks=breaks,
        recon_exceptions=pd.DataFrame(),
        recon_latest=ATTRIBUTION_PASS,
    )

    assert _control_status(summary, "QA Controls") == "FAIL"
    assert _final_status(summary) == "FAIL"


def test_high_severity_position_recon_break_fails_final_signoff() -> None:
    recon = pd.DataFrame(
        [
            {
                "security_id": "SEC_SPY",
                "currency": pd.NA,
                "status": "QUANTITY_BREAK",
                "workflow_status": "OPEN",
                "severity": "HIGH",
            }
        ]
    )

    summary = build_signoff_summary(
        breaks=pd.DataFrame(),
        recon_exceptions=recon,
        recon_latest=ATTRIBUTION_PASS,
    )

    assert _control_status(summary, "PBOR vs Custodian Positions") == "FAIL"
    assert _final_status(summary) == "FAIL"


def test_high_severity_cash_recon_break_fails_final_signoff() -> None:
    recon = pd.DataFrame(
        [
            {
                "security_id": pd.NA,
                "currency": "USD",
                "status": "CASH_BREAK",
                "workflow_status": "OPEN",
                "severity": "HIGH",
            }
        ]
    )

    summary = build_signoff_summary(
        breaks=pd.DataFrame(),
        recon_exceptions=recon,
        recon_latest=ATTRIBUTION_PASS,
    )

    assert _control_status(summary, "Cash Reconciliation") == "FAIL"
    assert _final_status(summary) == "FAIL"


def test_matched_and_within_tolerance_recon_rows_pass_position_and_cash_controls() -> None:
    recon = pd.DataFrame(
        [
            {
                "security_id": "SEC_JPM",
                "currency": pd.NA,
                "status": "MATCHED",
                "workflow_status": "CLOSED",
                "severity": "INFO",
            },
            {
                "security_id": pd.NA,
                "currency": "USD",
                "status": "WITHIN_TOLERANCE",
                "workflow_status": "CLOSED",
                "severity": "LOW",
            },
        ]
    )

    summary = build_signoff_summary(
        breaks=pd.DataFrame(),
        recon_exceptions=recon,
        recon_latest=ATTRIBUTION_PASS,
    )

    assert _control_status(summary, "PBOR vs Custodian Positions") == "PASS"
    assert _control_status(summary, "Cash Reconciliation") == "PASS"
    assert _final_status(summary) == "PASS"


def test_workflow_status_fallback_uses_technical_status() -> None:
    recon = pd.DataFrame(
        [
            {
                "security_id": "SEC_SPY",
                "currency": pd.NA,
                "status": "QUANTITY_BREAK",
                "severity": "HIGH",
            }
        ]
    )

    summary = build_signoff_summary(
        breaks=pd.DataFrame(),
        recon_exceptions=recon,
        recon_latest=ATTRIBUTION_PASS,
    )

    assert _control_status(summary, "PBOR vs Custodian Positions") == "FAIL"
    assert _final_status(summary) == "FAIL"
