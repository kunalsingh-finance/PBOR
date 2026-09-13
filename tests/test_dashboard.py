"""Exercise the dashboard against disposable workspaces, never the local book."""
from __future__ import annotations

import shutil
import sqlite3
from pathlib import Path

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from scripts.build_recon_demo_data import build_recon_demo_data
from src.review import load_review_history
from src.run_month_end import run_month_end


PROJECT_ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def calculated_workspace(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """Calculate the report once; each test receives its own writable copy."""
    root = tmp_path_factory.mktemp("dashboard_sample")
    shutil.copy2(PROJECT_ROOT / "policy.yaml", root / "policy.yaml")
    for folder in ("data", "sql"):
        shutil.copytree(PROJECT_ROOT / folder, root / folder)
    recon = root / "data" / "recon_demo"
    build_recon_demo_data(recon)
    run_month_end(root, "2026-01-10", recon_data_dir=recon)
    return root


@pytest.fixture
def workspace(calculated_workspace: Path, tmp_path: Path) -> Path:
    root = tmp_path / "workspace"
    shutil.copytree(calculated_workspace, root)
    return root


def app_for(root: Path) -> AppTest:
    source = (PROJECT_ROOT / "app" / "dashboard.py").read_text(encoding="utf-8")
    root_assignment = "ROOT = Path(__file__).resolve().parents[1]"
    assert source.count(root_assignment) == 1, "Keep the test workspace isolated from the repository."
    source = source.replace(
        root_assignment,
        f"ROOT = Path({root.as_posix()!r})\nsys.path.insert(0, {PROJECT_ROOT.as_posix()!r})",
        1,
    )
    return AppTest.from_string(source, default_timeout=20).run()


def widget(elements, label: str):
    return next(element for element in elements if element.label == label)


def signoff_rows(root: Path) -> pd.DataFrame:
    with sqlite3.connect(f"{(root / 'pbor_lite.db').as_uri()}?mode=ro", uri=True) as conn:
        return pd.read_sql_query("SELECT * FROM pbor_signoff_summary", conn)


def test_first_launch_offers_sample_without_creating_a_database(tmp_path: Path) -> None:
    app = app_for(tmp_path)

    assert not app.exception
    assert widget(app.button, "Open sample workspace") is not None
    assert any("synthetic data" in message.value for message in app.info)
    assert not (tmp_path / "pbor_lite.db").exists()
    assert not (tmp_path / "outputs").exists()


def test_open_sample_workspace_initializes_calculations_and_reconciliation(tmp_path: Path) -> None:
    shutil.copy2(PROJECT_ROOT / "policy.yaml", tmp_path / "policy.yaml")
    for folder in ("data", "sql"):
        shutil.copytree(PROJECT_ROOT / folder, tmp_path / folder)
    # Start with the committed portfolio inputs; the launch action must also
    # build its own reconciliation feeds instead of relying on prior local runs.
    recon_dir = tmp_path / "data" / "recon_demo"
    for name in ("internal_positions.csv", "custodian_positions.csv", "internal_cash.csv", "bank_cash.csv"):
        (recon_dir / name).unlink(missing_ok=True)
    assert not (tmp_path / "pbor_lite.db").exists()
    app = app_for(tmp_path)
    widget(app.button, "Open sample workspace").click().run(timeout=30)

    assert not app.exception
    assert not app.error
    assert (tmp_path / "pbor_lite.db").is_file()
    assert widget(app.selectbox, "Performance portfolio").value == "PF1"
    assert widget(app.metric, "Open reconciliation breaks").value == "5"
    assert widget(app.metric, "Release status").value == "On hold"
    assert {tab.label for tab in app.tabs} == {
        "Control overview", "Performance", "Attribution", "Exception workbench", "Reports & methodology",
    }
    assert (tmp_path / "outputs" / "2026-01" / "report.xlsx").is_file()
    assert (tmp_path / "outputs" / "2026-01" / "summary.json").is_file()
    assert widget(app.button, "Save review") is not None


def test_search_without_matches_hides_review_form_and_can_be_cleared(workspace: Path) -> None:
    app = app_for(workspace)
    widget(app.text_input, "Search security or account").set_value("NO_SUCH_SECURITY_[literal]").run()

    assert not app.exception
    assert any("No open breaks match these filters." in message.value for message in app.info)
    assert not [button for button in app.button if button.label == "Save review"]
    queue = next(frame.value for frame in app.dataframe if "Technical result" in frame.value.columns)
    assert queue.empty

    widget(app.text_input, "Search security or account").set_value("").run()
    assert not app.exception
    assert widget(app.button, "Save review") is not None


def test_saved_investigation_survives_rerun_and_a_new_session(workspace: Path) -> None:
    original_signoff = signoff_rows(workspace)
    app = app_for(workspace)
    selected_key = widget(app.selectbox, "Inspect exception").value
    widget(app.text_input, "Owner").set_value("Operations reviewer")
    widget(app.selectbox, "Review status").set_value("Investigating")
    note = "Compared source quantities with the settlement file; confirmation is pending."
    widget(app.text_area, "Investigation note").set_value(note)
    widget(app.button, "Save review").click().run()

    assert not app.exception
    assert any("Review saved" in message.value for message in app.success)
    app.run()
    assert not app.exception
    assert widget(app.text_input, "Owner").value == "Operations reviewer"
    assert widget(app.selectbox, "Review status").value == "Investigating"

    fresh_session = app_for(workspace)
    assert not fresh_session.exception
    assert widget(fresh_session.text_input, "Owner").value == "Operations reviewer"
    history = load_review_history(workspace / "pbor_lite.db")
    assert len(history) == 1
    assert history.iloc[0]["exception_key"] == selected_key
    assert history.iloc[0]["note"] == note
    assert history.iloc[0]["updated_at"]
    journal = next(frame.value for frame in fresh_session.dataframe if "note" in frame.value.columns)
    assert note in journal["note"].tolist()
    pd.testing.assert_frame_equal(signoff_rows(workspace), original_signoff)
    assert widget(fresh_session.metric, "Release status").value == "On hold"


@pytest.mark.parametrize(
    ("owner", "message"),
    [("", "A review owner is required."), ("Operations reviewer", "at least 10 characters")],
)
def test_incomplete_review_shows_validation_without_persisting(
    workspace: Path, owner: str, message: str,
) -> None:
    app = app_for(workspace)
    widget(app.text_input, "Owner").set_value(owner)
    widget(app.button, "Save review").click().run()

    assert not app.exception
    assert any(message in error.value for error in app.error)
    assert load_review_history(workspace / "pbor_lite.db").empty


def test_calculated_reports_are_available_for_download(workspace: Path) -> None:
    app = app_for(workspace)

    assert not app.exception
    downloads = {element.proto.label: element.proto for element in app.get("download_button")}
    for label in (
        "Download Excel report", "Download performance tear sheet", "Download full reporting pack",
        "Export performance", "Export attribution", "Export filtered queue",
    ):
        assert label in downloads
        assert downloads[label].url, f"{label} must reference generated download content."
    folder = workspace / "outputs" / "2026-01"
    assert (folder / "report.xlsx").read_bytes().startswith(b"PK")
    assert (folder / "onepager.pdf").read_bytes().startswith(b"%PDF")


def test_performance_selection_scopes_metrics_without_hiding_global_controls(workspace: Path) -> None:
    # A second book with distinct values makes cross-portfolio leakage observable.
    with sqlite3.connect(workspace / "pbor_lite.db") as conn:
        for table in ("pbor_monthly_returns", "pbor_daily_returns", "pbor_attribution_monthly"):
            second = pd.read_sql_query(f"SELECT * FROM {table}", conn)
            second["portfolio_id"] = "PF_SCOPE_CHECK"
            if table == "pbor_monthly_returns":
                second["portfolio_return_twr"] = 0.25
                second["active_return"] = 0.23
                second["benchmark_return"] = 0.02
            elif table == "pbor_daily_returns":
                second["portfolio_value_base"] = 654321.0
            second.to_sql(table, conn, if_exists="append", index=False)

    app = app_for(workspace)
    original_value = widget(app.metric, "Portfolio value").value
    original_breaks = widget(app.metric, "Open reconciliation breaks").value
    widget(app.selectbox, "Performance portfolio").set_value("PF_SCOPE_CHECK").run()

    assert not app.exception
    assert widget(app.metric, "Portfolio value").value == "654,321.00"
    assert widget(app.metric, "Portfolio value").value != original_value
    assert widget(app.metric, "Latest period · TWR").value == "25.00%"
    displayed = next(frame.value for frame in app.dataframe if "Modified Dietz" in frame.value.columns)
    assert displayed["TWR"].eq(0.25).all()
    assert widget(app.metric, "Open reconciliation breaks").value == original_breaks
    assert widget(app.metric, "Release status").value == "On hold"
