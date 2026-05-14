from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
ASOF_DATE = "2026-01-10"
OUTPUT_FOLDER = PROJECT_ROOT / "outputs" / "2026-01"


def _run(command: list[str]) -> None:
    result = subprocess.run(
        command,
        cwd=PROJECT_ROOT,
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        if result.stdout:
            print(result.stdout.strip())
        if result.stderr:
            print(result.stderr.strip())
        raise SystemExit(result.returncode)


def main() -> None:
    _run([sys.executable, "scripts/build_recon_demo_data.py"])
    print("Recon demo data created")

    _run([sys.executable, "-m", "src.run_month_end", "--asof", ASOF_DATE])
    print("Month-end run completed")

    _run(
        [
            sys.executable,
            "-m",
            "src.run_month_end",
            "--asof",
            ASOF_DATE,
            "--recon-data-dir",
            "data/recon_demo",
        ]
    )
    print("Recon month-end run completed")

    summary_path = OUTPUT_FOLDER / "summary.json"
    if not summary_path.exists():
        raise SystemExit(f"Expected summary file not found: {summary_path}")
    summary = json.loads(summary_path.read_text(encoding="utf-8"))

    print(f"Output folder: outputs/{OUTPUT_FOLDER.name}")
    print(f"Recon exceptions file: {'found' if (OUTPUT_FOLDER / 'recon_exceptions.csv').exists() else 'missing'}")
    print(f"Signoff summary file: {'found' if (OUTPUT_FOLDER / 'signoff_summary.csv').exists() else 'missing'}")
    print(f"Report workbook: {'found' if (OUTPUT_FOLDER / 'report.xlsx').exists() else 'missing'}")
    print(f"Final Sign-Off: {'Ready' if bool(summary.get('signoff_ready')) else 'Not Ready'}")
    print(f"Failed control areas: {', '.join(summary.get('failed_control_areas', [])) or 'None'}")
    print(f"Open high severity recon exceptions: {int(summary.get('open_high_severity_recon_exceptions', 0))}")
    print(f"SLA breached recon exceptions: {int(summary.get('sla_breached_recon_exceptions', 0))}")
    print("Launch dashboard: streamlit run app/dashboard.py")


if __name__ == "__main__":
    main()
