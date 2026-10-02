from __future__ import annotations

from pathlib import Path
import shlex
import shutil
import subprocess
import tempfile
import unittest

import yaml


ROOT = Path(__file__).resolve().parents[1]


@unittest.skipUnless(shutil.which("git"), "Git is required for workflow staging checks")
class MonthlyWorkflowTests(unittest.TestCase):
    def test_successful_month_stages_ignored_outputs_only(self) -> None:
        workflow = yaml.safe_load(
            (ROOT / ".github/workflows/monthly_run.yml").read_text(encoding="utf-8")
        )
        commit_step = next(
            step for step in workflow["jobs"]["monthly-run"]["steps"]
            if step["name"] == "Commit monthly outputs"
        )
        stage_commands = [
            line.strip() for line in commit_step["run"].splitlines()
            if line.strip().startswith("git add ")
        ]
        self.assertEqual(len(stage_commands), 1)

        with tempfile.TemporaryDirectory() as directory:
            repo = Path(directory)
            subprocess.run(["git", "init", "-q", str(repo)], check=True)
            (repo / ".gitignore").write_text(
                (ROOT / ".gitignore").read_text(encoding="utf-8"), encoding="utf-8"
            )
            month = "2026-09"
            for relative in (
                f"outputs/{month}/summary.json",
                f"outputs/{month}/nested/daily_returns.csv",
                "outputs/2026-08/summary.json",
                "data_real/raw.csv",
                "pipeline.log",
                "pbor_lite.db",
            ):
                target = repo / relative
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_text("synthetic fixture\n", encoding="utf-8")

            ignored = subprocess.run(
                ["git", "check-ignore", f"outputs/{month}/summary.json"],
                cwd=repo, capture_output=True, text=True,
            )
            self.assertEqual(ignored.returncode, 0, ignored.stderr)
            command = shlex.split(stage_commands[0].replace("$MONTH_TAG", month))
            subprocess.run(command, cwd=repo, check=True, capture_output=True, text=True)
            staged = subprocess.run(
                ["git", "diff", "--cached", "--name-only"], cwd=repo,
                check=True, capture_output=True, text=True,
            ).stdout.splitlines()
            self.assertEqual(
                set(staged),
                {f"outputs/{month}/summary.json", f"outputs/{month}/nested/daily_returns.csv"},
            )


if __name__ == "__main__":
    unittest.main()
