# PBOR | Portfolio Reconciliation & Performance Attribution

[![CI](https://github.com/kunalsingh-finance/PBOR/actions/workflows/ci.yml/badge.svg)](https://github.com/kunalsingh-finance/PBOR/actions/workflows/ci.yml)

A portfolio operations workspace for explaining investment performance, reconciling positions and cash, and reviewing reporting controls in one place.

PBOR rebuilds positions from transactions, calculates time-weighted and Modified Dietz returns, and decomposes active return using Brinson-Fachler sector attribution. Position and cash comparisons surface exceptions with severity, ownership, aging, and suggested review actions. A control summary shows which issues prevent reporting sign-off.

The exception workbench records owners, investigation status, and evidence in a persistent review journal; financial breaks clear only after source records are corrected and controls are rerun.

## Run the workspace

Use Python 3.11 and run these commands from the repository root:

```bash
python -m pip install -r requirements.txt
python -m streamlit run app/dashboard.py
```

On first launch, initialize the sample workspace from the dashboard. The bundled scenario runs locally without market-data credentials or external data calls. Open the local URL printed by Streamlit to use the application.

The sample uses synthetic portfolio, custodian, and bank records with intentional position and cash differences. An unresolved control or blocked sign-off is an expected result of that scenario. It represents an issue to review in the data.

## Operating flow

| Stage | What the workspace shows |
| --- | --- |
| Performance | Portfolio and benchmark returns, active return, and daily performance history. |
| Attribution | Sector allocation, selection, and interaction effects, with reconciliation to arithmetic active return. |
| Reconciliation | Quantity, price, market value, missing-record, and cash exceptions with technical status and review priority. |
| Controls and reporting | Data-quality findings, exception aging, control-area status, and reporting readiness, backed by Excel, CSV, and PDF exports. |

Reconciliation tolerances and return assumptions are configured in [policy.yaml](policy.yaml). Matched records and differences within tolerance are closed; actual breaks remain open. Reporting readiness depends on attribution, QA, position, and cash controls passing.

## Data and methodology

The default dataset is a fixed sample, not a live portfolio. Optional public-market-data inputs can be built separately; their availability depends on the upstream providers. Operational records remain synthetic.

- [Methodology](METHODOLOGY.md): return calculations, attribution, tolerances, and sign-off rules.
- [Formula reference](docs/Methodology.md): concise calculation notes.
- [Workflow walkthrough](docs/HowItWorks.md): inputs, processing stages, and outputs.
- [Sample scenario](docs/DemoScenario.md): the intentional reconciliation breaks.
- [Development guide](docs/DEVELOPMENT.md): command-line runs, generated files, tests, and CI.

## Scope

PBOR is a local analytical application with simplified portfolio coverage, benchmark construction, and control rules. It does not provide production accounting infrastructure or live custodian and bank integrations. Exception ownership, aging, and suggested causes are derived from the sample records and rules; they are aids to review, not evidence of a completed investigation.

Built with Python, pandas, NumPy, SQLite, Streamlit, matplotlib, and openpyxl.
