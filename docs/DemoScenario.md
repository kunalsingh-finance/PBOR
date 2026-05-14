# Demo Scenario

This project uses a synthetic PBOR-style simulation to compare internal portfolio records against custodian-style position records and bank-style cash records. The purpose is to demonstrate month-end reporting controls for an investment operations / portfolio analytics / middle-office workflow without using confidential client, custodian, bank, or trade data.

## Intentional Breaks

`SPY` quantity break:
Potential settlement timing issue, unmatched trade, or booking lag between internal PBOR-style records and custodian records.

`AAPL` price / market value break:
Potential stale price, pricing vendor mismatch, or valuation timing difference.

`MSFT` missing in custodian:
Potential custodian feed delay, settlement issue, or security mapping problem.

`GOOGL` missing in internal PBOR:
Potential missing PBOR booking or security setup issue.

`USD` cash break:
Potential wire, fee, dividend, settlement, or bank ledger timing issue.

## Why The Breaks Matter

The synthetic operational records are designed to show the kinds of exceptions a month-end control process must surface before a reporting pack is treated as review-ready. A reporting workflow can calculate returns and attribution correctly and still remain under review if positions or cash do not reconcile.

## Output Pack

`recon_exceptions.csv`:
Detailed reconciliation queue with technical status, workflow status, SLA bucket, owner, and action required.

`signoff_summary.csv`:
Control-area pass/fail summary across attribution, QA, PBOR-vs-custodian positions, cash reconciliation, and final reporting sign-off.

`report.xlsx`:
Review workbook containing performance, attribution, QA, auto-reconciliation, and sign-off tabs.

Dashboard Sign-Off Control Center:
Streamlit summary of failed control areas, high-severity open breaks, SLA aging, and final reporting readiness.
