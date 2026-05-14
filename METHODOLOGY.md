# Methodology

## Return Methodology

Time-Weighted Return (TWR) is the primary portfolio return in Portfolio Reconciliation & Reporting Control Engine. That is the standard measure for manager evaluation because it removes the impact of external cash flows and isolates investment performance.

Daily return is calculated from beginning market value, ending market value, and external flow for the day. Monthly TWR is then produced by chain-linking the daily series:

`(1 + r_1) x (1 + r_2) x ... x (1 + r_n) - 1`

Modified Dietz is retained as a secondary measure and control. It is most useful when cash flow is large relative to portfolio size. In Portfolio Reconciliation & Reporting Control Engine, that review point is tied to the policy threshold of more than 10% of net asset value. Modified Dietz provides a practical money-weighted approximation without requiring full intraday valuations.

Arithmetic return is stored separately because attribution reconciles on an arithmetic basis. Brinson-Fachler effects sum arithmetically, so arithmetic portfolio and benchmark returns provide the correct active-return reference for reconciliation.

## Attribution Methodology

Portfolio Reconciliation & Reporting Control Engine uses Brinson-Fachler sector attribution. For each sector:

- Allocation = `(w_p - w_b) x (r_b - R_b)`
- Selection = `w_b x (r_p - r_b)`
- Interaction = `(w_p - w_b) x (r_p - r_b)`

where:

- `w_p` = portfolio beginning weight
- `w_b` = benchmark beginning weight
- `r_p` = portfolio sector return
- `r_b` = benchmark sector return
- `R_b` = total benchmark return

Brinson-Fachler is used instead of the original Brinson-Hood-Beebower formulation because allocation is measured relative to total benchmark return rather than raw sector return alone. That makes the allocation result more useful in benchmark-relative reporting.

Cash is treated as its own sector. Its return is not inferred from incidental balances. The system applies the policy cash-return source, typically SOFR, so the cash segment reflects a documented assumption rather than a residual return.

Internal buy and sell flows are excluded from sector return construction. This avoids mixing transfer effects with sector performance. Sector return therefore reflects valuation change and income, not internal capital movement.

## Reconciliation Gate

Portfolio Reconciliation & Reporting Control Engine enforces a 5 basis-point reconciliation tolerance between reported active return and summed attribution effect. A difference below 5 bps is treated as within tolerance. A difference at or above 5 bps is a failed control.

When the gate fails, attribution output is withheld and the exception is logged as a QA break. In this reporting workflow simulation, attribution that does not reconcile to reported active return should remain under review rather than be presented as final.

The same control framework also checks weight integrity and sector-to-portfolio return consistency.

## Data & Controls

Prices in the real-data flow are sourced from yfinance adjusted close history. Cash return is sourced from SOFR through FRED when available. If a live SOFR refresh is not available, the configured policy rate is retained.

The real-data builder forward-fills price series for up to three consecutive calendar days. This supports weekends and short market closures without carrying stale prices indefinitely.

QA breaks are classified by type and severity. Current break categories include:

- missing prices
- missing FX rates
- duplicate prices
- unknown security identifiers
- return outliers
- NAV jumps with zero flow
- holdings mismatches
- attribution reconciliation failures

Severity levels are used as follows:

- `HIGH` = results should be treated as under review until resolved
- `MEDIUM` = material exception that requires review
- `LOW` = informational or non-blocking exception

## PBOR vs Custodian Reconciliation Methodology

Portfolio reports are only reliable when positions, cash, prices, and attribution controls tie out. The reconciliation workflow simulates how middle-office or investment operations teams identify breaks before month-end reporting sign-off.

The purpose of the PBOR-vs-custodian reconciliation is to compare internal PBOR-style records with external custodian and bank-style records before reporting is treated as review-ready. This project uses synthetic operational records only; real PBOR, custodian, bank, and broker files are confidential.

Position records are matched on:

- `asof_date`
- `portfolio_id`
- `account_id`
- `security_id`

Cash records are matched on:

- `asof_date`
- `portfolio_id`
- `account_id`
- `currency`

The reconciliation applies policy-driven tolerances for quantity, price percentage, market value, and cash balance differences. Exact matches receive `MATCHED`; non-zero differences inside tolerance receive `WITHIN_TOLERANCE`.

Break classification is ordered so the most operationally important issue is surfaced first:

- internal-only position = `MISSING_IN_CUSTODIAN`
- external-only position = `MISSING_IN_INTERNAL`
- quantity outside tolerance = `QUANTITY_BREAK`
- price percentage outside tolerance = `PRICE_BREAK`
- market value outside tolerance = `MARKET_VALUE_BREAK`
- internal-only cash = `MISSING_IN_BANK`
- bank-only cash = `MISSING_IN_INTERNAL_CASH`
- cash difference outside tolerance = `CASH_BREAK`

Severity logic is designed for review triage:

- `INFO` = matched record
- `LOW` = within configured tolerance
- `MEDIUM` = price break or moderate market value break
- `HIGH` = quantity break, cash break, missing record, or market value break above 10,000 base-currency units

Root cause and resolution notes are mapped from the break type. Quantity breaks point to settlement timing, booking, or unmatched transaction review. Price breaks point to pricing-source or stale-price review. Cash breaks point to ledger, bank activity, fees, dividends, wires, and interest timing. Missing-record breaks point to feed completeness, PBOR booking, and security setup.

Sign-off readiness requires attribution reconciliation to pass, no high-severity QA breaks, and no high-severity auto-reconciliation exceptions. If any of those controls fails, the reporting pack should remain under review.

## Exception Lifecycle and Sign-Off Logic

The reconciliation workflow separates technical status from workflow status. Technical status describes the result of the comparison, such as `MATCHED`, `WITHIN_TOLERANCE`, `QUANTITY_BREAK`, `PRICE_BREAK`, `MARKET_VALUE_BREAK`, missing-record breaks, or `CASH_BREAK`. Workflow status describes whether that item still requires operational review.

Lifecycle rules:

- `MATCHED` and `WITHIN_TOLERANCE` records are `CLOSED`
- all true breaks are `OPEN`

SLA buckets are derived from `age_days`:

- `CURRENT` = open break aged 0-1 days
- `WATCHLIST` = open break aged 2-3 days
- `BREACHED` = open break aged 4 or more days
- `N/A` = matched or within-tolerance record

The `action_required` field translates the break type into a practical review step. Quantity breaks point to trade blotter, settlement, and custodian booking review. Price breaks point to pricing source and stale price checks. Cash breaks point to ledger, bank activity, fees, dividends, wires, and interest review.

The sign-off summary evaluates five control areas:

- Attribution Reconciliation
- QA Controls
- PBOR vs Custodian Positions
- Cash Reconciliation
- Final Reporting Sign-Off

Final reporting sign-off requires all four underlying control areas to pass:

- Attribution Reconciliation `PASS`
- QA Controls `PASS`
- PBOR vs Custodian Positions `PASS`
- Cash Reconciliation `PASS`

If any control area fails, the reporting pack remains under review until the failed controls are resolved or documented.
