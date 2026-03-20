# Methodology

## Return Methodology

Time-Weighted Return (TWR) is the primary portfolio return in PBOR-Lite. That is the standard measure for manager evaluation because it removes the impact of external cash flows and isolates investment performance.

Daily return is calculated from beginning market value, ending market value, and external flow for the day. Monthly TWR is then produced by chain-linking the daily series:

`(1 + r_1) x (1 + r_2) x ... x (1 + r_n) - 1`

Modified Dietz is retained as a secondary measure and control. It is most useful when cash flow is large relative to portfolio size. In PBOR-Lite, that review point is tied to the policy threshold of more than 10% of net asset value. Modified Dietz provides a practical money-weighted approximation without requiring full intraday valuations.

Arithmetic return is stored separately because attribution reconciles on an arithmetic basis. Brinson-Fachler effects sum arithmetically, so arithmetic portfolio and benchmark returns provide the correct active-return reference for reconciliation.

## Attribution Methodology

PBOR-Lite uses Brinson-Fachler sector attribution. For each sector:

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

PBOR-Lite enforces a 5 basis-point reconciliation tolerance between reported active return and summed attribution effect. A difference below 5 bps is treated as within tolerance. A difference at or above 5 bps is a failed control.

When the gate fails, attribution output is withheld and the exception is logged as a QA break. In a client-reporting context, attribution that does not reconcile to reported active return should not be published.

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

- `HIGH` = publication should stop until resolved
- `MEDIUM` = material exception that requires review
- `LOW` = informational or non-blocking exception
