# Methodology

## Return Methodology

PBOR-Lite treats Time-Weighted Return (TWR) as the primary performance measure because it isolates the effect of investment decisions from the timing and size of external cash flows. That makes TWR the appropriate standard for manager evaluation and benchmark-relative reporting.

Daily portfolio return is calculated from beginning market value, ending market value, and external cash flow for the day. Monthly TWR is then produced by chain-linking the daily series:

`(1 + r_1) x (1 + r_2) x ... x (1 + r_n) - 1`

Modified Dietz is retained alongside TWR as a control and fallback methodology when cash flows are large relative to portfolio net asset value. In PBOR-Lite, large cash flow review is tied to the policy threshold of greater than 10% of NAV. Modified Dietz is useful in those cases because it approximates money-weighted performance while preserving a deterministic month-end calculation without requiring full intra-day valuations.

Arithmetic return is stored separately because it is useful for reconciliation against attribution. Brinson-Fachler attribution effects sum arithmetically, not geometrically, so arithmetic portfolio and benchmark returns provide the appropriate active-return reference for validating attribution totals against reported performance.

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

Brinson-Fachler is preferred over the original Brinson-Hood-Beebower formulation because it measures allocation relative to the benchmark's total return rather than raw sector return alone. That makes the allocation effect more interpretable in benchmark-relative reporting and more consistent with modern institutional performance practice.

Cash is treated as an explicit sector. Its return is not inferred from incidental cash balances. Instead, PBOR-Lite applies the configured policy cash-return source, typically SOFR, so the cash segment reflects a documented investment policy assumption rather than an arbitrary residual return.

Internal buy and sell flows are excluded from sector return construction. This prevents trade movement between sectors from contaminating sector performance with transfer effects. Sector returns therefore reflect valuation change and income, not internal capital reallocation.

## Reconciliation Gate

PBOR-Lite enforces a 5 basis-point attribution reconciliation tolerance. The attribution-active reconciliation compares the summed Brinson-Fachler active effect to the arithmetic active return used for reporting. A difference below 5 bps is treated as within tolerance. A difference at or above 5 bps is treated as a failed control.

When the reconciliation gate fails, attribution output is withheld from publication and the exception is logged as a QA break. This is deliberate. In a client-reporting context, publishing performance attribution that does not reconcile to reported active return is a control failure, not a cosmetic variance.

The same control framework also checks weight integrity and sector-to-portfolio return consistency so that attribution is supported by a coherent underlying return base.

## Data & Controls

Market prices in the Level 2 real-data flow are sourced from yfinance adjusted close history. Cash return is sourced from SOFR using FRED when available, with the configured policy rate retained when a live refresh is not available.

The real-data builder forward-fills price series for up to three consecutive calendar days. This supports weekends and short market closures while preventing open-ended carry-forward of stale prices.

QA breaks are classified by type and severity. Current break categories include, among others:

- missing prices
- missing FX rates
- duplicate prices
- unknown security identifiers
- return outliers
- NAV jumps with zero flow
- holdings mismatches
- attribution reconciliation failures

Severity is used to distinguish blocking exceptions from advisory review items:

- `HIGH` indicates a control break that should block publication until resolved
- `MEDIUM` indicates a material anomaly that requires investigation
- `LOW` is reserved for non-blocking informational exceptions
