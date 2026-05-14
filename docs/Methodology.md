# Methodology

## Return conventions

- Base currency: USD
- Reporting window: month-to-date as-of date
- Cash-flow timing: end-of-day
- External cash flows: `CONTRIB`, `WITHDRAW`
- Internal income: `DIV`, `INT`

## Monthly TWR

Daily return:

`r_t = (MV_t - MV_(t-1) - CF_t_external) / MV_(t-1)`

Monthly TWR:

`R_month = product(1 + r_t) - 1`

## Modified Dietz

For a period `[t0, tN]`:

`R_dietz = (V_N - V_0 - sum(C_i)) / (V_0 + sum(w_i * C_i))`

Where `w_i` is time-weighted by days remaining in period under end-of-day cash-flow convention.

## Brinson-Fachler attribution

At sector level:

- Allocation: `(w_p - w_b) * (r_b - R_b)`
- Selection: `w_b * (r_p - r_b)`
- Interaction: `(w_p - w_b) * (r_p - r_b)`

The active effect is:

`allocation + selection + interaction`

## Attribution reconciliation control

For each month, attribution is reconciled to active return:

`diff_bps = abs(sum(active_effect_sector) - active_return) * 10,000`

Review rule:

- Pass: `diff_bps < 5`
- Fail: attribution visuals are withheld until reconciliation is within tolerance

Reconciliation basis:

- Attribution is reconciled to arithmetic active return for the same start/end window.

## PBOR-vs-custodian reconciliation

The auto-reconciliation workflow compares synthetic internal PBOR-style records against synthetic custodian and bank-style records. It is included to demonstrate investment operations and middle-office controls without exposing confidential client, custodian, bank, or broker data.

Position matching key:

`asof_date + portfolio_id + account_id + security_id`

Cash matching key:

`asof_date + portfolio_id + account_id + currency`

Break rules:

- missing external position: `MISSING_IN_CUSTODIAN`
- missing internal position: `MISSING_IN_INTERNAL`
- quantity outside tolerance: `QUANTITY_BREAK`
- price percentage outside tolerance: `PRICE_BREAK`
- market value outside tolerance: `MARKET_VALUE_BREAK`
- cash outside tolerance: `CASH_BREAK`
- missing bank cash: `MISSING_IN_BANK`
- missing internal cash: `MISSING_IN_INTERNAL_CASH`

Sign-off rule:

- Ready only when attribution reconciles, no high-severity QA breaks remain, and no high-severity auto-reconciliation exceptions remain.
