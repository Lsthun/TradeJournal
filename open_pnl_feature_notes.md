# Open P&L + Open-Only Changes (Work-In-Progress)

## Scope
- Add open-position visibility across summary/equity: toggles, data fetch, and overlay.
- Add open-only filtering and explicit closed vs closed+open totals.
- Support mass refresh of current prices for open positions.

## UI Additions
- Top bar:
  - "Open positions only" checkbox: filters journal table; mutually exclusive with closed-only.
  - "Include Open P&L" checkbox: drives summary text and equity overlay.
  - "Refresh Open Prices" button: mass-fetches price data for filtered open positions (uses yfinance if present).
- Charts tab:
  - Retains its own "Include Open P&L" and "Refresh Open Prices" controls, now wired to shared logic.

## Logic Changes
- Summary:
  - Computes closed metrics as before; appends open P&L (current) when enabled.
  - Shows both closed P&L (filtered) and closed+open totals for clarity.
- Equity curve:
  - Closed P&L line remains the base series.
  - Optional orange marker/line shows closed + current open P&L when "Include Open P&L" is on.
- Open P&L calculation:
  - Iterates filtered open trades; fetches latest close via PriceDataManager (yfinance if missing).
  - Returns (pnl_value, missing_symbols) to surface gaps; missing symbols reported in summary/status.
- Filtering tweaks:
  - Open-only table view excludes closed trades.
  - Closed-only no longer drops partial exits (so partials count toward closed metrics).

## Known Gaps / Decisions
- Open equity overlay uses latest close only; no intraday pricing.
- If price data is absent and yfinance is missing/offline, open P&L shows N/A with missing symbols list.
- Equity curve still plots closed trades by exit date; open-only mode for the chart itself is not implemented.
- No persistence change; journal_state.pkl unaffected by open P&L flags (state not saved).

## Revert Plan
- Discard changes to trade_journal_app.py and journal_state.pkl to return to prior behavior.
- Keep this file (open_pnl_feature_notes.md) as the spec for a future revisit.
