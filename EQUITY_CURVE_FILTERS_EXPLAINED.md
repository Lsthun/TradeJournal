# Equity Curve Filter Combinations Explained

This document explains how the **Equity Curve** is calculated under different filter combinations in the Trade Journal app.

---

## Key Concepts

- **Closed P&L**: Realized profit/loss from trades that have been fully exited (bought AND sold)
- **Open P&L**: Unrealized profit/loss from positions still held, calculated as `(current_price - entry_price) × quantity`
- **Equity Curve**: A cumulative chart showing how your account value changes over time based on trade exits

---

## Filter Combinations

### 1. No Filters (Default View)
- **Table Shows**: All trades (both open and closed)
- **Equity Curve**: Cumulative sum of **Closed P&L only**
  - Each point on the curve = running total of realized P&L from closed trades up to that exit date
  - Open positions contribute $0 to the curve (no realized gains yet)

### 2. ☑️ Closed Positions Only
- **Table Shows**: Only trades with an exit date (fully closed)
- **Equity Curve**: Same as default — cumulative **Closed P&L**
  - Filtering the table doesn't change the equity calculation
  - The curve still shows all closed trade P&L

### 3. ☑️ Open Positions Only
- **Table Shows**: Only trades without an exit date (still holding)
- **Equity Curve**: **$0 flat line** (or no data)
  - Open positions have no realized P&L
  - There are no exit dates to plot on the time axis
  - This view is useful for reviewing current holdings, not equity performance

### 4. ☑️ Include Open P&L (with any of the above)
- **Table Shows**: Based on other filter settings
- **Equity Curve**: Closed P&L curve **PLUS** an orange marker showing current total
  
  The orange dot and dashed line represent:
  ```
  Closed + Open = (Total Closed P&L) + (Current Unrealized P&L)
  ```

  **How Open P&L is calculated:**
  1. For each open position, fetch the latest closing price (via yfinance or cache)
  2. Calculate: `(current_price - entry_price) × quantity`
  3. Sum all open position P&L values
  4. Add to the final closed P&L value

  **The tooltip shows:**
  - **Closed + Open**: Your total equity if you closed everything today
  - **Closed P&L**: What you've actually realized
  - **Open P&L**: Unrealized gains/losses on current holdings

---

## Visual Summary

| Filter State | Table Content | Equity Curve Shows |
|-------------|---------------|-------------------|
| Default (no filters) | All trades | Closed P&L cumulative |
| Closed only | Closed trades | Closed P&L cumulative |
| Open only | Open trades | $0 / No data |
| + Include Open P&L | (same as above) | Closed P&L + Orange marker with Closed+Open total |

---

## Important Notes

1. **The blue line always shows realized (closed) P&L** — it doesn't change based on filters
2. **The orange marker** only appears when "Include Open P&L" is checked
3. **Refresh Open Prices** button fetches latest prices from Yahoo Finance to update Open P&L calculations
4. **Missing prices** will show a warning — some symbols may not have data available
5. **Account filter** affects which trades are included in both table and equity calculations

---

## Example Scenario

You have:
- 5 closed trades with total P&L of +$10,000
- 2 open positions currently showing +$2,500 unrealized gain

| View | Equity Curve End Value |
|------|----------------------|
| Default | $10,000 (blue line) |
| With "Include Open P&L" | $10,000 (blue) + $12,500 (orange marker) |

The orange marker shows what your total would be IF you closed all positions at current prices.
