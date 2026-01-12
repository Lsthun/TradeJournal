# Candlestick Chart Feature Implementation

## Overview
Added a new "Charts" tab to the Trade Journal app that displays candlestick charts with trade entry/exit annotations using cached historical price data.

## Architecture

### Database Layer: SQLite Price Caching
- **File**: `price_data.db` (created in app directory)
- **Tables**:
  - `price_data`: Stores OHLC data for each symbol
    - Columns: symbol, date, open, high, low, close, volume
    - Indexed on (symbol, date) for fast lookups
  - `price_metadata`: Tracks when data was last fetched per symbol
    - Columns: symbol, last_fetched, start_date, end_date

### New Class: PriceDataManager
Manages all price data operations:
- `__init__(db_path)`: Initialize SQLite database
- `has_data(symbol)`: Check if cached data exists
- `get_price_data(symbol, start_date, end_date)`: Retrieve cached data as DataFrame
- `fetch_and_store(symbol, start_date, end_date)`: Download from yfinance and store
- `get_metadata(symbol)`: Get fetch history for a symbol

### UI Tab Structure
Changed from single-view to tabbed interface:
- **Journal Tab**: Original journal, table, and equity curve
- **Charts Tab**: New candlestick chart view with controls

### Charts Tab Components
1. **Symbol Selector**: Dropdown of symbols from loaded trades
2. **Download Button**: One-click data fetching for selected symbol
3. **Status Indicator**: Shows data availability and fetch status
4. **Chart Display Area**: Embedded mplfinance candlestick chart

## Data Flow

### Initial Load
1. User loads CSV into Journal tab
2. `update_chart_symbols()` populates dropdown with unique symbols from trades
3. User switches to Charts tab

### Data Fetching (Lazy Load)
1. User selects symbol from dropdown
2. System checks if data exists in SQLite via `has_data()`
3. If no data → displays "No data" message
4. User clicks "Download Data" button
5. System calculates date range:
   - **Start**: 90 days before first trade entry date
   - **End**: Current date OR 90 days after last exit date (whichever is earlier)
6. Calls `fetch_and_store()` to download from yfinance
7. Data stored in SQLite, displayed in chart

### Subsequent Loads
1. User selects same symbol again
2. System detects cached data exists
3. Displays chart immediately without re-fetching
4. User can manually refresh via "Download Data" button (overwrites cache)

## Chart Features

### Candlestick Display
- Open, High, Low, Close prices via mplfinance
- Volume bar (optional)
- Standard Yahoo Finance styling

### Trade Annotations
- **Green up-arrows** (^): Buy signals at low price point
- **Red down-arrows** (v): Sell signals at high price point
- Positioned at actual trade dates within candlestick range

## Deployment Considerations

### New Dependencies
```
yfinance>=0.1.70        # Price data fetching
mplfinance>=0.12.0      # Candlestick charting
```

Install with: `pip install -r requirements.txt`

### Database File
- Created automatically on first chart download
- Location: Same directory as app (`price_data.db`)
- Included in distribution as empty schema OR user creates on first use
- No special database setup needed

### Distribution Checklist
- ✅ Single database file (easy to bundle)
- ✅ No external services required (after initial yfinance download)
- ✅ Graceful degradation if yfinance unavailable
- ✅ One-time data fetch per symbol
- ✅ Clear UI for data refresh option

## Error Handling

### Missing yfinance
- Shows error dialog if user clicks download without yfinance installed
- Provides installation command

### Missing mplfinance
- Shows error dialog if chart attempted without mplfinance
- Provides installation command

### Network Errors
- Caught during fetch_and_store()
- User-friendly error message in status bar
- No partial data stored on failure

### No Trades for Symbol
- Validates trades exist before allowing download
- Shows warning if no trades found

## Code Integration Points

### TradeJournalApp Changes
1. Added `self.price_manager` instance in `__init__`
2. Added `self.db_path` for SQLite location
3. Split `_build_ui()` into:
   - Original `_build_ui()`: Creates notebook with tabs
   - New `_build_journal_tab()`: Journal tab content
   - New `_build_chart_tab()`: Charts tab content
4. Added chart-related state variables:
   - `self.current_chart_symbol`
   - `self.chart_canvas`
5. Updated trade modification methods to call `update_chart_symbols()`

### New Methods
- `_build_chart_tab(parent_frame)`: UI setup
- `on_chart_symbol_selected(event)`: Symbol dropdown handler
- `on_download_price_data()`: Download button handler
- `display_candlestick_chart(symbol)`: Render candlestick with annotations
- `update_chart_symbols()`: Sync dropdown with model trades

## Testing Recommendations

1. **Data Persistence**: Verify SQLite data persists between app sessions
2. **Symbol Updates**: Add/delete trades and confirm dropdown updates
3. **Date Range**: Test with open positions (no exit date)
4. **Network**: Test with/without internet connection
5. **Performance**: Large date ranges (>1 year) should remain responsive
6. **Deployment**: Test on clean machine without yfinance/mplfinance pre-installed

## Future Enhancements

- Price overlay (plot entry/exit prices as horizontal lines)
- Multi-symbol comparison
- Custom date range selector
- Performance overlay (cumulative P&L on chart)
- Export chart as image
- Technical indicators (moving averages, Bollinger bands, etc.)
