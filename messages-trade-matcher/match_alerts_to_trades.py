#!/usr/bin/env python3
"""
Match stock screener alerts to trade journal entries.

Reads alerts from alerts.csv and trades from journal_state.pkl,
matching by symbol and entry date. Exports matches to alerts_matched.csv.
"""

import pandas as pd
import pickle
import os
import sys
from datetime import datetime, date
from pathlib import Path

# Add parent directory to path to import trade_journal_app
sys.path.insert(0, str(Path(__file__).parent.parent))
from trade_journal_app import TradeJournalModel, Transaction, TradeEntry

# ============================================================================
# Configuration: Specify the alerts CSV file to match against trade journal
# Format: "filename.csv" or path to file (e.g., "alerts_2026-01-12_17.07.csv")
# ============================================================================
ALERTS_CSV_FILE = "alerts.csv"  # Change this to the alerts file you want to use
# ============================================================================


def load_alerts(filepath: str) -> pd.DataFrame:
    """Load alerts from CSV file."""
    if not os.path.exists(filepath):
        print(f"Error: {filepath} not found", file=sys.stderr)
        sys.exit(1)
    
    try:
        df = pd.read_csv(filepath)
        print(f"Loaded {len(df)} alerts from {filepath}", file=sys.stderr)
        return df
    except Exception as e:
        print(f"Error reading {filepath}: {e}", file=sys.stderr)
        sys.exit(1)


def load_trades(filepath: str) -> list:
    """Load trade entries from journal state pickle file."""
    if not os.path.exists(filepath):
        print(f"Error: {filepath} not found", file=sys.stderr)
        sys.exit(1)
    
    try:
        with open(filepath, 'rb') as f:
            data = pickle.load(f)
        
        # Extract transactions and rebuild trades using TradeJournalModel
        if isinstance(data, dict):
            transactions = data.get('transactions', [])
            
            # Rebuild the model to compute trades from transactions
            model = TradeJournalModel()
            model.transactions = transactions
            model._match_trades()
            trades = model.trades
        else:
            trades = []
        
        print(f"Loaded {len(trades)} trades from {len(transactions)} transactions", file=sys.stderr)
        return trades
    except Exception as e:
        print(f"Error reading {filepath}: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


def parse_timestamp(timestamp_str) -> date:
    """Parse ISO 8601 timestamp string to date object."""
    if not timestamp_str or pd.isna(timestamp_str):
        return None
    
    try:
        # Handle ISO format with timezone info
        dt = datetime.fromisoformat(timestamp_str.replace('Z', '+00:00'))
        return dt.date()
    except Exception:
        return None


def convert_date_to_datetime(d: date) -> datetime:
    """Convert date object to datetime at midnight."""
    if d is None:
        return None
    if isinstance(d, datetime):
        return d
    return datetime.combine(d, datetime.min.time())


def get_trade_date(trade) -> date:
    """Extract entry date from trade object."""
    if hasattr(trade, 'entry_date'):
        entry_date = trade.entry_date
        if isinstance(entry_date, date):
            return entry_date
        elif isinstance(entry_date, str):
            return parse_timestamp(entry_date)
    return None


def get_trade_symbol(trade) -> str:
    """Extract symbol from trade object."""
    if hasattr(trade, 'symbol'):
        return trade.symbol.upper() if trade.symbol else None
    return None


def main():
    """Main function to match alerts to trades."""
    
    # Paths
    alerts_csv = ALERTS_CSV_FILE
    journal_state = os.path.expanduser("~/Documents/journal_state.pkl")
    output_csv = "alerts_matched.csv"
    
    # Load data
    alerts_df = load_alerts(alerts_csv)
    trades = load_trades(journal_state)
    
    if alerts_df.empty:
        print("No alerts to process.", file=sys.stderr)
        return
    
    if not trades:
        print("No trades found in journal.", file=sys.stderr)
        return
    
    # Filter alerts to May 1, 2025 onwards
    cutoff_date = date(2025, 5, 1)
    alerts_df['alert_date_parsed'] = alerts_df['date'].apply(parse_timestamp)
    alerts_df = alerts_df[alerts_df['alert_date_parsed'] >= cutoff_date]
    
    print(f"Filtered to {len(alerts_df)} alerts from May 1, 2025 onwards", file=sys.stderr)
    
    if alerts_df.empty:
        print("No alerts after filtering by date.", file=sys.stderr)
        return
    
    # Get set of symbols in the trade journal
    trade_symbols = set()
    for trade in trades:
        symbol = get_trade_symbol(trade)
        if symbol:
            trade_symbols.add(symbol)
    
    print(f"Trade journal contains {len(trade_symbols)} unique symbols", file=sys.stderr)
    print(f"Sample trade symbols: {sorted(list(trade_symbols))[:10]}", file=sys.stderr)
    
    # Filter alerts to only those with symbols in the trade journal
    alerts_df['symbol_upper'] = alerts_df['symbol'].str.upper()
    alerts_before_filter = len(alerts_df)
    alerts_df = alerts_df[alerts_df['symbol_upper'].isin(trade_symbols)]
    
    print(f"Filtered alerts to {len(alerts_df)} matching trade journal symbols (from {alerts_before_filter})", file=sys.stderr)
    
    if alerts_df.empty:
        print("No alerts match symbols in the trade journal.", file=sys.stderr)
        return
    
    # Build a dict of trades for fast lookup: {(symbol, date): trade}
    trades_by_symbol_date = {}
    for trade in trades:
        symbol = get_trade_symbol(trade)
        trade_date = get_trade_date(trade)
        
        if symbol and trade_date:
            key = (symbol, trade_date)
            # Store list of trades (in case multiple trades on same symbol/date)
            if key not in trades_by_symbol_date:
                trades_by_symbol_date[key] = []
            trades_by_symbol_date[key].append(trade)
    
    print(f"Built lookup index with {len(trades_by_symbol_date)} symbol-date combinations", file=sys.stderr)
    
    # Debug: print first few trades and their date ranges
    if trades_by_symbol_date:
        all_trade_dates = set()
        for (sym, trade_date) in trades_by_symbol_date.keys():
            all_trade_dates.add(trade_date)
        
        sorted_trade_dates = sorted(list(all_trade_dates))
        print(f"Trade date range: {sorted_trade_dates[0]} to {sorted_trade_dates[-1]}", file=sys.stderr)
        print(f"Total unique trade dates: {len(sorted_trade_dates)}", file=sys.stderr)
        
        sample_keys = list(trades_by_symbol_date.keys())[:5]
        print(f"Sample trade keys: {sample_keys}", file=sys.stderr)
    
    # Debug: check first few alerts
    if not alerts_df.empty:
        for i in range(min(3, len(alerts_df))):
            row = alerts_df.iloc[i]
            symbol = row['symbol_upper']
            alert_date = row['alert_date_parsed']
            print(f"Sample alert {i}: symbol={symbol}, date={alert_date}", file=sys.stderr)
    
    # Match alerts to trades
    matches = []
    unmatched = []
    
    for idx, row in alerts_df.iterrows():
        symbol = row['symbol_upper']
        alert_date = row['alert_date_parsed']
        strategy = row['strategy'] if pd.notna(row['strategy']) else None
        
        if not symbol or alert_date is None:
            continue
        
        # Convert alert date to datetime for comparison with trade dates
        alert_datetime = convert_date_to_datetime(alert_date)
        
        # Look for matching trades - try exact match first, then nearby dates
        matching_trades = []
        match_type = "SAME_DAY_ENTRY"
        
        # Try exact date match (alert date = trade entry date)
        key = (symbol, alert_datetime)
        if key in trades_by_symbol_date:
            matching_trades = trades_by_symbol_date[key]
        
        # If no exact match, search for trades within 5 days
        if not matching_trades:
            for offset in range(1, 6):
                # Try dates before the alert
                nearby_datetime = alert_datetime - pd.Timedelta(days=offset)
                key = (symbol, nearby_datetime)
                if key in trades_by_symbol_date:
                    matching_trades = trades_by_symbol_date[key]
                    match_type = f"TRADE_{offset}D_BEFORE_ALERT"
                    break
                
                # Try dates after the alert
                nearby_datetime = alert_datetime + pd.Timedelta(days=offset)
                key = (symbol, nearby_datetime)
                if key in trades_by_symbol_date:
                    matching_trades = trades_by_symbol_date[key]
                    match_type = f"TRADE_{offset}D_AFTER_ALERT"
                    break
        
        if matching_trades:
            # Create one row per matching trade
            for trade in matching_trades:
                entry_price = trade.entry_price if hasattr(trade, 'entry_price') else None
                quantity = trade.quantity if hasattr(trade, 'quantity') else None
                trade_date = get_trade_date(trade)
                
                matches.append({
                    "symbol": symbol,
                    "alert_date": alert_date.isoformat(),
                    "strategy": strategy,
                    "entry_price": entry_price,
                    "quantity": quantity,
                    "trade_entry_date": trade_date.isoformat() if trade_date else None,
                    "match_type": match_type
                })
        else:
            unmatched.append({
                "symbol": symbol,
                "alert_date": alert_date.isoformat(),
                "strategy": strategy
            })
    
    # Write matches to CSV
    if matches:
        matches_df = pd.DataFrame(matches)
        matches_df.to_csv(output_csv, index=False)
        print(f"Successfully exported {len(matches)} matches to {output_csv}", file=sys.stderr)
    else:
        print("No matches found between alerts and trades.", file=sys.stderr)
    
    # Report unmatched
    if unmatched:
        print(f"Warning: {len(unmatched)} alerts did not match any trades", file=sys.stderr)
        # Optionally write unmatched to separate file for debugging
        unmatched_df = pd.DataFrame(unmatched)
        unmatched_csv = "alerts_unmatched.csv"
        unmatched_df.to_csv(unmatched_csv, index=False)
        print(f"Unmatched alerts written to {unmatched_csv}", file=sys.stderr)


if __name__ == "__main__":
    main()
