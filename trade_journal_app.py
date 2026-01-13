"""
Trade Journal Application
=========================

This desktop application allows a user to import CSV files of brokerage transactions,
parse them into a trade journal, compute profit and loss (P&L) statistics, and
visualize equity over time.  It is designed for Windows environments using the
standard `tkinter` GUI library and leverages `pandas` and `matplotlib` for
data handling and visualization.  The application currently parses Fidelity
transaction exports (as demonstrated in the included sample) and matches
buys and sells using a FIFO (first‑in, first‑out) method.  It tracks open
positions, closed trades, and allows the user to annotate individual trades
with notes.  Summary statistics and an equity curve are presented after
importing a file.  The journal can be exported back to CSV or Excel for
external analysis.

This module should be run directly using Python 3:

    python trade_journal_app.py

The application runs in memory only (notes and computed data are not
persisted between sessions) but demonstrates how a more permanent storage
solution (e.g., SQLite) could be integrated later.  It is written in a
modular fashion so that core parsing logic can be reused in a future web
application.
"""

import csv
import datetime as dt
import os
import sqlite3
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional

import pandas as pd
import numpy as np
import matplotlib
import matplotlib.pyplot as plt

# Use the TkAgg backend for embedding in Tkinter
matplotlib.use("TkAgg")

import tkinter as tk  # noqa: E402
from tkinter import filedialog, messagebox, simpledialog  # noqa: E402
from tkinter import ttk  # noqa: E402

import webbrowser  # For opening external chart URLs
try:
    import yfinance as yf
    HAS_YFINANCE = True
except ImportError:
    HAS_YFINANCE = False

try:
    import mplfinance as mpf
    HAS_MPLFINANCE = True
except ImportError:
    HAS_MPLFINANCE = False



import json

# Patch matplotlib RectangleSelector to use gray instead of white
try:
    from matplotlib.widgets import RectangleSelector
    _original_RectangleSelector_init = RectangleSelector.__init__
    
    def _patched_RectangleSelector_init(self, ax, onselect, **kwargs):
        # Set gray color for the selector rectangle if not already specified
        if 'props' not in kwargs:
            kwargs['props'] = dict(facecolor='#CCCCCC', alpha=0.25, edgecolor='#666666', linewidth=1.5)
        _original_RectangleSelector_init(self, ax, onselect, **kwargs)
    
    RectangleSelector.__init__ = _patched_RectangleSelector_init
except Exception:
    pass

from pathlib import Path

# Config file for persisting chart/UI settings
# Use script directory so running from different CWD still persists.
CONFIG_FILE = str(Path(__file__).resolve().parent / ".trade_journal_config.json")

# Color name to hex mapping
COLOR_MAP = {
    "blue": "#0000FF",
    "orange": "#FFA500",
    "red": "#FF0000",
    "green": "#00FF00",
    "purple": "#800080",
    "cyan": "#00FFFF",
    "magenta": "#FF00FF",
    "yellow": "#FFFF00",
}

def name_to_hex(color_name: str) -> str:
    """Convert color name to hex, or return as-is if already hex."""
    if color_name.startswith("#"):
        return color_name
    return COLOR_MAP.get(color_name.lower(), color_name)


def _normalize_hex(hex_color: str) -> str:
    if not isinstance(hex_color, str):
        return ""
    s = hex_color.strip()
    if not s:
        return ""
    return s.upper() if s.startswith("#") else s


def color_display_name(color_value: str) -> str:
    """Return a human-friendly color name for display (never returns hex).

    - If the color is a known name (e.g. "blue"), returns "Blue".
    - If the color is a hex that matches COLOR_MAP, returns that name.
    - Otherwise returns "Custom".
    """
    if not isinstance(color_value, str) or not color_value.strip():
        return "Custom"
    raw = color_value.strip()
    if not raw.startswith("#"):
        return raw.strip().capitalize()
    normalized = _normalize_hex(raw)
    for name, hx in COLOR_MAP.items():
        if _normalize_hex(hx) == normalized:
            return name.capitalize()
    return "Custom"

def load_chart_settings():
    """Load chart settings from config file."""
    default_settings = {
        "ema1_period": 20,
        "ema2_period": 50,
        "ema3_period": 200,
        "ema1_color": "blue",
        "ema2_color": "orange",
        "ema3_color": "purple",
        "ema1_type": "EMA",
        "ema2_type": "EMA",
        "ema3_type": "EMA",
        "ema1_enabled": True,
        "ema2_enabled": True,
        "ema3_enabled": False,
        "top_n": "",
        "top_filter_type": "None",
        "top_filter_metric": "PnL"
    }
    try:
        if os.path.exists(CONFIG_FILE):
            with open(CONFIG_FILE, 'r') as f:
                loaded = json.load(f)
                if isinstance(loaded, dict):
                    merged = default_settings.copy()
                    merged.update(loaded)
                    return merged
    except Exception:
        pass
    return default_settings

def save_chart_settings(settings):
    """Save chart settings to config file.

    Important: merges with existing config so partial updates don't wipe other keys.
    """
    try:
        merged = load_chart_settings()
        if isinstance(settings, dict):
            merged.update(settings)
        with open(CONFIG_FILE, 'w') as f:
            json.dump(merged, f, indent=2)
    except Exception as e:
        print(f"Warning: Could not save chart settings: {e}")


@dataclass
class Transaction:
    """Represents a single transaction from the CSV."""

    run_date: dt.datetime
    account: str
    account_number: str
    symbol: str
    action: str  # Raw action string (e.g. "YOU BOUGHT ...")
    price: float
    quantity: float
    amount: float
    settlement_date: Optional[dt.datetime]

    @property
    def is_buy(self) -> bool:
        """Return True if the transaction represents a purchase (quantity > 0)."""
        return self.quantity > 0

    @property
    def is_sell(self) -> bool:
        """Return True if the transaction represents a sale (quantity < 0)."""
        return self.quantity < 0


@dataclass
class TradeEntry:
    """Represents a matched trade (one or more buys matched to a sell).
    
    Note: Fee handling assumes fees=0 (not included in P&L calculations).
    If fees are present, they should be deducted from pnl manually during import.
    Status tracks whether the lot is OPEN (partially filled) or CLOSED (fully exited).
    """

    account: str
    account_number: str
    symbol: str
    entry_date: dt.datetime
    entry_price: float
    exit_date: Optional[dt.datetime]
    exit_price: Optional[float]
    quantity: float
    pnl: Optional[float]  # Profit or loss; None if still open
    hold_period: Optional[int]  # Days between entry and exit; None if open
    note: str = ""
    entry_strategy: str = ""  # Entry strategy description
    exit_strategy: str = ""  # Exit strategy description
    buy_id: int = -1  # Identifier linking trades back to original buy lot
    status: str = "OPEN"  # OPEN (partially filled) or CLOSED (fully exited)

    @property
    def is_closed(self) -> bool:
        return self.exit_date is not None and self.status == "CLOSED"

    @property
    def pnl_pct(self) -> Optional[float]:
        """Return the percentage return: ((exit_price - entry_price) / entry_price) * 100.
        
        Returns None if the trade is still open (no exit_price).
        """
        if self.exit_price is None or self.entry_price == 0:
            return None
        return round(((self.exit_price - self.entry_price) / self.entry_price) * 100, 2)


class PriceDataManager:
    """Manages price data caching and retrieval using SQLite."""

    def __init__(self, db_path: str):
        """Initialize the price data manager with a SQLite database path."""
        self.db_path = db_path
        self._init_db()

    def _init_db(self) -> None:
        """Initialize the SQLite database with price data table if it doesn't exist."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS price_data (
                        id INTEGER PRIMARY KEY AUTOINCREMENT,
                        symbol TEXT NOT NULL,
                        date TEXT NOT NULL,
                        open REAL NOT NULL,
                        high REAL NOT NULL,
                        low REAL NOT NULL,
                        close REAL NOT NULL,
                        volume INTEGER,
                        UNIQUE(symbol, date)
                    )
                """)
                cursor.execute("""
                    CREATE TABLE IF NOT EXISTS price_metadata (
                        symbol TEXT PRIMARY KEY,
                        last_fetched TEXT,
                        start_date TEXT,
                        end_date TEXT
                    )
                """)
                conn.commit()
        except Exception as e:
            print(f"Error initializing database: {e}")

    def has_data(self, symbol: str) -> bool:
        """Check if price data exists for a given symbol."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM price_data WHERE symbol = ?", (symbol.upper(),))
                count = cursor.fetchone()[0]
                return count > 0
        except Exception:
            return False

    def get_price_data(self, symbol: str, start_date: dt.date, end_date: dt.date) -> Optional[pd.DataFrame]:
        """Retrieve cached price data for a symbol within a date range.
        
        Returns a DataFrame with columns: Date, Open, High, Low, Close, Volume
        indexed by date. Returns None if data not available.
        """
        try:
            with sqlite3.connect(self.db_path) as conn:
                query = """
                    SELECT date, open, high, low, close, volume
                    FROM price_data
                    WHERE symbol = ? AND date BETWEEN ? AND ?
                    ORDER BY date
                """
                df = pd.read_sql_query(
                    query,
                    conn,
                    params=(symbol.upper(), start_date.isoformat(), end_date.isoformat())
                )
                if df.empty:
                    return None
                df['date'] = pd.to_datetime(df['date'])
                # Ensure price columns are numeric (SQLite may return as text)
                for col in ['open', 'high', 'low', 'close', 'volume']:
                    if col in df.columns:
                        df[col] = pd.to_numeric(df[col], errors='coerce')
                df.set_index('date', inplace=True)
                return df
        except Exception as e:
            print(f"Error retrieving price data: {e}")
            return None

    def fetch_and_store(self, symbol: str, start_date: dt.date, end_date: dt.date) -> Optional[pd.DataFrame]:
        """Fetch price data from yfinance and store in SQLite.
        
        Returns the fetched DataFrame or None if unsuccessful.
        Raises RuntimeError if yfinance is not available.
        """
        if not HAS_YFINANCE:
            raise RuntimeError("yfinance is required. Install with: pip install yfinance")
        
        try:
            # Fetch data from yfinance (use uppercase symbol)
            symbol_upper = symbol.upper()
            print(f"\n>>> Fetching {symbol_upper} from {start_date} to {end_date}")
            print(f">>> yfinance version: {yf.__version__ if hasattr(yf, '__version__') else 'unknown'}")
            
            df = yf.download(symbol_upper, start=start_date, end=end_date, progress=False)
            print(f">>> Raw response type: {type(df)}")
            print(f">>> Raw response shape: {df.shape if hasattr(df, 'shape') else 'N/A'}")
            
            # Check if we got any data
            if df is None or (hasattr(df, 'empty') and df.empty):
                print(f">>> ERROR: No data returned from yfinance for {symbol_upper}")
                return None
            
            print(f">>> Raw columns: {df.columns.tolist()}")
            print(f">>> First few rows:\n{df.head()}")
            
            # yfinance returns data with Date as index, reset it to a column
            df.reset_index(inplace=True)
            print(f">>> After reset_index columns: {df.columns.tolist()}")
            
            # Handle MultiIndex columns (yfinance returns tuple column names when fetching single ticker)
            if isinstance(df.columns, pd.MultiIndex):
                print(f">>> MultiIndex detected, flattening columns...")
                # Flatten to single level - take the first level (the actual column names like 'Close', 'Open', etc)
                df.columns = [col[0] if isinstance(col, tuple) else col for col in df.columns]
                print(f">>> After flattening MultiIndex columns: {df.columns.tolist()}")
            
            # Ensure proper column names (lowercase)
            df.columns = [col.lower() for col in df.columns]
            print(f">>> After lowercase columns: {df.columns.tolist()}")
            
            # Validate required columns exist
            required_cols = {'date', 'open', 'high', 'low', 'close'}
            if not required_cols.issubset(set(df.columns)):
                print(f"Missing required columns. Got: {df.columns.tolist()}")
                return None
            
            print(f"Successfully fetched {len(df)} rows for {symbol_upper}")
            
            # Store in database
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Clear existing data for this symbol (full refresh)
                cursor.execute("DELETE FROM price_data WHERE symbol = ?", (symbol_upper,))
                
                # Insert new data
                for _, row in df.iterrows():
                    # Convert date to string format
                    date_val = row['date']
                    if isinstance(date_val, str):
                        date_str = date_val[:10]  # Extract YYYY-MM-DD if it's a timestamp string
                    else:
                        date_str = pd.to_datetime(date_val).date().isoformat()
                    
                    cursor.execute("""
                        INSERT OR REPLACE INTO price_data
                        (symbol, date, open, high, low, close, volume)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    """, (
                        symbol_upper,
                        date_str,
                        float(row['open']),
                        float(row['high']),
                        float(row['low']),
                        float(row['close']),
                        int(row.get('volume', 0))
                    ))
                
                # Update metadata
                cursor.execute("""
                    INSERT OR REPLACE INTO price_metadata
                    (symbol, last_fetched, start_date, end_date)
                    VALUES (?, ?, ?, ?)
                """, (symbol_upper, dt.datetime.now().isoformat(), start_date.isoformat(), end_date.isoformat()))
                
                conn.commit()
                print(f"Successfully stored {len(df)} rows for {symbol_upper}")
            
            return df
        except Exception as e:
            print(f"Error fetching data for {symbol}: {e}")
            import traceback
            traceback.print_exc()
            return None

    def get_metadata(self, symbol: str) -> Optional[Dict[str, str]]:
        """Get metadata about when data was last fetched for a symbol."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    "SELECT last_fetched, start_date, end_date FROM price_metadata WHERE symbol = ?",
                    (symbol.upper(),)
                )
                row = cursor.fetchone()
                if row:
                    return {
                        "last_fetched": row[0],
                        "start_date": row[1],
                        "end_date": row[2]
                    }
        except Exception:
            pass
        return None


class TradeJournalModel:
    """Core logic for storing transactions, matching trades, and computing metrics."""

    def __init__(self):
        self.transactions: List[Transaction] = []
        self.trades: List[TradeEntry] = []
        # Open positions keyed by (account_number, symbol) storing a list of remaining buys
        # Each element is a dict with keys: qty, price, date
        self.open_positions: Dict[Tuple[str, str], List[Dict[str, object]]] = {}
        # Notes keyed by unique trade key (tuple) to persist across sessions
        self.notes: Dict[tuple, str] = {}
        # Counter for assigning unique IDs to buys
        self.next_buy_id: int = 0
        # Screenshots keyed by unique trade key (list of image paths for multiple screenshots)
        self.screenshots: Dict[tuple, List[str]] = {}
        # Set of unique transaction keys to detect duplicates across sessions
        # Each key is a tuple of (run_date, account_number, symbol, quantity, price, amount)
        self.seen_tx_keys: set = set()

        # Track duplicate transactions encountered when loading a new CSV.  Each element
        # is a Transaction object representing a row that was skipped because it
        # matched an existing transaction key from a prior session.  Duplicates
        # within the same file are not stored here because they are treated as
        # valid separate transactions when the time portion of the run date is
        # omitted (e.g., multiple buys on the same day).  This list is cleared
        # on every call to ``load_csv`` so that only the duplicates from the most
        # recent import are available for display.
        self.duplicate_transactions: List[Transaction] = []

    def clear(self) -> None:
        """Reset all stored data."""
        self.transactions.clear()
        self.trades.clear()
        self.open_positions.clear()
        self.notes.clear()
        self.screenshots.clear()
        self.entry_strategies.clear()
        self.exit_strategies.clear()
        self.seen_tx_keys.clear()
        self.next_buy_id = 0

    def reset_matching(self) -> None:
        """Reset trades and open positions while preserving transactions, notes, screenshots and seen keys."""
        self.trades.clear()
        self.open_positions.clear()
        # do not clear notes, screenshots or seen_tx_keys
        self.next_buy_id = 0

    def load_csv(self, filepath: str) -> None:
        """Load and parse transactions from a Fidelity CSV file.

        The CSV may contain footer lines (legal notices); only rows that
        begin with a date (MM/DD/YYYY) will be parsed.  Quantity is
        interpreted as positive for buys and negative for sells.
        """
        # Do not clear existing transactions, notes or screenshots; only reset matching state
        self.reset_matching()
        # Clear previous duplicate records
        self.duplicate_transactions.clear()
        self.duplicate_count = 0
        # Capture keys from prior sessions to detect duplicates across sessions
        existing_keys = set(self.seen_tx_keys)
        new_keys: set = set()
        try:
            with open(filepath, newline="", encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                # Skip blank lines at the beginning until we find the header
                header: List[str] = []
                for row in reader:
                    if not row or not any(cell.strip() for cell in row):
                        continue
                    header = row
                    break
                if not header:
                    raise RuntimeError("CSV file appears to be empty or missing header")
                # Map column names to indices for flexibility
                header_map = {name.strip(): idx for idx, name in enumerate(header)}
                # Determine which columns exist
                run_date_idx = header_map.get("Run Date")
                account_idx = header_map.get("Account")
                acct_num_idx = header_map.get("Account Number")
                action_idx = header_map.get("Action")
                symbol_idx = header_map.get("Symbol")
                price_idx = header_map.get("Price ($)")
                qty_idx = header_map.get("Quantity")
                amount_idx = header_map.get("Amount ($)")
                settlement_idx = header_map.get("Settlement Date")
                # Validate essential columns
                if run_date_idx is None or qty_idx is None or price_idx is None:
                    raise RuntimeError("CSV file is missing required columns like Run Date, Quantity or Price")
                # Helper to parse floats robustly
                def to_float(s: str) -> float:
                    try:
                        return float(s.replace(',', '')) if s else 0.0
                    except ValueError:
                        return 0.0
                # Process each row
                for row in reader:
                    # Skip completely blank rows
                    if not row or len(row) <= run_date_idx:
                        continue
                    run_date_str = row[run_date_idx].strip()
                    if not run_date_str:
                        continue
                    # Skip footnotes and other non-transaction rows that do not start with a digit
                    if not run_date_str[0].isdigit():
                        continue
                    # Parse date and optional time
                    run_date: Optional[dt.datetime] = None
                    for fmt in ("%m/%d/%Y %I:%M %p", "%m/%d/%Y"):
                        try:
                            run_date = dt.datetime.strptime(run_date_str, fmt)
                            break
                        except ValueError:
                            continue
                    if run_date is None:
                        continue
                    # Helper to safely get a cell string
                    def safe_get(idx: Optional[int]) -> str:
                        return row[idx].strip() if (idx is not None and idx < len(row)) else ""
                    account = safe_get(account_idx)
                    acct_num = safe_get(acct_num_idx)
                    action = safe_get(action_idx)
                    symbol = safe_get(symbol_idx)
                    price_str = safe_get(price_idx)
                    qty_str = safe_get(qty_idx)
                    amount_str = safe_get(amount_idx)
                    settle_str = safe_get(settlement_idx)
                    price = to_float(price_str)
                    qty = to_float(qty_str)
                    amount = to_float(amount_str)
                    settlement_date: Optional[dt.datetime] = None
                    if settle_str:
                        for fmt in ("%m/%d/%Y %I:%M %p", "%m/%d/%Y"):
                            try:
                                settlement_date = dt.datetime.strptime(settle_str, fmt)
                                break
                            except ValueError:
                                continue
                    # Construct transaction object early to record duplicates if necessary
                    tx = Transaction(
                        run_date=run_date,
                        account=account,
                        account_number=acct_num,
                        symbol=symbol,
                        action=action,
                        price=price,
                        quantity=qty,
                        amount=amount,
                        settlement_date=settlement_date,
                    )
                    # Compute duplicate key
                    key = (run_date, acct_num, symbol, qty, price, amount)
                    # Check duplicates across sessions only
                    if key in existing_keys:
                        # Record duplicate and skip adding to transactions
                        self.duplicate_transactions.append(tx)
                        self.duplicate_count += 1
                        continue
                    # Accept this transaction and record key for this file
                    new_keys.add(key)
                    self.transactions.append(tx)
            # Finished reading file
            # Update global seen keys with new keys from this import
            self.seen_tx_keys.update(new_keys)
        except Exception as e:
            raise RuntimeError(f"Failed to load CSV: {e}")
        # Re-match trades based on the updated transaction list
        self._match_trades()

    def save_state(self, filepath: str, filter_state: dict = None) -> None:
        """Persist the current transactions, notes, screenshots, and filter state to disk."""
        import pickle
        data = {
            'transactions': self.transactions,
            'notes': self.notes,
            'screenshots': self.screenshots,
            'seen_tx_keys': self.seen_tx_keys,
            'filter_state': filter_state or {},
            'entry_strategies': self.entry_strategies,
            'exit_strategies': self.exit_strategies,
        }
        try:
            with open(filepath, 'wb') as f:
                pickle.dump(data, f)
        except Exception:
            pass  # ignore persistence errors

    def load_state(self, filepath: str) -> dict:
        """Load transactions, notes, screenshots, and filter state from a persisted file.
        
        Returns:
            dict: The filter state dictionary (may be empty if not present in saved file)
        """
        import pickle
        if not os.path.exists(filepath):
            return {}
        try:
            with open(filepath, 'rb') as f:
                data = pickle.load(f)
            self.transactions = data.get('transactions', [])
            self.notes = data.get('notes', {})
            self.screenshots = data.get('screenshots', {})
            self.entry_strategies = data.get('entry_strategies', {})
            self.exit_strategies = data.get('exit_strategies', {})
            self.seen_tx_keys = data.get('seen_tx_keys', set())
            # Reset buy id counter and re-match trades
            self.next_buy_id = 0
            self._match_trades()
            return data.get('filter_state', {})
        except Exception:
            # If loading fails, silently ignore and start fresh
            self.clear()
            return {}

    def compute_key(self, trade: TradeEntry) -> tuple:
        """Compute a stable unique key for a trade entry to map notes and attachments.

        The key includes account_number, symbol, entry date (ISO), rounded entry price,
        rounded quantity, exit date (ISO or None), rounded exit price (or 0 if None),
        and rounded P&L (or 0 if None). Rounding helps mitigate minor float differences.
        """
        entry_date_str = trade.entry_date.isoformat()
        exit_date_str = trade.exit_date.isoformat() if trade.exit_date else None
        # Use rounding to 6 decimal places for floats
        def r(value: Optional[float]) -> float:
            return round(value if value is not None else 0.0, 6)
        return (
            trade.account_number or "",
            trade.symbol or "",
            entry_date_str,
            r(trade.entry_price),
            r(trade.quantity),
            exit_date_str,
            r(trade.exit_price),
            r(trade.pnl),
        )

    def _match_trades(self) -> None:
        """Match buy and sell transactions into trade entries using FIFO."""
        # Reset trades and open positions
        self.trades = []
        self.open_positions = {}
        # Use a sorted copy of transactions for matching to preserve original order in self.transactions
        sorted_txs = sorted(self.transactions, key=lambda tx: tx.run_date)
        # Process buys and sells
        for tx in sorted_txs:
            key = (tx.account_number, tx.symbol)
            if tx.is_buy:
                buy_id = self.next_buy_id
                self.next_buy_id += 1
                if key not in self.open_positions:
                    self.open_positions[key] = []
                self.open_positions[key].append({
                    "qty": tx.quantity,
                    "price": tx.price,
                    "date": tx.run_date,
                    "id": buy_id,
                })
            elif tx.is_sell:
                remaining = abs(tx.quantity)
                if key not in self.open_positions or not self.open_positions[key]:
                    # Sell without prior buys; unmatched portion becomes open trade
                    trade = TradeEntry(
                        account=tx.account,
                        account_number=tx.account_number,
                        symbol=tx.symbol,
                        entry_date=tx.run_date,
                        entry_price=tx.price,
                        exit_date=None,
                        exit_price=None,
                        quantity=remaining,
                        pnl=None,
                        hold_period=None,
                        buy_id=-1,
                    )
                    self.trades.append(trade)
                    continue
                while remaining > 1e-8:
                    if not self.open_positions[key]:
                        # No buys left; record unmatched sell portion
                        trade = TradeEntry(
                            account=tx.account,
                            account_number=tx.account_number,
                            symbol=tx.symbol,
                            entry_date=tx.run_date,
                            entry_price=tx.price,
                            exit_date=None,
                            exit_price=None,
                            quantity=remaining,
                            pnl=None,
                            hold_period=None,
                            buy_id=-1,
                        )
                        self.trades.append(trade)
                        break
                    buy = self.open_positions[key][0]
                    matched_qty = min(buy["qty"], remaining)
                    entry_date = buy["date"]
                    entry_price = buy["price"]
                    exit_date = tx.run_date
                    exit_price = tx.price
                    pnl = (exit_price - entry_price) * matched_qty
                    hold_period = (exit_date - entry_date).days
                    trade = TradeEntry(
                        account=tx.account,
                        account_number=tx.account_number,
                        symbol=tx.symbol,
                        entry_date=entry_date,
                        entry_price=entry_price,
                        exit_date=exit_date,
                        exit_price=exit_price,
                        quantity=matched_qty,
                        pnl=pnl,
                        hold_period=hold_period,
                        buy_id=buy["id"],
                        status="CLOSED",  # Mark as closed (fully allocated)
                    )
                    self.trades.append(trade)
                    remaining -= matched_qty
                    buy["qty"] -= matched_qty
                    if buy["qty"] <= 1e-8:
                        self.open_positions[key].pop(0)
        # Record remaining open buys as open trades
        for key, buys in self.open_positions.items():
            acct_num, symbol = key
            for buy in buys:
                trade = TradeEntry(
                    account="",
                    account_number=acct_num,
                    symbol=symbol,
                    entry_date=buy["date"],
                    entry_price=buy["price"],
                    exit_date=None,
                    exit_price=None,
                    quantity=buy["qty"],
                    pnl=None,
                    hold_period=None,
                    buy_id=buy["id"],
                    status="OPEN",  # Mark as open (not fully exited)
                )
                self.trades.append(trade)
        # Build open quantity maps
        self.open_qty_by_buy_id = {}
        for buys in self.open_positions.values():
            for buy in buys:
                self.open_qty_by_buy_id[buy["id"]] = self.open_qty_by_buy_id.get(buy["id"], 0.0) + buy["qty"]
        self.open_qty_by_symbol = {}
        for (acct_num, symbol), buys in self.open_positions.items():
            total_qty = sum(buy["qty"] for buy in buys)
            self.open_qty_by_symbol[(acct_num, symbol)] = total_qty

    def compute_summary(self, account_filter: Optional[str] = None, *, closed_only: bool = False,
                        start_date: Optional[dt.date] = None, end_date: Optional[dt.date] = None) -> Dict[str, float]:
        """Compute summary statistics for trades.

        Filters trades by account_number (if ``account_filter`` is provided), by date range on entry_date
        (inclusive), and optionally excludes trades whose originating buy position has not been fully closed
        when ``closed_only`` is True. Only trades with exit_date and status==CLOSED are counted as closed trades
        for statistics (requirement: trade = fully closed lot).
        
        Returns:
            Dictionary with keys: total_pnl, num_trades, num_wins, num_losses, num_breakeven,
            win_ratio, avg_pnl, avg_hold, avg_winner_pnl_pct, avg_loser_pnl_pct, 
            avg_hold_winners, avg_hold_losers, profit_factor, expectancy.
        """
        total_pnl = 0.0
        num_trades = 0
        num_wins = 0
        num_losses = 0
        num_breakeven = 0
        total_hold = 0
        winner_pnl_sum = 0.0
        loser_pnl_sum = 0.0
        winner_pnl_pct_sum = 0.0
        loser_pnl_pct_sum = 0.0
        winner_hold_sum = 0
        loser_hold_sum = 0
        
        for trade in self.trades:
            # Only consider CLOSED trades (fully exited lots) with status == "CLOSED"
            if not trade.is_closed:
                continue
            # Account filter
            if account_filter and account_filter != "all" and trade.account_number != account_filter:
                continue
            # Date range filter on entry date (inclusive)
            if start_date and trade.entry_date.date() < start_date:
                continue
            if end_date and trade.entry_date.date() > end_date:
                continue
            # Closed-only filter: skip trades whose originating buy is still open
            if closed_only:
                if trade.buy_id < 0:
                    continue
                if self.open_qty_by_buy_id.get(trade.buy_id, 0.0) > 1e-8:
                    continue
            
            pnl = trade.pnl or 0.0
            pnl_pct = trade.pnl_pct or 0.0
            total_pnl += pnl
            num_trades += 1
            
            if pnl > 1e-8:  # Win (PnL > 0)
                num_wins += 1
                winner_pnl_sum += pnl
                winner_pnl_pct_sum += pnl_pct
                winner_hold_sum += trade.hold_period or 0
            elif pnl < -1e-8:  # Loss (PnL < 0)
                num_losses += 1
                loser_pnl_sum += pnl
                loser_pnl_pct_sum += pnl_pct
                loser_hold_sum += trade.hold_period or 0
            else:  # Breakeven (PnL ≈ 0)
                num_breakeven += 1
            
            total_hold += trade.hold_period or 0
        
        # Compute derived metrics
        win_ratio = (num_wins / (num_wins + num_losses)) if (num_wins + num_losses) > 0 else 0.0
        avg_pnl = (total_pnl / num_trades) if num_trades else 0.0
        avg_hold = (total_hold / num_trades) if num_trades else 0.0
        avg_winner_pnl = (winner_pnl_sum / num_wins) if num_wins > 0 else 0.0
        avg_loser_pnl = (loser_pnl_sum / num_losses) if num_losses > 0 else 0.0
        avg_winner_pnl_pct = (winner_pnl_pct_sum / num_wins) if num_wins > 0 else 0.0
        avg_loser_pnl_pct = (loser_pnl_pct_sum / num_losses) if num_losses > 0 else 0.0
        avg_hold_winners = (winner_hold_sum / num_wins) if num_wins > 0 else 0.0
        avg_hold_losers = (loser_hold_sum / num_losses) if num_losses > 0 else 0.0
        
        # Profit Factor = sum(wins pnl) / abs(sum(losses pnl))
        profit_factor = 0.0
        if num_losses > 0 and loser_pnl_sum != 0:
            profit_factor = winner_pnl_sum / abs(loser_pnl_sum)
        
        # Expectancy = win_rate * avg_win + (1 - win_rate) * avg_loss
        expectancy = win_ratio * avg_winner_pnl + (1 - win_ratio) * avg_loser_pnl
        
        return {
            "total_pnl": total_pnl,
            "num_trades": num_trades,
            "num_wins": num_wins,
            "num_losses": num_losses,
            "num_breakeven": num_breakeven,
            "win_ratio": win_ratio,
            "avg_pnl": avg_pnl,
            "avg_hold": avg_hold,
            "avg_winner_pnl_pct": avg_winner_pnl_pct,
            "avg_loser_pnl_pct": avg_loser_pnl_pct,
            "avg_hold_winners": avg_hold_winners,
            "avg_hold_losers": avg_hold_losers,
            "profit_factor": profit_factor,
            "expectancy": expectancy,
        }

    def equity_curve(self, account_filter: Optional[str] = None, *, closed_only: bool = False,
                     start_date: Optional[dt.date] = None, end_date: Optional[dt.date] = None) -> pd.DataFrame:
        """Return a DataFrame representing the cumulative equity over time.

        Each closed trade contributes its P&L at the exit date.  Trades are filtered by account,
        date range on entry date (inclusive), and optionally whether their originating buy position is fully
        closed when ``closed_only`` is True. Dates outside the specified range are excluded. The DataFrame
        contains columns 'date' and 'equity', sorted chronologically.
        """
        data: Dict[dt.date, float] = {}
        for trade in self.trades:
            if not trade.is_closed:
                continue
            if account_filter and account_filter != "all" and trade.account_number != account_filter:
                continue
            # Date range filter on entry date (inclusive) - match compute_summary logic
            if start_date and trade.entry_date.date() < start_date:
                continue
            if end_date and trade.entry_date.date() > end_date:
                continue
            if closed_only:
                if trade.buy_id < 0 or self.open_qty_by_buy_id.get(trade.buy_id, 0.0) > 1e-8:
                    continue
            exit_date = trade.exit_date.date()  # type: ignore
            data[exit_date] = data.get(exit_date, 0.0) + (trade.pnl or 0.0)
        dates = sorted(data.keys())
        equity_values = []
        cumulative = 0.0
        for d in dates:
            cumulative += data[d]
            equity_values.append(cumulative)
        return pd.DataFrame({"date": dates, "equity": equity_values})


class TradeJournalApp:
    """Graphical application for the trade journal."""

    def __init__(self, root: tk.Tk):
        self.root = root
        self.root.title("Trade Journal")
        self.model = TradeJournalModel()
        # Determine path for persisting state within the same directory as script
        script_dir = os.path.dirname(os.path.abspath(__file__))
        self.persist_path = os.path.join(script_dir, 'journal_state.pkl')
        self.db_path = os.path.join(script_dir, 'price_data.db')
        # Initialize price data manager
        self.price_manager = PriceDataManager(self.db_path)
        # UI elements
        self._build_ui()
        # Sorting state: which column and whether descending
        self.sort_by: Optional[str] = None
        self.sort_descending: bool = False
        # Mapping from Treeview item id to trade key for notes/screenshots
        self.id_to_key: Dict[str, tuple] = {}
        # Mapping from group row id to list of trade indices (used for deletion)
        self.group_id_to_indices: Dict[str, List[int]] = {}
        # Date filter boundaries (dt.date objects)
        self.start_date: Optional[dt.date] = None
        self.end_date: Optional[dt.date] = None
        # Chart-related state
        self.current_chart_symbol: Optional[str] = None
        self.chart_canvas = None
        # Load persisted data (if available)
        self.load_persisted_data()
        # Register handler to save on close
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)

    def _build_ui(self) -> None:
        """Construct the user interface."""
        # Top frame for file actions and account filter - using grid for better responsive layout
        top_frame = ttk.Frame(self.root)
        top_frame.pack(side=tk.TOP, fill=tk.X, padx=5, pady=5)

        # Collapse button for top controls
        self.top_controls_visible = True
        collapse_btn = ttk.Button(top_frame, text="−", width=2, command=self._toggle_top_controls)
        collapse_btn.grid(row=0, column=0, padx=(0, 5), pady=2)
        self.top_collapse_btn = collapse_btn
        self.top_controls_frame = top_frame

        # Row 0: Main action buttons
        load_btn = ttk.Button(top_frame, text="Load CSV", command=self.load_csv)
        load_btn.grid(row=0, column=1, padx=(0, 5), pady=2)

        export_btn = ttk.Button(top_frame, text="Export Journal", command=self.export_journal)
        export_btn.grid(row=0, column=2, padx=(0, 5), pady=2)

        # Button to add a manual transaction
        add_tx_btn = ttk.Button(top_frame, text="Add Transaction", command=self.add_transaction_dialog)
        add_tx_btn.grid(row=0, column=3, padx=(0, 5), pady=2)

        # Button to delete selected transactions
        del_selected_btn = ttk.Button(top_frame, text="Delete Selected", command=self.delete_selected_transactions)
        del_selected_btn.grid(row=0, column=4, padx=(0, 5), pady=2)

        # Button to clear the entire journal
        clear_btn = ttk.Button(top_frame, text="Clear Journal", command=self.clear_journal)
        clear_btn.grid(row=0, column=5, padx=(0, 5), pady=2)

        # Account filter on row 0
        ttk.Label(top_frame, text="Filter Account:").grid(row=0, column=6, padx=(5, 2), pady=2)
        self.account_var = tk.StringVar(value="all")
        self.account_dropdown = ttk.Combobox(top_frame, textvariable=self.account_var, state="readonly", width=15)
        self.account_dropdown.grid(row=0, column=7, padx=(0, 5), pady=2)
        self.account_dropdown.bind("<<ComboboxSelected>>", self.on_account_filter_change)

        # Row 1: Top N filter controls
        ttk.Label(top_frame, text="Top N:").grid(row=1, column=1, padx=(0, 2), pady=2, sticky="e")
        
        # Load saved settings
        saved_settings = load_chart_settings()
        
        self.top_n_var = tk.StringVar(value=saved_settings.get("top_n", ""))
        top_n_entry = ttk.Entry(top_frame, textvariable=self.top_n_var, width=5)
        top_n_entry.grid(row=1, column=2, padx=(0, 2), pady=2)
        # Filter type: None, Winners, Losers
        self.top_filter_type_var = tk.StringVar(value=saved_settings.get("top_filter_type", "None"))
        top_filter_combo = ttk.Combobox(top_frame, textvariable=self.top_filter_type_var,
                                         values=["None", "Winners", "Losers"], state="readonly", width=8)
        top_filter_combo.grid(row=1, column=3, padx=(0, 2), pady=2)
        # Metric for winners/losers: PnL or PnL %
        ttk.Label(top_frame, text="by:").grid(row=1, column=4, padx=(0, 2), pady=2, sticky="e")
        self.top_filter_metric_var = tk.StringVar(value=saved_settings.get("top_filter_metric", "PnL"))
        top_metric_combo = ttk.Combobox(top_frame, textvariable=self.top_filter_metric_var,
                                         values=["PnL", "PnL %"], state="readonly", width=8)
        top_metric_combo.grid(row=1, column=5, padx=(0, 2), pady=2)
        apply_top_btn = ttk.Button(top_frame, text="Apply", command=self.on_top_filter_change)
        apply_top_btn.grid(row=1, column=6, padx=(0, 5), pady=2)

        # Checkbox to show only fully closed positions
        self.closed_only_var = tk.BooleanVar(value=False)
        closed_check = ttk.Checkbutton(top_frame, text="Closed positions only", variable=self.closed_only_var, command=self.on_closed_filter_change)
        closed_check.grid(row=1, column=7, columnspan=2, padx=(0, 5), pady=2, sticky="w")

        # Checkbox to group trades by symbol (collapsed parent row per closed position)
        self.group_var = tk.BooleanVar(value=True)
        group_check = ttk.Checkbutton(top_frame, text="Group by symbol", variable=self.group_var, command=self.on_group_change)
        group_check.grid(row=2, column=0, columnspan=2, padx=(0, 5), pady=2, sticky="w")

        # Row 2: Date filter fields
        ttk.Label(top_frame, text="Start Date (YYYY-MM-DD):").grid(row=2, column=0, padx=(0, 2), pady=2, sticky="e")
        self.start_date_var = tk.StringVar(value="")
        start_entry = ttk.Entry(top_frame, textvariable=self.start_date_var, width=12)
        # Bind a mouse click to open date picker
        start_entry.bind("<Button-1>", lambda e: self.open_date_picker(self.start_date_var))
        start_entry.grid(row=2, column=1, padx=(0, 2), pady=2)
        # Button to open date picker explicitly
        start_pick_btn = ttk.Button(top_frame, text="📅", width=3, command=lambda: self.open_date_picker(self.start_date_var))
        start_pick_btn.grid(row=2, column=2, padx=(0, 5), pady=2)
        ttk.Label(top_frame, text="End Date (YYYY-MM-DD):").grid(row=2, column=3, padx=(0, 2), pady=2, sticky="e")
        self.end_date_var = tk.StringVar(value="")
        end_entry = ttk.Entry(top_frame, textvariable=self.end_date_var, width=12)
        end_entry.bind("<Button-1>", lambda e: self.open_date_picker(self.end_date_var))
        end_entry.grid(row=2, column=4, padx=(0, 2), pady=2)
        end_pick_btn = ttk.Button(top_frame, text="📅", width=3, command=lambda: self.open_date_picker(self.end_date_var))
        end_pick_btn.grid(row=2, column=5, padx=(0, 5), pady=2)
        apply_date_btn = ttk.Button(top_frame, text="Apply Date Filter", command=self.apply_date_filter)
        apply_date_btn.grid(row=2, column=6, padx=(0, 5), pady=2)

        # Row 3: Strategy filters, clear filters and toggle table buttons
        ttk.Label(top_frame, text="Filter Entry:").grid(row=3, column=0, padx=(0, 2), pady=2, sticky="e")
        self.entry_strategy_filter_var = tk.StringVar(value="all")
        self.entry_strategy_filter_combo = ttk.Combobox(top_frame, textvariable=self.entry_strategy_filter_var, width=15)
        self.entry_strategy_filter_combo.grid(row=3, column=1, padx=(0, 5), pady=2)
        self.entry_strategy_filter_combo.bind("<<ComboboxSelected>>", self.on_strategy_filter_change)
        self.entry_strategy_filter_combo.bind("<KeyRelease>", self.on_strategy_filter_change)

        ttk.Label(top_frame, text="Filter Exit:").grid(row=3, column=2, padx=(0, 2), pady=2, sticky="e")
        self.exit_strategy_filter_var = tk.StringVar(value="all")
        self.exit_strategy_filter_combo = ttk.Combobox(top_frame, textvariable=self.exit_strategy_filter_var, width=15)
        self.exit_strategy_filter_combo.grid(row=3, column=3, padx=(0, 5), pady=2)
        self.exit_strategy_filter_combo.bind("<<ComboboxSelected>>", self.on_strategy_filter_change)
        self.exit_strategy_filter_combo.bind("<KeyRelease>", self.on_strategy_filter_change)

        clear_filter_btn = ttk.Button(top_frame, text="Clear Filters", command=self.clear_filters)
        clear_filter_btn.grid(row=3, column=4, padx=(0, 5), pady=2, sticky="w")
        
        self.table_visible = tk.BooleanVar(value=True)
        self.toggle_btn = ttk.Button(top_frame, text="Hide Table", command=self.toggle_table_visibility)
        self.toggle_btn.grid(row=3, column=5, columnspan=2, padx=(0, 5), pady=2, sticky="w")

        # Main frame with notebook (tabs)
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Create notebook for tabs
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)

        # TAB 1: Journal (original content)
        journal_frame = ttk.Frame(self.notebook)
        self.notebook.add(journal_frame, text="Journal")

        # TAB 2: Charts
        chart_frame = ttk.Frame(self.notebook)
        self.notebook.add(chart_frame, text="Charts")
        self._build_chart_tab(chart_frame)

        # Build journal tab content using existing layout structure
        self._build_journal_tab(journal_frame)

    def _build_journal_tab(self, parent_frame: ttk.Frame) -> None:
        """Build the journal tab content."""
        # Main horizontal PanedWindow: left (table+chart) and right (notes/summary full height)
        main_paned = ttk.PanedWindow(parent_frame, orient=tk.HORIZONTAL)
        main_paned.pack(fill=tk.BOTH, expand=True)

        # LEFT PANE: Table and Chart (vertical stack)
        left_frame = ttk.Frame(main_paned)
        main_paned.add(left_frame, weight=2)

        # Vertical pane for table and chart
        self.left_paned = ttk.PanedWindow(left_frame, orient=tk.VERTICAL)
        self.left_paned.pack(fill=tk.BOTH, expand=True)

        # Top section: Table
        self.table_paned = ttk.PanedWindow(self.left_paned, orient=tk.HORIZONTAL)
        self.left_paned.add(self.table_paned, weight=2)

        # Left pane: Table frame
        self.table_frame = ttk.Frame(self.table_paned)
        self.table_paned.add(self.table_frame, weight=3)

        # Treeview for trades
        columns = (
            "account", "symbol", "entry_date", "entry_price", "exit_date",
            "exit_price", "quantity", "pnl", "pnl_pct", "hold_period", "screenshot", "entry_strategy", "exit_strategy", "note"
        )
        self.tree = ttk.Treeview(self.table_frame, columns=columns, show="headings", selectmode="extended")
        for col in columns:
            header = col.replace("_", " ").title()
            # Special case for pnl_pct to show % symbol
            if col == "pnl_pct":
                header = "Pnl %"
            # Attach a command to each heading to allow sorting and column reordering
            self.tree.heading(col, text=header, command=lambda c=col: self.on_sort(c))
            # Configure column with sensible min width but allow stretch
            # move=True allows users to drag columns to reorder them
            min_width = 70  # Minimum width for most columns
            if col in {"note", "entry_strategy", "exit_strategy"}:
                min_width = 100
            elif col in {"account", "symbol", "entry_date", "exit_date"}:
                min_width = 85
            
            anchor = tk.W  # Default left align
            if col in {"entry_price", "exit_price", "pnl", "quantity", "pnl_pct", "hold_period"}:
                anchor = tk.E  # Right align for numbers
            elif col == "screenshot":
                anchor = tk.CENTER
            
            self.tree.column(col, width=min_width, minwidth=min_width, stretch=True, anchor=anchor)
        
        # Scrollbars
        vsb = ttk.Scrollbar(self.table_frame, orient="vertical", command=self.tree.yview)
        hsb = ttk.Scrollbar(self.table_frame, orient="horizontal", command=self.tree.xview)
        self.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        self.table_frame.columnconfigure(0, weight=1)
        self.table_frame.rowconfigure(0, weight=1)
        # Bind selection
        self.tree.bind("<<TreeviewSelect>>", self.on_tree_select)
        # Bind double-click on screenshot column to view screenshots
        self.tree.bind("<Double-Button-1>", self.on_tree_double_click)

        # RIGHT PANE: Notes and summary panel with scrollbar (spans full height)
        right_frame = ttk.Frame(main_paned)
        main_paned.add(right_frame, weight=1)
        
        # Add a scrollbar to the right frame
        right_canvas = tk.Canvas(right_frame, bg="gray20", highlightthickness=0)
        right_scrollbar = ttk.Scrollbar(right_frame, orient="vertical", command=right_canvas.yview)
        right_scrollable_frame = ttk.Frame(right_canvas)
        
        right_scrollable_frame.bind(
            "<Configure>",
            lambda e: right_canvas.configure(scrollregion=right_canvas.bbox("all"))
        )
        
        right_window_id = right_canvas.create_window(
            (0, 0),
            window=right_scrollable_frame,
            anchor="nw",
            width=right_canvas.winfo_width(),
        )
        right_canvas.configure(yscrollcommand=right_scrollbar.set)
        
        # Bind canvas resizing to update window width
        def _on_right_canvas_configure(e: tk.Event) -> None:
            try:
                right_canvas.itemconfig(right_window_id, width=e.width)
            except Exception:
                pass

        right_canvas.bind("<Configure>", _on_right_canvas_configure)

        # Mouse wheel scrolling (so the summary is reachable without resizing)
        def _on_right_mousewheel(e: tk.Event) -> str:
            try:
                # macOS uses small deltas; normalize a bit
                delta = int(-1 * (e.delta))
                if delta == 0:
                    return "break"
                step = 1 if delta > 0 else -1
                right_canvas.yview_scroll(step, "units")
            except Exception:
                pass
            return "break"

        def _bind_right_mousewheel(_: tk.Event) -> None:
            right_canvas.bind_all("<MouseWheel>", _on_right_mousewheel)

        def _unbind_right_mousewheel(_: tk.Event) -> None:
            try:
                right_canvas.unbind_all("<MouseWheel>")
            except Exception:
                pass

        right_canvas.bind("<Enter>", _bind_right_mousewheel)
        right_canvas.bind("<Leave>", _unbind_right_mousewheel)
        
        right_canvas.pack(side="left", fill="both", expand=True)
        right_scrollbar.pack(side="right", fill="y")
        
        # Entry Strategy label and text
        ttk.Label(right_scrollable_frame, text="Entry Strategy:").pack(anchor="w")
        ttk.Label(right_scrollable_frame, text="(comma-separated for multiple)", font=("TkDefaultFont", 8), foreground="gray").pack(anchor="w")
        self.entry_strategy_text = tk.Text(right_scrollable_frame, height=3, width=30)
        self.entry_strategy_text.pack(fill=tk.X, pady=(0, 5))
        
        # Exit Strategy label and text
        ttk.Label(right_scrollable_frame, text="Exit Strategy:").pack(anchor="w")
        ttk.Label(right_scrollable_frame, text="(comma-separated for multiple)", font=("TkDefaultFont", 8), foreground="gray").pack(anchor="w")
        self.exit_strategy_text = tk.Text(right_scrollable_frame, height=3, width=30)
        self.exit_strategy_text.pack(fill=tk.X, pady=(0, 5))
        
        # Note label and text
        ttk.Label(right_scrollable_frame, text="Trade Note:").pack(anchor="w")
        self.note_text = tk.Text(right_scrollable_frame, height=5, width=30)
        self.note_text.pack(fill=tk.X, pady=(0, 5))
        save_note_btn = ttk.Button(right_scrollable_frame, text="Save Note", command=self.save_note)
        save_note_btn.pack(anchor="w")
        # Button to add screenshot
        add_ss_btn = ttk.Button(right_scrollable_frame, text="Add Screenshot", command=self.add_screenshot)
        add_ss_btn.pack(anchor="w", pady=(5, 0))
        # Button to view screenshots in a zoomed window
        view_ss_btn = ttk.Button(right_scrollable_frame, text="View Screenshots", command=self.view_screenshots)
        view_ss_btn.pack(anchor="w", pady=(0, 2))
        # Button to remove all screenshots
        remove_ss_btn = ttk.Button(right_scrollable_frame, text="Remove Screenshots", command=self.remove_screenshot)
        remove_ss_btn.pack(anchor="w", pady=(0, 5))
        # Label to display screenshot count
        ttk.Label(right_scrollable_frame, text="Screenshots:").pack(anchor="w", pady=(10, 0))
        self.screenshot_var = tk.StringVar(value="")
        self.screenshot_label = ttk.Label(right_scrollable_frame, textvariable=self.screenshot_var, foreground="blue")
        self.screenshot_label.pack(anchor="w")
        # Image preview label (for displaying the screenshot)
        self.screenshot_preview_label = ttk.Label(right_scrollable_frame)
        self.screenshot_preview_label.pack(anchor="w", pady=(5, 0))

        # Buttons to view price charts (stacked vertically)
        button_frame = ttk.Frame(right_scrollable_frame)
        button_frame.pack(anchor="w", pady=(5, 5))
        
        ttk.Button(button_frame, text="View Chart", command=self.view_internal_chart).pack(anchor="w", pady=(0, 3))
        ttk.Button(button_frame, text="TradingView Chart", command=self.view_tradingview_chart).pack(anchor="w")
        # Summary labels
        ttk.Label(right_scrollable_frame, text="Summary:").pack(anchor="w", pady=(10, 0))
        self.summary_var = tk.StringVar(value="No data loaded")
        summary_label = ttk.Label(right_scrollable_frame, textvariable=self.summary_var, justify=tk.LEFT)
        summary_label.pack(anchor="w")

        # Bottom section: Chart (narrower in its own pane)
        chart_frame = ttk.Frame(self.left_paned)
        self.left_paned.add(chart_frame, weight=1)
        
        ttk.Label(chart_frame, text="Equity Curve:").pack(anchor="w", padx=2, pady=2)
        
        # Create matplotlib figure with modern style
        plt.style.use('seaborn-v0_8-darkgrid')
        self.fig, self.ax = plt.subplots(figsize=(8, 3.5), dpi=100)
        self.fig.patch.set_facecolor('#f8f9fa')
        self.ax.set_facecolor('#ffffff')
        self.equity_canvas = None  # Canvas for equity curve
        self.chart_canvas = None  # Canvas for candlestick chart
        self.chart_toolbar = None  # Matplotlib toolbar for interactivity
        
        self.chart_frame = chart_frame
        self.right_frame = right_frame

    def _build_chart_tab(self, parent_frame: ttk.Frame) -> None:
        """Build the charts tab with symbol selector and candlestick chart."""
        # Top control frame with collapse button
        top_frame = ttk.Frame(parent_frame)
        top_frame.pack(side=tk.TOP, fill=tk.X, padx=5, pady=5)
        
        # Collapse/Expand button
        self.chart_controls_visible = True
        self.chart_collapse_btn = ttk.Button(top_frame, text="−", width=2, command=self._toggle_chart_controls)
        self.chart_collapse_btn.pack(side=tk.LEFT, padx=(0, 5))
        
        # Symbol selector
        ttk.Label(top_frame, text="Symbol:").pack(side=tk.LEFT, padx=(0, 5))
        self.chart_symbol_var = tk.StringVar()
        self.chart_symbol_combo = ttk.Combobox(top_frame, textvariable=self.chart_symbol_var, state="normal", width=15)
        self.chart_symbol_combo.pack(side=tk.LEFT, padx=(0, 10))
        self.chart_symbol_combo.bind("<<ComboboxSelected>>", self.on_chart_symbol_selected)
        self.chart_symbol_combo.bind("<Return>", self._validate_and_select_symbol)
        self.chart_symbol_combo.bind("<KeyRelease>", self._auto_capitalize_symbol)

        # Download/Refresh button
        self.chart_download_btn = ttk.Button(top_frame, text="Download Data", command=self.on_download_price_data)
        self.chart_download_btn.pack(side=tk.LEFT, padx=(0, 5))

        # Status label
        self.chart_status_var = tk.StringVar(value="Ready")
        status_label = ttk.Label(top_frame, textvariable=self.chart_status_var, foreground="cyan")
        status_label.pack(side=tk.LEFT, padx=(0, 5))
        
        # Control frame for indicators (will be shown/hidden)
        self.chart_controls_frame = ttk.Frame(parent_frame)
        self.chart_controls_frame.pack(side=tk.TOP, fill=tk.X, padx=5, pady=5)

        # Create paned window for chart and trades list
        self.chart_paned = ttk.PanedWindow(parent_frame, orient=tk.VERTICAL)
        self.chart_paned.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Chart display area (frame for matplotlib canvas)
        self.chart_display_frame = ttk.Frame(self.chart_paned)
        self.chart_paned.add(self.chart_display_frame, weight=3)

        # Trades list frame
        trades_frame = ttk.Frame(self.chart_paned)
        self.chart_paned.add(trades_frame, weight=1)
        
        ttk.Label(trades_frame, text="Trades for Symbol:").pack(anchor="w", padx=5, pady=2)
        
        # Create treeview for trades in chart tab
        self.chart_trades_tree = ttk.Treeview(trades_frame, columns=("account", "entry", "entry_price", "exit", "exit_price", "qty", "pnl", "pnl_pct"), show="headings", height=6)
        for col, heading in [("account", "Account"), ("entry", "Entry"), ("entry_price", "Entry Price"), ("exit", "Exit"), ("exit_price", "Exit Price"), ("qty", "Qty"), ("pnl", "P&L"), ("pnl_pct", "P&L %")]:
            self.chart_trades_tree.heading(col, text=heading)
            self.chart_trades_tree.column(col, width=70, anchor=tk.CENTER if col in ("qty", "pnl_pct") else tk.E)
        
        vsb = ttk.Scrollbar(trades_frame, orient="vertical", command=self.chart_trades_tree.yview)
        self.chart_trades_tree.configure(yscrollcommand=vsb.set)
        self.chart_trades_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True, padx=5, pady=2)
        vsb.pack(side=tk.RIGHT, fill="y")

    def _auto_capitalize_symbol(self, event=None) -> None:
        """Auto-capitalize symbol input as user types."""
        current = self.chart_symbol_var.get()
        if current != current.upper():
            # Get cursor position
            cursor_pos = self.chart_symbol_combo.index(tk.INSERT)
            self.chart_symbol_var.set(current.upper())
            self.chart_symbol_combo.icursor(cursor_pos)

    def _toggle_chart_controls(self) -> None:
        """Toggle visibility of chart control frame."""
        if self.chart_controls_visible:
            self.chart_controls_frame.pack_forget()
            self.chart_collapse_btn.config(text="+")
            self.chart_controls_visible = False
        else:
            # Re-pack above the chart paned window (pack_forget() loses original order)
            before_widget = getattr(self, "chart_paned", None)
            if before_widget is not None:
                self.chart_controls_frame.pack(side=tk.TOP, fill=tk.X, padx=5, pady=5, before=before_widget)
            else:
                self.chart_controls_frame.pack(side=tk.TOP, fill=tk.X, padx=5, pady=5)
            self.chart_collapse_btn.config(text="−")
            self.chart_controls_visible = True

    def _toggle_top_controls(self) -> None:
        """Toggle visibility of top controls section."""

    def _toggle_top_controls(self) -> None:
        """Toggle visibility of top controls section."""
        if self.top_controls_visible:
            # Hide all controls except the collapse button
            for child in self.top_controls_frame.winfo_children():
                if child != self.top_collapse_btn:
                    child.grid_remove()
            self.top_collapse_btn.config(text="+")
            self.top_controls_visible = False
        else:
            # Show all controls
            for child in self.top_controls_frame.winfo_children():
                child.grid()
            self.top_collapse_btn.config(text="−")
            self.top_controls_visible = True

    def _populate_chart_trades_list(self, symbol: str) -> None:
        """Populate the trades list in the chart tab for the selected symbol."""
        # Clear existing items
        for item in self.chart_trades_tree.get_children():
            self.chart_trades_tree.delete(item)
        
        # Get trades for this symbol (case-insensitive)
        trades_for_symbol = [t for t in self.model.trades if t.symbol.upper() == symbol.upper()]
        
        # Sort by entry date
        trades_for_symbol.sort(key=lambda t: t.entry_date)
        
        # Insert trades into the tree
        for trade in trades_for_symbol:
            account_str = trade.account_number or ""
            entry_date_str = trade.entry_date.strftime("%Y-%m-%d")
            exit_date_str = trade.exit_date.strftime("%Y-%m-%d") if trade.exit_date else ""
            entry_price_str = f"{trade.entry_price:.2f}"
            exit_price_str = f"{trade.exit_price:.2f}" if trade.exit_price else ""
            qty_str = f"{trade.quantity:.2f}"
            pnl_str = f"{trade.pnl:.2f}" if trade.pnl is not None else ""
            pnl_pct_str = f"{trade.pnl_pct:.2f}%" if trade.pnl_pct is not None else ""
            
            self.chart_trades_tree.insert("", "end", values=(account_str, entry_date_str, entry_price_str, exit_date_str, exit_price_str, qty_str, pnl_str, pnl_pct_str))

    def _update_trades_table_for_zoom(self, trades_for_symbol: list, ohlc_df: pd.DataFrame, xlim: tuple) -> None:
        """Filter trades table to show only trades within the current zoom bounds."""
        # Clear existing items
        for item in self.chart_trades_tree.get_children():
            self.chart_trades_tree.delete(item)
        
        # Convert x-axis limits to date indices
        x_min, x_max = int(xlim[0]), int(xlim[1])
        
        # Get dates corresponding to the visible x-axis range
        visible_trades = []
        for trade in trades_for_symbol:
            try:
                # Check if entry date is within zoom range
                entry_idx = (ohlc_df.index.date == trade.entry_date.date()).argmax()
                exit_idx = None
                if trade.exit_date:
                    exit_idx = (ohlc_df.index.date == trade.exit_date.date()).argmax()
                
                # Include trade if either entry or exit is in the visible range
                if (x_min <= entry_idx <= x_max) or (exit_idx is not None and x_min <= exit_idx <= x_max):
                    visible_trades.append(trade)
            except Exception:
                pass
        
        # Sort by entry date
        visible_trades.sort(key=lambda t: t.entry_date)
        
        # Insert filtered trades into the tree
        for trade in visible_trades:
            account_str = trade.account_number or ""
            entry_date_str = trade.entry_date.strftime("%Y-%m-%d")
            exit_date_str = trade.exit_date.strftime("%Y-%m-%d") if trade.exit_date else ""
            entry_price_str = f"{trade.entry_price:.2f}"
            exit_price_str = f"{trade.exit_price:.2f}" if trade.exit_price else ""
            qty_str = f"{trade.quantity:.2f}"
            pnl_str = f"{trade.pnl:.2f}" if trade.pnl is not None else ""
            pnl_pct_str = f"{trade.pnl_pct:.2f}%" if trade.pnl_pct is not None else ""
            
            self.chart_trades_tree.insert("", "end", values=(account_str, entry_date_str, entry_price_str, exit_date_str, exit_price_str, qty_str, pnl_str, pnl_pct_str))

    def _pick_color(self, color_var: tk.StringVar, on_color_change=None) -> None:
        """Open color picker dialog and update a variable + optional UI callback."""
        from tkinter import colorchooser
        # Get current color - convert to hex if it's a named color
        current_color = name_to_hex(color_var.get())
        color = colorchooser.askcolor(title="Pick a color", color=current_color)
        if color[1]:  # color[1] is the hex value
            color_var.set(color[1])
            if callable(on_color_change):
                try:
                    on_color_change(color[1])
                except Exception:
                    pass

    def _validate_and_select_symbol(self, event=None) -> None:
        """Validate typed symbol and match to journal symbols on Enter key."""
        typed = self.chart_symbol_var.get().upper()
        available = [t.symbol for t in self.model.trades]
        available_upper = [s.upper() for s in available]
        
        if typed in available_upper:
            # Find the original casing
            idx = available_upper.index(typed)
            self.chart_symbol_var.set(available[idx])
            self.on_chart_symbol_selected()
        else:
            messagebox.showerror("Invalid Symbol", 
                               f"'{typed}' not found in trade journal.\n\nAvailable symbols: {', '.join(available)}")
            self.chart_symbol_var.set("")

    def on_chart_symbol_selected(self, event: Optional[tk.Event] = None) -> None:
        """Handle symbol selection in the chart tab."""
        symbol = self.chart_symbol_var.get()
        if symbol:
            self.current_chart_symbol = symbol
            # Check if data exists for this symbol
            if self.price_manager.has_data(symbol):
                # Load cached data
                self.display_candlestick_chart(symbol)
            else:
                self.chart_status_var.set(f"No data for {symbol}. Click 'Download Data' to fetch.")

    def on_download_price_data(self) -> None:
        """Handle downloading price data for the selected symbol."""
        symbol = self.chart_symbol_var.get()
        if not symbol:
            messagebox.showwarning("No Symbol", "Please select a symbol first.")
            return
        
        if not HAS_YFINANCE:
            messagebox.showerror("Missing Dependency", 
                               "yfinance is required. Install with: pip install yfinance")
            return

        # Find trades for this symbol to determine date range (case-insensitive)
        trades_for_symbol = [t for t in self.model.trades if t.symbol.upper() == symbol.upper()]
        if not trades_for_symbol:
            messagebox.showwarning("No Trades", f"No trades found for {symbol}\n\nAvailable symbols: {', '.join(sorted(set(t.symbol for t in self.model.trades)))}")
            return

        # Determine date range: 3 months before first trade, to current date or 3 months after last exit
        first_entry = min(t.entry_date for t in trades_for_symbol)
        last_exit = max((t.exit_date for t in trades_for_symbol if t.exit_date), default=None)
        
        start_date = (first_entry - dt.timedelta(days=90)).date()
        if last_exit:
            end_date = min((last_exit + dt.timedelta(days=90)).date(), dt.date.today())
        else:
            end_date = dt.date.today()

        # Show progress message with date range
        self.chart_status_var.set(f"Fetching {symbol} ({start_date} to {end_date})...")
        self.root.update()

        try:
            # Fetch and store data
            print(f"\n=== Fetching {symbol} data ===")
            print(f"Date range: {start_date} to {end_date}")
            df = self.price_manager.fetch_and_store(symbol, start_date, end_date)
            if df is not None and not df.empty:
                self.chart_status_var.set(f"✓ Loaded {len(df)} days for {symbol}")
                # Display the chart
                self.display_candlestick_chart(symbol)
            else:
                self.chart_status_var.set(f"✗ No data found for {symbol}")
                messagebox.showinfo("No Data", f"yfinance returned no data for {symbol}.\n\nThis could mean:\n- Symbol doesn't exist or is delisted\n- No trading data available for the date range\n- Network issue\n\nCheck your spelling or try a different symbol.")
        except Exception as e:
            error_msg = str(e)
            self.chart_status_var.set(f"✗ Error: {error_msg[:50]}")
            messagebox.showerror("Download Error", f"Failed to download data:\n\n{error_msg}")
            print(f"\nERROR: {error_msg}")
            import traceback
            traceback.print_exc()

    def display_candlestick_chart(self, symbol: str) -> None:
        """Display an interactive candlestick chart for the given symbol with trade annotations and technical indicators."""
        if not HAS_MPLFINANCE:
            messagebox.showerror("Missing Dependency",
                               "mplfinance is required. Install with: pip install mplfinance")
            return

        try:
            # Get price data
            metadata = self.price_manager.get_metadata(symbol)
            if metadata:
                start_date = dt.datetime.fromisoformat(metadata['start_date']).date()
                end_date = dt.datetime.fromisoformat(metadata['end_date']).date()
            else:
                start_date = dt.date.today() - dt.timedelta(days=180)
                end_date = dt.date.today()

            df = self.price_manager.get_price_data(symbol, start_date, end_date)
            if df is None or df.empty:
                self.chart_status_var.set(f"No price data available for {symbol}")
                return

            # Get trades for this symbol (case-insensitive)
            trades_for_symbol = [t for t in self.model.trades if t.symbol.upper() == symbol.upper()]
            
            # Populate the trades list in the chart tab
            self._populate_chart_trades_list(symbol)

            # Clear previous canvas and controls (but keep the display_frame structure)
            for widget in self.chart_display_frame.winfo_children():
                widget.destroy()
            
            # Clear previous controls
            for widget in self.chart_controls_frame.winfo_children():
                widget.destroy()

            # Load saved settings
            saved_settings = load_chart_settings()
            
            # Create control frame for indicators in the chart_controls_frame
            ttk.Label(self.chart_controls_frame, text="Moving Averages:").pack(side=tk.LEFT, padx=5)
            
            # EMA1 controls
            ema1_enabled_var = tk.BooleanVar(value=saved_settings.get("ema1_enabled", True))
            ttk.Checkbutton(self.chart_controls_frame, text="MA1", variable=ema1_enabled_var).pack(side=tk.LEFT, padx=(10, 3))
            ema1_type_var = tk.StringVar(value=saved_settings.get("ema1_type", "EMA"))
            type1_combo = ttk.Combobox(self.chart_controls_frame, textvariable=ema1_type_var, values=["EMA", "SMA"], state="readonly", width=4)
            type1_combo.pack(side=tk.LEFT, padx=(0, 3))
            ttk.Label(self.chart_controls_frame, text="P:").pack(side=tk.LEFT, padx=(5, 0))
            ema1_var = tk.StringVar(value=str(saved_settings["ema1_period"]))
            ema1_spinbox = ttk.Spinbox(self.chart_controls_frame, from_=1, to=200, textvariable=ema1_var, width=4)
            ema1_spinbox.pack(side=tk.LEFT, padx=(0, 3))
            
            ema1_color_var = tk.StringVar(value=saved_settings["ema1_color"])
            ema1_hex = name_to_hex(ema1_color_var.get())
            ema1_swatch = tk.Canvas(self.chart_controls_frame, width=20, height=12, highlightthickness=1, highlightbackground="#888")
            ema1_rect = ema1_swatch.create_rectangle(0, 0, 20, 12, fill=ema1_hex, outline=ema1_hex)
            ema1_swatch.pack(side=tk.LEFT, padx=(0, 2))
            ttk.Button(
                self.chart_controls_frame,
                text="C",
                width=1,
                command=lambda: self._pick_color(
                    ema1_color_var,
                    on_color_change=lambda hx: ema1_swatch.itemconfig(ema1_rect, fill=hx, outline=hx)
                )
            ).pack(side=tk.LEFT, padx=(0, 8))
            
            # EMA2 controls
            ema2_enabled_var = tk.BooleanVar(value=saved_settings.get("ema2_enabled", True))
            ttk.Checkbutton(self.chart_controls_frame, text="MA2", variable=ema2_enabled_var).pack(side=tk.LEFT, padx=(10, 3))
            ema2_type_var = tk.StringVar(value=saved_settings.get("ema2_type", "EMA"))
            type2_combo = ttk.Combobox(self.chart_controls_frame, textvariable=ema2_type_var, values=["EMA", "SMA"], state="readonly", width=4)
            type2_combo.pack(side=tk.LEFT, padx=(0, 3))
            ttk.Label(self.chart_controls_frame, text="P:").pack(side=tk.LEFT, padx=(5, 0))
            ema2_var = tk.StringVar(value=str(saved_settings["ema2_period"]))
            ema2_spinbox = ttk.Spinbox(self.chart_controls_frame, from_=1, to=200, textvariable=ema2_var, width=4)
            ema2_spinbox.pack(side=tk.LEFT, padx=(0, 3))
            
            ema2_color_var = tk.StringVar(value=saved_settings["ema2_color"])
            ema2_hex = name_to_hex(ema2_color_var.get())
            ema2_swatch = tk.Canvas(self.chart_controls_frame, width=20, height=12, highlightthickness=1, highlightbackground="#888")
            ema2_rect = ema2_swatch.create_rectangle(0, 0, 20, 12, fill=ema2_hex, outline=ema2_hex)
            ema2_swatch.pack(side=tk.LEFT, padx=(0, 2))
            ttk.Button(
                self.chart_controls_frame,
                text="C",
                width=1,
                command=lambda: self._pick_color(
                    ema2_color_var,
                    on_color_change=lambda hx: ema2_swatch.itemconfig(ema2_rect, fill=hx, outline=hx)
                )
            ).pack(side=tk.LEFT, padx=(0, 8))
            
            # EMA3 controls
            ema3_enabled_var = tk.BooleanVar(value=saved_settings.get("ema3_enabled", False))
            ttk.Checkbutton(self.chart_controls_frame, text="MA3", variable=ema3_enabled_var).pack(side=tk.LEFT, padx=(10, 3))
            ema3_type_var = tk.StringVar(value=saved_settings.get("ema3_type", "EMA"))
            type3_combo = ttk.Combobox(self.chart_controls_frame, textvariable=ema3_type_var, values=["EMA", "SMA"], state="readonly", width=4)
            type3_combo.pack(side=tk.LEFT, padx=(0, 3))
            ttk.Label(self.chart_controls_frame, text="P:").pack(side=tk.LEFT, padx=(5, 0))
            ema3_var = tk.StringVar(value=str(saved_settings["ema3_period"]))
            ema3_spinbox = ttk.Spinbox(self.chart_controls_frame, from_=1, to=200, textvariable=ema3_var, width=4)
            ema3_spinbox.pack(side=tk.LEFT, padx=(0, 3))
            
            ema3_color_var = tk.StringVar(value=saved_settings["ema3_color"])
            ema3_hex = name_to_hex(ema3_color_var.get())
            ema3_swatch = tk.Canvas(self.chart_controls_frame, width=20, height=12, highlightthickness=1, highlightbackground="#888")
            ema3_rect = ema3_swatch.create_rectangle(0, 0, 20, 12, fill=ema3_hex, outline=ema3_hex)
            ema3_swatch.pack(side=tk.LEFT, padx=(0, 2))
            ttk.Button(
                self.chart_controls_frame,
                text="C",
                width=1,
                command=lambda: self._pick_color(
                    ema3_color_var,
                    on_color_change=lambda hx: ema3_swatch.itemconfig(ema3_rect, fill=hx, outline=hx)
                )
            ).pack(side=tk.LEFT, padx=(0, 8))
            
            # Update button with save functionality
            def update_and_save():
                self._update_chart_indicators(symbol, df, trades_for_symbol, 
                                             ema1_var, ema2_var, ema3_var,
                                             ema1_type_var, ema2_type_var, ema3_type_var,
                                             ema1_color_var, ema2_color_var, ema3_color_var,
                                             ema1_enabled_var, ema2_enabled_var, ema3_enabled_var)
                # Save settings
                save_chart_settings(
                    {
                        "ema1_period": int(ema1_var.get()),
                        "ema2_period": int(ema2_var.get()),
                        "ema3_period": int(ema3_var.get()),
                        "ema1_color": ema1_color_var.get(),
                        "ema2_color": ema2_color_var.get(),
                        "ema3_color": ema3_color_var.get(),
                        "ema1_type": ema1_type_var.get(),
                        "ema2_type": ema2_type_var.get(),
                        "ema3_type": ema3_type_var.get(),
                        "ema1_enabled": ema1_enabled_var.get(),
                        "ema2_enabled": ema2_enabled_var.get(),
                        "ema3_enabled": ema3_enabled_var.get(),
                    }
                )
            
            ttk.Button(self.chart_controls_frame, text="Update", command=update_and_save).pack(side=tk.LEFT, padx=5)

            # Build candlestick chart with initial values
            self._plot_candlestick_with_indicators(symbol, df, trades_for_symbol, 
                                                   int(ema1_var.get()), int(ema2_var.get()), int(ema3_var.get()),
                                                   ema1_type_var.get(), ema2_type_var.get(), ema3_type_var.get(),
                                                   ema1_color_var.get(), ema2_color_var.get(), ema3_color_var.get(),
                                                   ema1_enabled_var.get(), ema2_enabled_var.get(), ema3_enabled_var.get())

            self.chart_status_var.set(f"Interactive chart for {symbol} - Adjust moving averages and click Update")

        except Exception as e:
            error_msg = str(e)
            self.chart_status_var.set(f"Error displaying chart: {error_msg}")
            messagebox.showerror("Chart Error", f"Failed to display chart:\n\n{error_msg}")
            print(f"\nERROR: {error_msg}")
            import traceback
            traceback.print_exc()

    def _update_chart_indicators(self, symbol: str, df: pd.DataFrame, trades_for_symbol: list,
                                 ema1_var: tk.StringVar, ema2_var: tk.StringVar, ema3_var: tk.StringVar,
                                 ema1_type_var: tk.StringVar, ema2_type_var: tk.StringVar, ema3_type_var: tk.StringVar,
                                 ema1_color_var: tk.StringVar, ema2_color_var: tk.StringVar, ema3_color_var: tk.StringVar,
                                 ema1_enabled_var: tk.BooleanVar, ema2_enabled_var: tk.BooleanVar, ema3_enabled_var: tk.BooleanVar) -> None:
        """Update chart when moving average periods, types, colors, or visibility change."""
        try:
            ema1 = int(ema1_var.get())
            ema2 = int(ema2_var.get())
            ema3 = int(ema3_var.get())
            if ema1 < 1 or ema2 < 1 or ema3 < 1 or ema1 > 200 or ema2 > 200 or ema3 > 200:
                raise ValueError("Moving average periods must be between 1 and 200")
        except ValueError as e:
            messagebox.showerror("Invalid Input", f"Moving average periods must be numbers between 1 and 200: {e}")
            return
        
        # Destroy old canvas and toolbar references
        if self.chart_canvas:
            try:
                self.chart_canvas.get_tk_widget().destroy()
            except Exception:
                pass
            self.chart_canvas = None
        
        # Completely destroy all children in chart_display_frame
        for widget in list(self.chart_display_frame.winfo_children()):
            try:
                widget.destroy()
            except Exception:
                pass
        
        # Recreate the chart
        self._plot_candlestick_with_indicators(symbol, df, trades_for_symbol, 
                                               ema1, ema2, ema3,
                                               ema1_type_var.get(), ema2_type_var.get(), ema3_type_var.get(),
                                               ema1_color_var.get(), ema2_color_var.get(), ema3_color_var.get(),
                                               ema1_enabled_var.get(), ema2_enabled_var.get(), ema3_enabled_var.get())

    def _plot_candlestick_with_indicators(self, symbol: str, df: pd.DataFrame, trades_for_symbol: list,
                                          ema1_period: int, ema2_period: int, ema3_period: int,
                                          ema1_type: str = "EMA", ema2_type: str = "EMA", ema3_type: str = "EMA",
                                          ema1_color: str = "blue", ema2_color: str = "orange", ema3_color: str = "purple",
                                          ema1_enabled: bool = True, ema2_enabled: bool = True, ema3_enabled: bool = False) -> None:
        """Create and display the candlestick chart with selected indicators."""
        # Rename columns to match mplfinance expectations
        ohlc_df = df[['open', 'high', 'low', 'close']].copy()
        ohlc_df.columns = ['Open', 'High', 'Low', 'Close']
        if 'volume' in df.columns:
            ohlc_df['Volume'] = df['volume']

        # Calculate moving averages with user-specified periods and types
        def calculate_moving_average(close_series, period: int, ma_type: str):
            if ma_type == "SMA":
                return close_series.rolling(window=period).mean()
            else:  # EMA
                return close_series.ewm(span=period, adjust=False).mean()
        
        ema1 = calculate_moving_average(df['close'], ema1_period, ema1_type)
        ema2 = calculate_moving_average(df['close'], ema2_period, ema2_type)
        ema3 = calculate_moving_average(df['close'], ema3_period, ema3_type)

        # Create candlestick figure with additional plots
        apds = []

        # Add moving averages as overlays with custom colors (only if enabled)
        ema1_plot_color = name_to_hex(ema1_color)
        ema2_plot_color = name_to_hex(ema2_color)
        ema3_plot_color = name_to_hex(ema3_color)
        
        if ema1_enabled:
            apds.append(mpf.make_addplot(ema1, color=ema1_plot_color, width=1.5, secondary_y=False))
        if ema2_enabled:
            apds.append(mpf.make_addplot(ema2, color=ema2_plot_color, width=1.5, secondary_y=False))
        if ema3_enabled:
            apds.append(mpf.make_addplot(ema3, color=ema3_plot_color, width=1.5, secondary_y=False))
        
        # Add buy signals (green arrows at low)
        buy_trades = [t for t in trades_for_symbol if t.entry_date.date() in df.index.date]
        if buy_trades:
            buy_dots = [np.nan] * len(ohlc_df)
            for t in buy_trades:
                try:
                    idx = (ohlc_df.index.date == t.entry_date.date()).argmax()
                    if idx < len(buy_dots):
                        buy_dots[idx] = ohlc_df.iloc[idx]['Low'] * 0.99  # Slightly below the low
                except Exception:
                    pass
            apds.append(mpf.make_addplot(buy_dots, type='scatter', marker='^', markersize=100, color='green'))

        # Add sell signals (red arrows at high)
        sell_trades = [t for t in trades_for_symbol if t.exit_date and t.exit_date.date() in df.index.date]
        if sell_trades:
            sell_dots = [np.nan] * len(ohlc_df)
            for t in sell_trades:
                try:
                    idx = (ohlc_df.index.date == t.exit_date.date()).argmax()
                    if idx < len(sell_dots):
                        sell_dots[idx] = ohlc_df.iloc[idx]['High'] * 1.01  # Slightly above the high
                except Exception:
                    pass
            apds.append(mpf.make_addplot(sell_dots, type='scatter', marker='v', markersize=100, color='red'))

        # Build title with symbol only
        title_text = f"{symbol} Price Chart"

        # Create the plot
        fig, axes = mpf.plot(
            ohlc_df,
            type='candle',
            addplot=apds if apds else None,
            volume=False,
            style='yahoo',
            title=title_text,
            ylabel='Price',
            figsize=(12, 6),
            returnfig=True
        )
        
        # Configure matplotlib for better zoom rectangle visibility
        # Set a custom color for the zoom rectangle selector
        fig.patch.set_facecolor('#ffffff')  # White figure background
        
        # Add custom legend for moving averages
        ax = axes[0]
        # Create custom legend entries for moving averages (only those enabled)
        from matplotlib.lines import Line2D
        legend_elements = []
        
        if ema1_enabled:
            ema1_color_hex = name_to_hex(ema1_color)
            legend_elements.append(Line2D([0], [0], color=ema1_color_hex, lw=2, label=f'{ema1_type} {ema1_period}'))
        if ema2_enabled:
            ema2_color_hex = name_to_hex(ema2_color)
            legend_elements.append(Line2D([0], [0], color=ema2_color_hex, lw=2, label=f'{ema2_type} {ema2_period}'))
        if ema3_enabled:
            ema3_color_hex = name_to_hex(ema3_color)
            legend_elements.append(Line2D([0], [0], color=ema3_color_hex, lw=2, label=f'{ema3_type} {ema3_period}'))
        
        legend_elements.extend([
            Line2D([0], [0], marker='^', color='w', markerfacecolor='green', markersize=8, label='Buy'),
            Line2D([0], [0], marker='v', color='w', markerfacecolor='red', markersize=8, label='Sell'),
        ])
        ax.legend(handles=legend_elements, loc='upper left', framealpha=0.95)

        # Embed in tkinter with toolbar for interactivity
        if self.chart_canvas:
            self.chart_canvas.get_tk_widget().destroy()
        if hasattr(self, 'chart_toolbar') and self.chart_toolbar:
            try:
                self.chart_toolbar.destroy()
            except Exception:
                pass

        from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg, NavigationToolbar2Tk
        
        canvas_frame = ttk.Frame(self.chart_display_frame)
        canvas_frame.pack(fill=tk.BOTH, expand=True)
        
        # Toolbar frame (use ttk widgets; macOS renders these reliably)
        toolbar_frame = ttk.Frame(canvas_frame)
        toolbar_frame.pack(side=tk.TOP, fill=tk.X)
        
        self.chart_canvas = FigureCanvasTkAgg(fig, master=canvas_frame)

        # Create a real Matplotlib toolbar (hidden) so that pan/zoom/home/back/save actually work.
        # We keep a text-only toolbar UI for macOS compatibility.
        try:
            self.chart_toolbar = NavigationToolbar2Tk(self.chart_canvas, toolbar_frame, pack_toolbar=False)
        except TypeError:
            # Older Matplotlib versions don't support pack_toolbar
            self.chart_toolbar = NavigationToolbar2Tk(self.chart_canvas, toolbar_frame)
            try:
                self.chart_toolbar.pack_forget()
            except Exception:
                pass
        try:
            self.chart_toolbar.update()
        except Exception:
            pass
        # Make sure Matplotlib can find the toolbar from the canvas object.
        try:
            self.chart_canvas.toolbar = self.chart_toolbar
            self.chart_canvas.figure.canvas.toolbar = self.chart_toolbar
        except Exception:
            pass

        # Text-only toolbar (avoids the white-block icon issue on macOS themes)
        text_toolbar = ttk.Frame(toolbar_frame)
        text_toolbar.pack(side=tk.LEFT, fill=tk.X, expand=True)

        def _call_toolbar(method_name: str) -> None:
            tb = getattr(self, 'chart_toolbar', None)
            if not tb:
                return
            try:
                getattr(tb, method_name)()
            except Exception as e:
                print(f"Toolbar action '{method_name}' failed: {e}")

        ttk.Button(text_toolbar, text="Home", command=lambda: _call_toolbar('home')).pack(side=tk.LEFT, padx=2)
        ttk.Button(text_toolbar, text="Back", command=lambda: _call_toolbar('back')).pack(side=tk.LEFT, padx=2)
        ttk.Button(text_toolbar, text="Forward", command=lambda: _call_toolbar('forward')).pack(side=tk.LEFT, padx=2)
        ttk.Button(text_toolbar, text="Pan", command=lambda: _call_toolbar('pan')).pack(side=tk.LEFT, padx=2)
        ttk.Button(text_toolbar, text="Zoom", command=lambda: _call_toolbar('zoom')).pack(side=tk.LEFT, padx=2)
        ttk.Button(text_toolbar, text="Save", command=lambda: _call_toolbar('save_figure')).pack(side=tk.LEFT, padx=2)
        
        # Add quantity labels to buy/sell arrows (with stacking for multiple sells/buys on same day)
        ax = axes[0]
        
        # Store zoom bounds for later filtering
        self.chart_zoom_xlim = None
        self.chart_zoom_ylim = None
        
        def update_quantity_labels():
            """Update quantity labels based on current axis limits, stacking multiple trades on same day."""
            # Remove old quantity text labels
            for text in ax.texts[::]:  # Iterate through a copy
                if hasattr(text, '_quantity_label'):
                    text.remove()
            
            # Get current axis limits
            xlim = ax.get_xlim()
            ylim = ax.get_ylim()
            
            # Group buy trades by entry date and create stacked labels
            buy_by_date = {}
            for t in buy_trades:
                date_key = t.entry_date.date()
                if date_key not in buy_by_date:
                    buy_by_date[date_key] = []
                buy_by_date[date_key].append(t)
            
            # Add buy quantity labels with stacking (only if within current zoom bounds)
            for date_key, trades in buy_by_date.items():
                try:
                    idx = (ohlc_df.index.date == date_key).argmax()
                    if idx < len(ohlc_df) and xlim[0] <= idx <= xlim[1]:
                        low_price = ohlc_df.iloc[idx]['Low']
                        base_y_pos = low_price * 0.92  # Position further from arrow
                        # Stack labels with larger vertical offsets for clarity
                        for i, t in enumerate(trades):
                            y_offset = i * (low_price * 0.08)  # Larger offset for better spacing
                            y_pos = base_y_pos - y_offset
                            label = ax.text(idx, y_pos, f"{int(t.quantity)}", color='green', ha='center', va='top', fontsize=7, fontweight='bold')
                            label._quantity_label = True
                except Exception:
                    pass
            
            # Group sell trades by exit date and create stacked labels
            sell_by_date = {}
            for t in sell_trades:
                date_key = t.exit_date.date()
                if date_key not in sell_by_date:
                    sell_by_date[date_key] = []
                sell_by_date[date_key].append(t)
            
            # Add sell quantity labels with stacking (only if within current zoom bounds)
            for date_key, trades in sell_by_date.items():
                try:
                    idx = (ohlc_df.index.date == date_key).argmax()
                    if idx < len(ohlc_df) and xlim[0] <= idx <= xlim[1]:
                        high_price = ohlc_df.iloc[idx]['High']
                        base_y_pos = high_price * 1.08  # Position further from arrow
                        # Stack labels with larger vertical offsets for clarity
                        for i, t in enumerate(trades):
                            y_offset = i * (high_price * 0.08)  # Larger offset for better spacing
                            y_pos = base_y_pos + y_offset
                            label = ax.text(idx, y_pos, f"{int(t.quantity)}", color='red', ha='center', va='bottom', fontsize=7, fontweight='bold')
                            label._quantity_label = True
                except Exception:
                    pass
        
        # Initial label placement
        update_quantity_labels()
        
        # Store the update function for later use when zoom changes
        self.update_quantity_labels = update_quantity_labels
        
        # Add event handler for zoom/pan changes
        def on_limits_change(event_ax):
            """Handle axis limit changes (zoom/pan)."""
            try:
                # Update quantity labels based on new zoom level
                update_quantity_labels()
                
                # Update trades table to show only visible trades
                xlim = ax.get_xlim()
                self._update_trades_table_for_zoom(trades_for_symbol, ohlc_df, xlim)
                
                # Redraw canvas
                self.chart_canvas.draw_idle()
            except Exception as e:
                print(f"Error updating labels/table: {e}")
        
        ax.callbacks.connect('xlim_changed', on_limits_change)
        ax.callbacks.connect('ylim_changed', on_limits_change)
        
        self.chart_canvas.draw()
        self.chart_canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)

    def update_chart_symbols(self) -> None:
        """Update the list of available symbols in the chart tab."""
        symbols = sorted(set(t.symbol for t in self.model.trades))
        self.chart_symbol_combo['values'] = symbols
        if symbols and not self.chart_symbol_var.get():
            self.chart_symbol_var.set(symbols[0])



    def load_csv(self) -> None:
        """Prompt the user to select a CSV file and load it."""
        filetypes = [("CSV files", "*.csv"), ("All files", "*.*")]
        filepath = filedialog.askopenfilename(title="Open CSV", filetypes=filetypes)
        if not filepath:
            return
        try:
            # Load new transactions and skip duplicates based on persisted state
            prev_count = len(self.model.transactions)
            self.model.load_csv(filepath)
            dupes = getattr(self.model, 'duplicate_count', 0)
        except Exception as e:
            messagebox.showerror("Error", str(e))
            return
        # Populate account filter options
        acct_numbers = sorted({tx.account_number for tx in self.model.transactions})
        self.account_dropdown["values"] = ["all"] + acct_numbers
        self.account_dropdown.set("all")
        # Populate table and update summary
        self.populate_table()
        self.update_summary_and_chart()
        # Update chart tab symbol list
        self.update_chart_symbols()
        # Inform user about duplicates
        try:
            if dupes:
                # Show a summary message and detailed view of duplicates
                messagebox.showinfo("Duplicates Skipped", f"{dupes} duplicate transactions were skipped.")
                # Present the details of duplicates in a separate window
                self.show_duplicate_transactions()
        except Exception:
            pass

    def toggle_table_visibility(self) -> None:
        """Toggle the visibility of the table pane."""
        if self.table_visible.get():
            # Hide table - remove from left_paned
            self.left_paned.remove(self.table_paned)
            self.table_visible.set(False)
            self.toggle_btn.config(text="Show Table")
        else:
            # Show table - re-add to left_paned at position 0
            self.left_paned.insert(0, self.table_paned, weight=2)
            self.table_visible.set(True)
            self.toggle_btn.config(text="Hide Table")
        # Populate table and update summary
        self.populate_table()
        self.update_summary_and_chart()

    def autofit_columns(self) -> None:
        """Auto-fit table columns to content width."""
        import tkinter.font as tkFont
        
        try:
            # Get the font used by the treeview
            try:
                font = tkFont.nametofont(self.tree.cget("font") or "TkDefaultFont")
            except:
                font = tkFont.nametofont("TkDefaultFont")
            
            padding = 20  # Extra padding for column widths
            
            # Column mapping for header names
            columns = (
                "account", "symbol", "entry_date", "entry_price", "exit_date",
                "exit_price", "quantity", "pnl", "pnl_pct", "hold_period", "screenshot", "note"
            )
            
            # Headers mapping
            headers = {
                "account": "Account",
                "symbol": "Symbol",
                "entry_date": "Entry Date",
                "entry_price": "Entry Price",
                "exit_date": "Exit Date",
                "exit_price": "Exit Price",
                "quantity": "Quantity",
                "pnl": "Pnl",
                "pnl_pct": "Pnl %",
                "hold_period": "Hold Period",
                "screenshot": "Screenshot",
                "note": "Note",
            }
            
            for col in columns:
                max_width = font.measure(headers[col]) + padding
                
                # Get all items (including group headers if applicable)
                for item in self.tree.get_children():
                    # Check if this is a group item with children
                    children = self.tree.get_children(item)
                    if children:
                        # It's a group, get its cell value
                        try:
                            values = self.tree.item(item, "values")
                            if values and len(values) > columns.index(col):
                                cell_text = str(values[columns.index(col)])
                                width = font.measure(cell_text) + padding
                                max_width = max(max_width, width)
                        except:
                            pass
                        # Process children
                        for child in children:
                            try:
                                values = self.tree.item(child, "values")
                                if values and len(values) > columns.index(col):
                                    cell_text = str(values[columns.index(col)])
                                    width = font.measure(cell_text) + padding
                                    max_width = max(max_width, width)
                            except:
                                pass
                    else:
                        # It's a regular item
                        try:
                            values = self.tree.item(item, "values")
                            if values and len(values) > columns.index(col):
                                cell_text = str(values[columns.index(col)])
                                width = font.measure(cell_text) + padding
                                max_width = max(max_width, width)
                        except:
                            pass
                
                # Apply a reasonable minimum and maximum width
                max_width = max(40, min(max_width, 250))
                self.tree.column(col, width=max_width)
            
            # Auto-fit tree column (#0) if in group mode
            if self.tree.cget("show") == "tree headings":
                max_width = font.measure("Symbol") + padding
                for item in self.tree.get_children():
                    try:
                        text = self.tree.item(item, "text")
                        width = font.measure(text) + padding
                        max_width = max(max_width, width)
                    except:
                        pass
                max_width = max(40, min(max_width, 150))
                self.tree.column("#0", width=max_width)
        except Exception:
            # If autofit fails for any reason, silently continue
            pass

    def populate_table(self) -> None:
        """Insert trades into the treeview."""
        # Clear existing items and id-key mapping
        for item in self.tree.get_children():
            self.tree.delete(item)
        self.id_to_key.clear()
        # Clear group mapping for deletion
        self.group_id_to_indices.clear()
        
        # Populate strategy filter dropdowns with unique values from trades
        # Split comma-separated strategies into individual options
        def extract_individual_strategies(strategy_str: str) -> set:
            """Extract individual strategies from comma-separated string."""
            if not strategy_str:
                return set()
            return {s.strip() for s in strategy_str.split(',') if s.strip()}
        
        entry_strategies = set()
        exit_strategies = set()
        for idx, trade in enumerate(self.model.trades):
            key = self.model.compute_key(trade)
            entry_strat = self.model.entry_strategies.get(key, "")
            exit_strat = self.model.exit_strategies.get(key, "")
            if entry_strat:
                entry_strategies.update(extract_individual_strategies(entry_strat))
            if exit_strat:
                exit_strategies.update(extract_individual_strategies(exit_strat))
        
        # Update combo box values
        self.entry_strategy_filter_combo['values'] = ["all"] + sorted(list(entry_strategies))
        self.exit_strategy_filter_combo['values'] = ["all"] + sorted(list(exit_strategies))
        # Determine filters
        closed_only = self.closed_only_var.get()
        account_filter = self.account_var.get()
        group_by_symbol = self.group_var.get()
        entry_strategy_filter = self.entry_strategy_filter_var.get()
        exit_strategy_filter = self.exit_strategy_filter_var.get()
        # Determine sort parameters
        sort_by = self.sort_by
        descending = self.sort_descending

        # Helper to determine if a trade should be shown based on filters
        def parse_strategies(strategy_str: str) -> list:
            """Parse comma-separated strategies into a list, trimmed and lowercased."""
            if not strategy_str:
                return []
            return [s.strip().lower() for s in strategy_str.split(',') if s.strip()]
        
        def trade_visible(index: int, trade: TradeEntry) -> bool:
            # Apply top filter set if present
            if hasattr(self, 'top_filter_set') and self.top_filter_set is not None:
                if index not in self.top_filter_set:
                    return False
            # Account filter
            if account_filter and account_filter != "all" and trade.account_number != account_filter:
                return False
            # Entry strategy filter (supports partial matching and multiple strategies)
            if entry_strategy_filter and entry_strategy_filter != "all":
                key = self.model.compute_key(trade)
                trade_entry_strategy = self.model.entry_strategies.get(key, "")
                filter_strategies = parse_strategies(entry_strategy_filter)
                # Check if any filter strategy is contained in the trade strategy
                if not any(f in trade_entry_strategy.lower() for f in filter_strategies):
                    return False
            # Exit strategy filter (supports partial matching and multiple strategies)
            if exit_strategy_filter and exit_strategy_filter != "all":
                key = self.model.compute_key(trade)
                trade_exit_strategy = self.model.exit_strategies.get(key, "")
                filter_strategies = parse_strategies(exit_strategy_filter)
                # Check if any filter strategy is contained in the trade strategy
                if not any(f in trade_exit_strategy.lower() for f in filter_strategies):
                    return False
            # Closed-only filter
            if closed_only:
                if not trade.is_closed:
                    return False
                if trade.buy_id < 0:
                    return False
                if self.model.open_qty_by_buy_id.get(trade.buy_id, 0.0) > 1e-8:
                    return False
            # Date filter on entry_date (inclusive)
            if self.start_date and trade.entry_date.date() < self.start_date:
                return False
            if self.end_date and trade.entry_date.date() > self.end_date:
                return False
            return True

        # If grouping by symbol, switch treeview to tree mode
        if group_by_symbol:
            # Show tree column (#0) to display group labels
            self.tree.configure(show="tree headings")
            # Set a reasonable width for the tree column if not already set
            self.tree.column("#0", width=120)
            # Build grouped data
            group_map: Dict[Tuple[str, str], List[Tuple[int, TradeEntry]]] = {}
            for idx, trade in enumerate(self.model.trades):
                if not trade_visible(idx, trade):
                    continue
                key = (trade.account_number, trade.symbol)
                group_map.setdefault(key, []).append((idx, trade))
            # Build aggregated info for each group
            aggregated: List[Dict[str, object]] = []
            for (acct_num, symbol), items in group_map.items():
                trades = [t for _, t in items]
                total_qty = sum(t.quantity for t in trades)
                # Compute cost basis and proceeds; treat missing exit_price as 0
                cost_basis = sum(t.entry_price * t.quantity for t in trades)
                # Only consider closed trades for proceeds; open trades contribute 0
                proceeds = sum((t.exit_price or 0.0) * t.quantity for t in trades if t.exit_date)
                # Weighted average entry and exit prices
                avg_entry_price = cost_basis / total_qty if total_qty else 0.0
                avg_exit_price = proceeds / total_qty if total_qty else 0.0
                total_pnl = sum(t.pnl or 0.0 for t in trades)
                # Determine aggregated entry date (earliest) and exit date (latest among closed trades)
                entry_date = min(t.entry_date for t in trades)
                exit_dates = [t.exit_date for t in trades if t.exit_date]
                exit_date = max(exit_dates) if exit_dates else None
                # Weighted average hold period (consider only closed trades)
                hold_numer = sum((t.hold_period or 0) * t.quantity for t in trades if t.exit_date)
                hold_denom = sum(t.quantity for t in trades if t.exit_date)
                avg_hold = (hold_numer / hold_denom) if hold_denom else None
                # Calculate pnl_pct for the aggregated group
                total_pnl_pct = None
                if cost_basis > 1e-8:  # Avoid division by zero
                    total_pnl_pct = (total_pnl / cost_basis) * 100
                # Determine if the overall position is still open (any remaining quantity)
                open_remaining = self.model.open_qty_by_symbol.get((acct_num, symbol), 0.0) if closed_only else 0.0
                # Determine if any trade in this group has a screenshot
                has_screenshot = any(self.model.compute_key(t) in self.model.screenshots for t in trades)
                aggregated.append({
                    "key": (acct_num, symbol),
                    "trades": items,
                    "total_qty": total_qty,
                    "avg_entry_price": avg_entry_price,
                    "avg_exit_price": avg_exit_price if exit_date else None,
                    "total_pnl": total_pnl if exit_date else None,
                    "total_pnl_pct": total_pnl_pct if exit_date else None,
                    "entry_date": entry_date,
                    "exit_date": exit_date,
                    "avg_hold": avg_hold,
                    "open_remaining": open_remaining,
                    "has_screenshot": has_screenshot,
                })
            # Sort aggregated groups based on sort_by and direction
            def agg_key_func(item: Dict[str, object]):
                # Determine key based on column
                col = sort_by
                # If no sort specified, default to entry_date
                if not col:
                    return item["entry_date"]
                if col == "account":
                    return item["key"][0]
                if col == "symbol":
                    return item["key"][1]
                if col == "entry_date":
                    return item["entry_date"]
                if col == "exit_date":
                    return item["exit_date"] or dt.datetime.min
                if col == "entry_price":
                    return item["avg_entry_price"]
                if col == "exit_price":
                    return item["avg_exit_price"] if item["avg_exit_price"] is not None else float('-inf')
                if col == "quantity":
                    return item["total_qty"]
                if col == "pnl":
                    # None values should sort last when ascending
                    return item["total_pnl"] if item["total_pnl"] is not None else float('-inf')
                if col == "pnl_pct":
                    # None values should sort last when ascending
                    return item["total_pnl_pct"] if item["total_pnl_pct"] is not None else float('-inf')
                if col == "hold_period":
                    return item["avg_hold"] if item["avg_hold"] is not None else float('-inf')
                if col == "screenshot":
                    # sort by presence of screenshot
                    return 1 if item.get("has_screenshot") else 0
                # Default fallback
                return item["entry_date"]
            aggregated.sort(key=agg_key_func, reverse=descending)
            # Insert aggregated groups and children
            for group_idx, agg in enumerate(aggregated):
                acct_num, symbol = agg["key"]
                entry_date = agg["entry_date"]
                exit_date = agg["exit_date"]
                # Format values
                entry_str = entry_date.strftime("%Y-%m-%d")
                exit_str = exit_date.strftime("%Y-%m-%d") if exit_date else ""
                qty_str = f"{agg['total_qty']:.2f}"
                entry_price_str = f"{agg['avg_entry_price']:.2f}"
                exit_price_str = f"{agg['avg_exit_price']:.2f}" if agg["avg_exit_price"] is not None else ""
                pnl_str = f"{agg['total_pnl']:.2f}" if agg["total_pnl"] is not None else ""
                pnl_pct_str = f"{agg['total_pnl_pct']:.2f}%" if agg["total_pnl_pct"] is not None else ""
                hold_str = str(int(round(agg['avg_hold']))) if agg["avg_hold"] is not None else ""
                # Build values tuple consistent with tree columns
                # Determine screenshot indicator for aggregated row
                group_ss_indicator = "📎" if agg.get("has_screenshot") else ""
                row_values = (
                    acct_num,
                    symbol,
                    entry_str,
                    entry_price_str,
                    exit_str,
                    exit_price_str,
                    qty_str,
                    pnl_str,
                    pnl_pct_str,
                    hold_str,
                    group_ss_indicator,
                    ""
                )
                # Use a unique id for the group row; prefix 'g' to avoid collision with numeric trade indices
                group_id = f"g{group_idx}_{acct_num}_{symbol}"
                self.tree.insert("", "end", iid=group_id, text=f"{symbol}", values=row_values, open=False)
                # Record mapping from group id to child trade indices for deletion
                self.group_id_to_indices[group_id] = [idx for idx, _ in agg["trades"]]
                # Sort child trades if sort_by is set
                child_items = agg["trades"]
                if sort_by:
                    def child_key_func(item: Tuple[int, TradeEntry]):
                        idx, t = item
                        if sort_by == "account":
                            return t.account_number
                        if sort_by == "symbol":
                            return t.symbol
                        if sort_by == "entry_date":
                            return t.entry_date
                        if sort_by == "exit_date":
                            return t.exit_date or dt.datetime.min
                        if sort_by == "entry_price":
                            return t.entry_price
                        if sort_by == "exit_price":
                            return t.exit_price if t.exit_price is not None else float('-inf')
                        if sort_by == "quantity":
                            return t.quantity
                        if sort_by == "pnl":
                            return t.pnl if t.pnl is not None else float('-inf')
                        if sort_by == "pnl_pct":
                            return t.pnl_pct if t.pnl_pct is not None else float('-inf')
                        if sort_by == "hold_period":
                            return t.hold_period if t.hold_period is not None else float('-inf')
                        if sort_by == "screenshot":
                            return 1 if self.model.compute_key(t) in self.model.screenshots else 0
                        return t.entry_date
                    child_items = sorted(child_items, key=child_key_func, reverse=descending)
                for idx, t in child_items:
                    entry_date_str = t.entry_date.strftime("%Y-%m-%d")
                    exit_date_str = t.exit_date.strftime("%Y-%m-%d") if t.exit_date else ""
                    key = self.model.compute_key(t)
                    screen_indicator = "📎" if key in self.model.screenshots else ""
                    note_str = self.model.notes.get(key, "")
                    entry_strategy_str = self.model.entry_strategies.get(key, "")
                    exit_strategy_str = self.model.exit_strategies.get(key, "")
                    row = (
                        t.account_number,
                        t.symbol,
                        entry_date_str,
                        f"{t.entry_price:.2f}",
                        exit_date_str,
                        f"{t.exit_price:.2f}" if t.exit_price else "",
                        f"{t.quantity:.2f}",
                        f"{t.pnl:.2f}" if t.pnl is not None else "",
                        f"{t.pnl_pct:.2f}%" if t.pnl_pct is not None else "",
                        str(t.hold_period) if t.hold_period is not None else "",
                        screen_indicator,
                        entry_strategy_str,
                        exit_strategy_str,
                        note_str,
                    )
                    # Use the numeric index as iid for child to allow mapping notes back
                    row_id = str(idx)
                    self.tree.insert(group_id, "end", iid=row_id, text="", values=row)
        else:
            # Non-group view: show headings only (no tree column)
            self.tree.configure(show="headings")
            # Build list of filtered trades
            visible_trades: List[Tuple[int, TradeEntry]] = []
            for idx, trade in enumerate(self.model.trades):
                if trade_visible(idx, trade):
                    visible_trades.append((idx, trade))
            # Sort trades based on current sort settings
            if sort_by:
                def trade_key_func(item: Tuple[int, TradeEntry]):
                    i, t = item
                    if sort_by == "account":
                        return t.account_number
                    if sort_by == "symbol":
                        return t.symbol
                    if sort_by == "entry_date":
                        return t.entry_date
                    if sort_by == "exit_date":
                        return t.exit_date or dt.datetime.min
                    if sort_by == "entry_price":
                        return t.entry_price
                    if sort_by == "exit_price":
                        return t.exit_price if t.exit_price is not None else float('-inf')
                    if sort_by == "quantity":
                        return t.quantity
                    if sort_by == "pnl":
                        return t.pnl if t.pnl is not None else float('-inf')
                    if sort_by == "pnl_pct":
                        return t.pnl_pct if t.pnl_pct is not None else float('-inf')
                    if sort_by == "hold_period":
                        return t.hold_period if t.hold_period is not None else float('-inf')
                    if sort_by == "screenshot":
                        return 1 if self.model.compute_key(t) in self.model.screenshots else 0
                    # default
                    return t.entry_date
                visible_trades.sort(key=trade_key_func, reverse=descending)
            # Insert rows
            for idx, trade in visible_trades:
                entry_date_str = trade.entry_date.strftime("%Y-%m-%d")
                exit_date_str = trade.exit_date.strftime("%Y-%m-%d") if trade.exit_date else ""
                key = self.model.compute_key(trade)
                screen_indicator = "📎" if key in self.model.screenshots else ""
                note_str = self.model.notes.get(key, "")
                entry_strategy_str = self.model.entry_strategies.get(key, "")
                exit_strategy_str = self.model.exit_strategies.get(key, "")
                row = (
                    trade.account_number,
                    trade.symbol,
                    entry_date_str,
                    f"{trade.entry_price:.2f}",
                    exit_date_str,
                    f"{trade.exit_price:.2f}" if trade.exit_price else "",
                    f"{trade.quantity:.2f}",
                    f"{trade.pnl:.2f}" if trade.pnl is not None else "",
                    f"{trade.pnl_pct:.2f}%" if trade.pnl_pct is not None else "",
                    str(trade.hold_period) if trade.hold_period is not None else "",
                    screen_indicator,
                    entry_strategy_str,
                    exit_strategy_str,
                    note_str,
                )
                row_id = str(idx)
                self.tree.insert("", "end", iid=row_id, values=row)
                self.id_to_key[row_id] = self.model.compute_key(trade)
        
        # Auto-fit columns to content
        self.autofit_columns()

    def on_tree_select(self, event: tk.Event) -> None:
        """Handle selection of a treeview item to load its note."""
        selected = self.tree.selection()
        if not selected:
            return
        item_id = selected[0]
        # Determine if this row corresponds to a trade
        key = self.id_to_key.get(item_id)
        if key is not None:
            # Load note, strategies, and screenshots for this trade
            note = self.model.notes.get(key, "")
            self.note_text.delete("1.0", tk.END)
            self.note_text.insert(tk.END, note)
            entry_strategy = self.model.entry_strategies.get(key, "")
            self.entry_strategy_text.delete("1.0", tk.END)
            self.entry_strategy_text.insert(tk.END, entry_strategy)
            exit_strategy = self.model.exit_strategies.get(key, "")
            self.exit_strategy_text.delete("1.0", tk.END)
            self.exit_strategy_text.insert(tk.END, exit_strategy)
            ss_list = self.model.screenshots.get(key)
            if ss_list and len(ss_list) > 0:
                self.screenshot_var.set(f"{len(ss_list)} screenshot(s)")
                # Try to load preview of first screenshot
                self._update_screenshot_preview(ss_list[0])
            else:
                self.screenshot_var.set("(none)")
                self.screenshot_preview_label.configure(image="")
                self.screenshot_preview_label.image = None
        else:
            # Aggregated or unknown row selected
            self.note_text.delete("1.0", tk.END)
            self.screenshot_var.set("")
            self.screenshot_preview_label.configure(image="")
            self.screenshot_preview_label.image = None
    
    def on_tree_double_click(self, event: tk.Event) -> None:
        """Handle double-click on tree item to view screenshots if clicked on screenshot column."""
        item_id = self.tree.identify("item", event.x, event.y)
        column = self.tree.identify("column", event.x, event.y)
        
        if not item_id or not column:
            return
        
        # Check if clicked on screenshot column (column #11)
        # The columns are: account(#1), symbol(#2), entry_date(#3), entry_price(#4), 
        # exit_date(#5), exit_price(#6), quantity(#7), pnl(#8), pnl_pct(#9), hold_period(#10),
        # screenshot(#11), entry_strategy(#12), exit_strategy(#13), note(#14)
        if column != "#11":
            return
        
        key = self.id_to_key.get(item_id)
        if key is None:
            return
        
        if key in self.model.screenshots and self.model.screenshots[key]:
            self.view_screenshots()

    def save_note(self) -> None:
        """Save the note and strategies for the selected trade entry."""
        selected = self.tree.selection()
        if not selected:
            messagebox.showinfo("No Selection", "Please select a trade to add a note.")
            return
        item_id = selected[0]
        key = self.id_to_key.get(item_id)
        if key is None:
            messagebox.showinfo("Invalid Selection", "Please select an individual trade to add a note.")
            return
        note = self.note_text.get("1.0", tk.END).strip()
        self.model.notes[key] = note
        entry_strategy = self.entry_strategy_text.get("1.0", tk.END).strip()
        self.model.entry_strategies[key] = entry_strategy
        exit_strategy = self.exit_strategy_text.get("1.0", tk.END).strip()
        self.model.exit_strategies[key] = exit_strategy
        
        # Persist changes to disk
        self.model.save_state(self.persist_path, filter_state={
            "account_filter": self.account_var.get(),
            "start_date": self.start_date_var.get(),
            "end_date": self.end_date_var.get(),
            "closed_only": self.closed_only_var.get(),
            "group_by_symbol": self.group_var.get(),
            "entry_strategy_filter": self.entry_strategy_filter_var.get(),
            "exit_strategy_filter": self.exit_strategy_filter_var.get(),
        })
        
        # Refresh the table to show all updates
        self.populate_table()

    def add_screenshot(self) -> None:
        """Add a screenshot file to the selected trade (supports multiple screenshots)."""
        selected = self.tree.selection()
        if not selected:
            messagebox.showinfo("No Selection", "Please select a trade to attach a screenshot.")
            return
        item_id = selected[0]
        key = self.id_to_key.get(item_id)
        if key is None:
            messagebox.showinfo("Invalid Selection", "Please select an individual trade (not a grouped row) to attach a screenshot.")
            return
        # Prompt user for image file
        filetypes = [
            ("Image files", "*.png *.jpg *.jpeg *.gif *.bmp"),
            ("All files", "*.*"),
        ]
        filepath = filedialog.askopenfilename(title="Select Image", filetypes=filetypes)
        if not filepath:
            return
        # Add screenshot to list (initialize if needed)
        if key not in self.model.screenshots:
            self.model.screenshots[key] = []
        if filepath not in self.model.screenshots[key]:
            self.model.screenshots[key].append(filepath)
        # Update screenshot display
        num_screenshots = len(self.model.screenshots[key])
        self.screenshot_var.set(f"{num_screenshots} screenshot(s)")
        # Load preview of the first screenshot
        if self.model.screenshots[key]:
            self._update_screenshot_preview(self.model.screenshots[key][0])
    
    def _update_screenshot_preview(self, filepath: str) -> None:
        """Load and display a preview of the given screenshot."""
        photo = None
        try:
            from PIL import Image, ImageTk  # type: ignore
            img = Image.open(filepath)
            img.thumbnail((200, 200))
            photo = ImageTk.PhotoImage(img)
        except Exception:
            try:
                photo = tk.PhotoImage(file=filepath)
            except Exception:
                photo = None
        if photo:
            self.screenshot_preview_label.configure(image=photo)
            self.screenshot_preview_label.image = photo
        else:
            self.screenshot_preview_label.configure(image="")
            self.screenshot_preview_label.image = None

    def view_screenshots(self) -> None:
        """Open a window showing all screenshots for the selected trade."""
        selected = self.tree.selection()
        if not selected:
            messagebox.showinfo("No Selection", "Please select a trade to view its screenshots.")
            return
        item_id = selected[0]
        key = self.id_to_key.get(item_id)
        if key is None:
            messagebox.showinfo("Invalid Selection", "Please select an individual trade (not a grouped row).")
            return
        if key not in self.model.screenshots or not self.model.screenshots[key]:
            messagebox.showinfo("No Screenshots", "This trade has no attached screenshots.")
            return
        
        # Create a new window
        ss_window = tk.Toplevel(self.root)
        ss_window.title("Screenshots")
        ss_window.geometry("800x600")
        
        screenshots = self.model.screenshots[key]
        
        # Create a frame for navigation buttons and image display
        nav_frame = ttk.Frame(ss_window)
        nav_frame.pack(fill=tk.X, padx=10, pady=10)
        
        # Track current screenshot index
        current_index = [0]
        
        def update_image():
            """Update the displayed image."""
            filepath = screenshots[current_index[0]]
            try:
                from PIL import Image, ImageTk  # type: ignore
                img = Image.open(filepath)
                # Scale to fit window while maintaining aspect ratio
                img.thumbnail((750, 500))
                photo = ImageTk.PhotoImage(img)
                img_label.configure(image=photo)
                img_label.image = photo
                counter_label.config(text=f"Screenshot {current_index[0] + 1} of {len(screenshots)}")
            except Exception as e:
                img_label.configure(text=f"Could not load image: {e}")
        
        def prev_image():
            if current_index[0] > 0:
                current_index[0] -= 1
                update_image()
        
        def next_image():
            if current_index[0] < len(screenshots) - 1:
                current_index[0] += 1
                update_image()
        
        # Navigation buttons
        prev_btn = ttk.Button(nav_frame, text="← Previous", command=prev_image)
        prev_btn.pack(side=tk.LEFT, padx=5)
        
        counter_label = ttk.Label(nav_frame, text="")
        counter_label.pack(side=tk.LEFT, padx=20)
        
        next_btn = ttk.Button(nav_frame, text="Next →", command=next_image)
        next_btn.pack(side=tk.LEFT, padx=5)
        
        # Image label
        img_label = ttk.Label(ss_window)
        img_label.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Display first image
        update_image()
    
    def view_internal_chart(self) -> None:
        """Switch to Charts tab, download data, and display chart for the selected trade's symbol."""
        selected = self.tree.selection()
        if not selected:
            messagebox.showinfo("No Selection", "Please select a trade to view its chart.")
            return
        item_id = selected[0]
        try:
            values = self.tree.item(item_id, "values")
        except Exception:
            values = None
        symbol: Optional[str] = None
        if values and len(values) >= 2:
            symbol = str(values[1]).strip()
        # If still not available, attempt to find from the corresponding TradeEntry
        if not symbol:
            key = self.id_to_key.get(item_id)
            if key is not None:
                # Find matching trade
                for t in self.model.trades:
                    if self.model.compute_key(t) == key:
                        symbol = t.symbol
                        break
        if not symbol:
            messagebox.showinfo("Unknown Symbol", "Could not determine the symbol for the selected row.")
            return
        
        # Switch to Charts tab
        self.notebook.select(1)  # Index 1 is Charts tab
        
        # Set the symbol
        self.chart_symbol_var.set(symbol)
        
        # Trigger download and chart display
        self.on_chart_symbol_selected()
        self.on_download_price_data()

    def view_tradingview_chart(self) -> None:
        """Open an external TradingView chart for the selected trade's symbol in the default web browser.

        When a row is selected, this method extracts the symbol from the row's data
        and opens a TradingView chart for that symbol in a new browser tab. If
        no row is selected or the symbol cannot be determined, an informative
        message is shown instead.
        """
        selected = self.tree.selection()
        if not selected:
            messagebox.showinfo("No Selection", "Please select a trade to view its chart.")
            return
        item_id = selected[0]
        # Attempt to get the symbol from the row values; second column contains the symbol
        try:
            values = self.tree.item(item_id, "values")
        except Exception:
            values = None
        symbol: Optional[str] = None
        if values and len(values) >= 2:
            symbol = str(values[1]).strip()
        # If still not available, attempt to find from the corresponding TradeEntry
        if not symbol:
            key = self.id_to_key.get(item_id)
            if key is not None:
                # Find matching trade
                for t in self.model.trades:
                    if self.model.compute_key(t) == key:
                        symbol = t.symbol
                        break
        if not symbol:
            messagebox.showinfo("Unknown Symbol", "Could not determine the symbol for the selected row.")
            return
        # Compose TradingView URL; convert to uppercase for consistency. Use daily interval.
        sym_upper = symbol.upper()
        # Add interval=D to default to a daily chart. Other parameters could be added here.
        url = f"https://www.tradingview.com/chart/?symbol={sym_upper}&interval=D"
        try:
            webbrowser.open_new_tab(url)
        except Exception:
            messagebox.showerror("Error", f"Failed to open chart for {sym_upper}.")

    def view_chart(self) -> None:
        """Deprecated: use view_internal_chart or view_tradingview_chart instead."""
        self.view_tradingview_chart()

    def remove_screenshot(self) -> None:
        """Remove all screenshots associated with the selected trade (if any).

        This method detaches all previously attached images from the selected trade
        and updates the table indicator and preview accordingly. If the selected
        row is a grouped summary or no screenshots exist, an informational
        message is shown.
        """
        selected = self.tree.selection()
        if not selected:
            messagebox.showinfo("No Selection", "Please select a trade to remove screenshots.")
            return
        item_id = selected[0]
        key = self.id_to_key.get(item_id)
        if key is None:
            messagebox.showinfo("Invalid Selection", "Please select an individual trade (not a grouped row).")
            return
        # If no screenshots attached, notify user
        if key not in self.model.screenshots or not self.model.screenshots[key]:
            messagebox.showinfo("No Screenshots", "No screenshots are attached to this trade.")
            return
        # Remove all screenshots for this trade
        try:
            del self.model.screenshots[key]
        except Exception:
            pass
        # Clear preview and filename label
        self.screenshot_var.set("(none)")
        self.screenshot_preview_label.configure(image="")
        self.screenshot_preview_label.image = None
        # Update screenshot indicator in the table for this row
        try:
            current_values = list(self.tree.item(item_id, "values"))
            # The screenshot column is at index 10 of the values tuple
            if len(current_values) > 10:
                current_values[10] = ""
                self.tree.item(item_id, values=current_values)
        except Exception:
            # If direct update fails (e.g. in grouped view), re-populate the table
            pass
        # If in grouped view, refresh the table so that group indicator updates correctly
        if self.group_var.get():
            self.populate_table()
        # Persist the updated state immediately
        try:
            filter_state = {
                'account': self.account_var.get(),
                'closed_only': self.closed_only_var.get(),
                'group_by_symbol': self.group_var.get(),
                'start_date': self.start_date_var.get(),
                'end_date': self.end_date_var.get(),
            }
            self.model.save_state(self.persist_path, filter_state)
        except Exception:
            pass

    def show_duplicate_transactions(self) -> None:
        """Display a window listing transactions that were skipped as duplicates.

        This method opens a new top-level window containing a table of
        duplicate transactions (those that matched existing entries across
        sessions). The user can review the duplicates to verify why they were
        ignored. If no duplicates were recorded, the method does nothing.
        """
        duplicates = getattr(self.model, 'duplicate_transactions', [])
        if not duplicates:
            return
        dup_win = tk.Toplevel(self.root)
        dup_win.title("Duplicate Transactions")
        dup_win.geometry("600x300")
        # Treeview to display duplicate transactions
        cols = ("run_date", "account_number", "symbol", "quantity", "price", "amount")
        tree = ttk.Treeview(dup_win, columns=cols, show="headings")
        for col in cols:
            header = col.replace("_", " ").title()
            tree.heading(col, text=header)
            tree.column(col, width=100, anchor=tk.CENTER)
        # Populate the tree
        for idx, tx in enumerate(duplicates):
            run_date_str = tx.run_date.strftime("%Y-%m-%d %H:%M")
            tree.insert("", "end", iid=str(idx), values=(
                run_date_str,
                tx.account_number,
                tx.symbol,
                f"{tx.quantity:.2f}",
                f"{tx.price:.2f}",
                f"{tx.amount:.2f}",
            ))
        tree.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        # Add a scrollbar
        vsb = ttk.Scrollbar(dup_win, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=vsb.set)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        # Close button
        close_btn = ttk.Button(dup_win, text="Close", command=dup_win.destroy)
        close_btn.pack(pady=(0, 5))

    def add_transaction_dialog(self) -> None:
        """Open a dialog to allow the user to manually add a transaction.

        The dialog collects basic fields for a transaction: account number,
        symbol, quantity (positive for buys, negative for sells), price, and
        run date (with optional time). Upon submission, a new Transaction is
        created, added to the model, and the trades are re‑matched. Duplicate
        detection across sessions still applies; if the new transaction matches
        an existing one, it will be silently ignored. After adding, the
        journal is refreshed and persisted.
        """
        dialog = tk.Toplevel(self.root)
        dialog.title("Add Transaction")
        dialog.resizable(False, False)
        # Fields for account number (combobox), symbol, action (buy/sell), quantity, price, date, time
        ttk.Label(dialog, text="Account Number:").grid(row=0, column=0, sticky="e", padx=5, pady=5)
        acct_var = tk.StringVar()
        # Build list of existing accounts for convenience; allow typing new values
        existing_accounts = sorted({tx.account_number for tx in self.model.transactions})
        acct_combo = ttk.Combobox(dialog, textvariable=acct_var, values=existing_accounts, state="normal")
        acct_combo.grid(row=0, column=1, padx=5, pady=5)

        ttk.Label(dialog, text="Symbol:").grid(row=1, column=0, sticky="e", padx=5, pady=5)
        sym_var = tk.StringVar()
        sym_entry = ttk.Entry(dialog, textvariable=sym_var)
        sym_entry.grid(row=1, column=1, padx=5, pady=5)

        # Action (Buy or Sell)
        ttk.Label(dialog, text="Action:").grid(row=2, column=0, sticky="e", padx=5, pady=5)
        action_var = tk.StringVar(value="Buy")
        action_combo = ttk.Combobox(dialog, textvariable=action_var, values=["Buy", "Sell"], state="readonly")
        action_combo.grid(row=2, column=1, padx=5, pady=5)

        ttk.Label(dialog, text="Quantity:").grid(row=3, column=0, sticky="e", padx=5, pady=5)
        qty_var = tk.StringVar()
        qty_entry = ttk.Entry(dialog, textvariable=qty_var)
        qty_entry.grid(row=3, column=1, padx=5, pady=5)

        ttk.Label(dialog, text="Price per Share:").grid(row=4, column=0, sticky="e", padx=5, pady=5)
        price_var = tk.StringVar()
        price_entry = ttk.Entry(dialog, textvariable=price_var)
        price_entry.grid(row=4, column=1, padx=5, pady=5)

        ttk.Label(dialog, text="Date (YYYY-MM-DD):").grid(row=5, column=0, sticky="e", padx=5, pady=5)
        date_var = tk.StringVar()
        date_entry = ttk.Entry(dialog, textvariable=date_var)
        date_entry.grid(row=5, column=1, padx=5, pady=5)
        # Provide a date picker button
        date_btn = ttk.Button(dialog, text="📅", width=3, command=lambda: self.open_date_picker(date_var))
        date_btn.grid(row=5, column=2, padx=5, pady=5)

        ttk.Label(dialog, text="Time (HH:MM) [optional]:").grid(row=6, column=0, sticky="e", padx=5, pady=5)
        time_var = tk.StringVar()
        time_entry = ttk.Entry(dialog, textvariable=time_var)
        time_entry.grid(row=6, column=1, padx=5, pady=5)
        # Function to handle submission
        def submit() -> None:
            acct = acct_var.get().strip()
            sym = sym_var.get().strip()
            qty_str = qty_var.get().strip()
            price_str = price_var.get().strip()
            date_str = date_var.get().strip()
            time_str = time_var.get().strip()
            action_choice = action_var.get().strip()
            # Validate required fields
            if not acct or not sym or not qty_str or not price_str or not date_str:
                messagebox.showwarning("Missing Data", "Please fill in account, symbol, quantity, price and date.")
                return
            # Parse quantity magnitude
            try:
                qty_val = float(qty_str)
                if qty_val <= 0:
                    messagebox.showwarning("Invalid Quantity", "Quantity must be a positive number.")
                    return
            except ValueError:
                messagebox.showwarning("Invalid Quantity", "Quantity must be a number.")
                return
            # Determine sign based on action
            qty = qty_val if action_choice == "Buy" else -qty_val
            # Parse price
            try:
                price = float(price_str)
            except ValueError:
                messagebox.showwarning("Invalid Price", "Price must be a number.")
                return
            # Parse date/time
            try:
                if time_str:
                    run_dt = dt.datetime.strptime(f"{date_str} {time_str}", "%Y-%m-%d %H:%M")
                else:
                    run_dt = dt.datetime.strptime(date_str, "%Y-%m-%d")
            except ValueError:
                messagebox.showwarning("Invalid Date/Time", "Date or time format is invalid. Date must be YYYY-MM-DD and time HH:MM.")
                return
            # Determine amount and action description
            amount = price * qty
            action_desc = f"Manual {action_choice}" if action_choice else "Manual Entry"
            tx = Transaction(
                run_date=run_dt,
                account=acct,
                account_number=acct,
                symbol=sym,
                action=action_desc,
                price=price,
                quantity=qty,
                amount=amount,
                settlement_date=None,
            )
            # Compute duplicate key for this transaction
            key = (tx.run_date, tx.account_number, tx.symbol, tx.quantity, tx.price, tx.amount)
            # Check if duplicate across sessions (using model.seen_tx_keys). If so, ignore.
            if key in self.model.seen_tx_keys:
                messagebox.showinfo("Duplicate", "This transaction already exists in the journal and will be ignored.")
                dialog.destroy()
                return
            # Otherwise, add to model
            self.model.transactions.append(tx)
            # Record this key so future imports consider it existing
            self.model.seen_tx_keys.add(key)
            # Re-match trades
            self.model.reset_matching()
            self.model._match_trades()
            # Refresh UI
            acct_numbers = sorted({t.account_number for t in self.model.transactions})
            self.account_dropdown["values"] = ["all"] + acct_numbers
            if self.account_var.get() not in ["all"] + acct_numbers:
                self.account_var.set("all")
            self.populate_table()
            self.update_summary_and_chart()
            # Update chart symbols
            self.update_chart_symbols()
            # Persist state
            try:
                filter_state = {
                    'account': self.account_var.get(),
                    'closed_only': self.closed_only_var.get(),
                    'group_by_symbol': self.group_var.get(),
                    'start_date': self.start_date_var.get(),
                    'end_date': self.end_date_var.get(),
                }
                self.model.save_state(self.persist_path, filter_state)
            except Exception:
                pass
            dialog.destroy()
        # Buttons
        submit_btn = ttk.Button(dialog, text="Add", command=submit)
        submit_btn.grid(row=7, column=0, columnspan=2, pady=(10, 5))
        cancel_btn = ttk.Button(dialog, text="Cancel", command=dialog.destroy)
        cancel_btn.grid(row=7, column=2, pady=(10, 5))

    def delete_selected_transactions(self) -> None:
        """Delete the selected trade entries and their underlying transactions.

        This method allows the user to remove one or more trade entries from the
        journal. It maps each selected trade entry back to the underlying
        transaction records by matching on account number, symbol, run date,
        price and quantity (with appropriate sign for sells). Each matching
        Transaction is removed from the model. A confirmation dialog is shown
        before any deletions occur. After deletion, the trades are re‑matched,
        the UI is updated, and the journal is persisted.
        """
        selected = self.tree.selection()
        if not selected:
            messagebox.showinfo("No Selection", "Please select one or more trades to delete.")
            return
        # Determine the trade keys for the selected rows and groups
        keys_to_delete: List[tuple] = []
        # Also track indices of trades to delete (for group rows)
        indices_to_delete: List[int] = []
        for item_id in selected:
            # If this is an individual trade row
            if item_id in self.id_to_key:
                key = self.id_to_key[item_id]
                if key is not None:
                    keys_to_delete.append(key)
            # If this is a group row, delete all child trades in the group
            elif item_id in self.group_id_to_indices:
                for idx in self.group_id_to_indices[item_id]:
                    indices_to_delete.append(idx)
        # Add keys from group indices
        for idx in indices_to_delete:
            if 0 <= idx < len(self.model.trades):
                key = self.model.compute_key(self.model.trades[idx])
                keys_to_delete.append(key)
        if not keys_to_delete:
            messagebox.showinfo("Invalid Selection", "Please select individual trades or group rows to delete.")
            return
        # Remove duplicate keys
        unique_keys = list(dict.fromkeys(keys_to_delete))
        # Ask for confirmation
        if not messagebox.askyesno("Confirm Deletion", f"Delete {len(unique_keys)} selected trade(s)? This cannot be undone."):
            return
        # Build set of transaction keys to remove (matching run_date, acct_num, symbol, qty, price, amount)
        tx_keys_to_remove: set = set()
        for key in unique_keys:
            # Find the corresponding trade entry
            for trade in self.model.trades:
                if self.model.compute_key(trade) == key:
                    # Identify underlying buy transaction (entry)
                    buy_run_date = trade.entry_date
                    buy_price = trade.entry_price
                    buy_qty = trade.quantity
                    acct_num = trade.account_number
                    symbol = trade.symbol
                    # Use a negative amount for buys to match the CSV (spending money)
                    buy_amount = -buy_price * buy_qty
                    buy_key = (buy_run_date, acct_num, symbol, buy_qty, buy_price, buy_amount)
                    tx_keys_to_remove.add(buy_key)
                    # If closed trade, also identify matching sell transaction
                    if trade.exit_date and trade.exit_price is not None:
                        sell_run_date = trade.exit_date
                        sell_price = trade.exit_price
                        # Negative quantity for sell
                        sell_qty = -trade.quantity
                        # Use a positive amount for sells (proceeds)
                        sell_amount = sell_price * trade.quantity
                        sell_key = (sell_run_date, acct_num, symbol, sell_qty, sell_price, sell_amount)
                        tx_keys_to_remove.add(sell_key)
                    break
        # Remove transactions matching these keys
        new_transactions: List[Transaction] = []
        for tx in self.model.transactions:
            k = (tx.run_date, tx.account_number, tx.symbol, tx.quantity, tx.price, tx.amount)
            if k not in tx_keys_to_remove:
                new_transactions.append(tx)
        # Replace transactions list
        self.model.transactions = new_transactions
        # Remove corresponding keys from seen_tx_keys
        self.model.seen_tx_keys = {k for k in self.model.seen_tx_keys if k not in tx_keys_to_remove}
        # Remove notes and screenshots for deleted trades
        for key in unique_keys:
            self.model.notes.pop(key, None)
            self.model.screenshots.pop(key, None)
        # Rematch trades
        self.model.reset_matching()
        self.model._match_trades()
        # Refresh UI
        acct_numbers = sorted({tx.account_number for tx in self.model.transactions})
        self.account_dropdown["values"] = ["all"] + acct_numbers
        if self.account_var.get() not in ["all"] + acct_numbers:
            self.account_var.set("all")
        self.populate_table()
        self.update_summary_and_chart()
        # Update chart symbols
        self.update_chart_symbols()
        # Persist changes
        try:
            filter_state = {
                'account': self.account_var.get(),
                'closed_only': self.closed_only_var.get(),
                'group_by_symbol': self.group_var.get(),
                'start_date': self.start_date_var.get(),
                'end_date': self.end_date_var.get(),
            }
            self.model.save_state(self.persist_path, filter_state)
        except Exception:
            pass

    def clear_journal(self) -> None:
        """Completely remove all transactions from the journal after confirmation."""
        if not self.model.transactions:
            messagebox.showinfo("No Data", "The journal is already empty.")
            return
        if not messagebox.askyesno("Confirm Clear", "Delete all transactions and clear the journal? This cannot be undone."):
            return
        # Clear model and UI
        self.model.clear()
        # Remove persisted state file
        try:
            if os.path.exists(self.persist_path):
                os.remove(self.persist_path)
        except Exception:
            pass
        # Clear UI components
        self.account_dropdown["values"] = ["all"]
        self.account_var.set("all")
        self.populate_table()
        self.update_summary_and_chart()
        try:
            self.update_chart_symbols()
        except Exception:
            pass

    def on_top_filter_change(self) -> None:
        """Handle changes to the top N winners/losers filter.

        This method parses the top N value and filter type, computes the
        appropriate set of trade indices, and refreshes the table, summary,
        and chart. If the top N value is empty or invalid, no filtering is
        applied. The filter applies only to closed trades with P&L values.
        """
        # Reset top filter set
        self.top_filter_set = None
        # Parse N
        n_str = self.top_n_var.get().strip()
        try:
            n = int(n_str) if n_str else 0
            if n < 0:
                raise ValueError
        except ValueError:
            messagebox.showwarning("Invalid N", "Top N must be a non-negative integer.")
            return
        filter_type = self.top_filter_type_var.get()
        if filter_type not in {"None", "Winners", "Losers"}:
            filter_type = "None"
        # Compute top filter set if necessary
        if n > 0 and filter_type != "None":
            metric = self.top_filter_metric_var.get()
            self.top_filter_set = self.compute_top_filter_set(n, filter_type, metric)
        
        # Save settings to config (merge-update; does not wipe other keys)
        save_chart_settings(
            {
                "top_n": n_str,
                "top_filter_type": filter_type,
                "top_filter_metric": self.top_filter_metric_var.get(),
            }
        )
        
        # Refresh table and summary
        self.populate_table()
        self.update_summary_and_chart()

    def compute_top_filter_set(self, n: int, filter_type: str, metric: str = "PnL") -> Optional[set]:
        """Compute a set of trade indices for the top N winners or losers.

        :param n: the number of trades to include
        :param filter_type: either "Winners" or "Losers"
        :param metric: either "PnL" or "PnL %" for sorting
        :return: a set of indices corresponding to the top trades meeting
                 the current filters (account, date range, and closed-only). If
                 no trades meet the criteria, returns None.
        """
        # Build list of candidate trades with their indices and P&L
        candidates: List[Tuple[int, TradeEntry]] = []
        account_filter = self.account_var.get()
        closed_only = self.closed_only_var.get()
        for idx, trade in enumerate(self.model.trades):
            # Only consider trades with an exit date (closed) and a P&L value
            if not trade.is_closed or trade.pnl is None:
                continue
            # Account filter
            if account_filter and account_filter != "all" and trade.account_number != account_filter:
                continue
            # Date range filter on entry_date (inclusive)
            if self.start_date and trade.entry_date.date() < self.start_date:
                continue
            if self.end_date and trade.entry_date.date() > self.end_date:
                continue
            # Closed-only filter: skip trades whose originating buy is still open
            if closed_only:
                if trade.buy_id < 0:
                    continue
                if self.model.open_qty_by_buy_id.get(trade.buy_id, 0.0) > 1e-8:
                    continue
            # Add candidate
            candidates.append((idx, trade))
        if not candidates:
            return None
        # Filter by winners or losers and sort by selected metric
        if metric == "PnL %":
            # Use pnl_pct for sorting
            if filter_type == "Winners":
                # Only trades with positive P&L %
                filtered = [(idx, t) for idx, t in candidates if t.pnl_pct is not None and t.pnl_pct > 0]
                # Sort descending by pnl_pct
                filtered.sort(key=lambda x: x[1].pnl_pct if x[1].pnl_pct is not None else 0, reverse=True)
            else:  # "Losers"
                filtered = [(idx, t) for idx, t in candidates if t.pnl_pct is not None and t.pnl_pct < 0]
                # Sort ascending by pnl_pct (most negative first)
                filtered.sort(key=lambda x: x[1].pnl_pct if x[1].pnl_pct is not None else 0)
        else:  # "PnL"
            if filter_type == "Winners":
                # Only trades with positive P&L
                filtered = [(idx, t) for idx, t in candidates if t.pnl > 0]
                # Sort descending by pnl
                filtered.sort(key=lambda x: x[1].pnl, reverse=True)
            else:  # "Losers"
                filtered = [(idx, t) for idx, t in candidates if t.pnl < 0]
                # Sort ascending by pnl (most negative first)
                filtered.sort(key=lambda x: x[1].pnl)
        # Select top N indices
        top_indices = {idx for idx, _ in filtered[:n]}
        return top_indices if top_indices else None

    def load_persisted_data(self) -> None:
        """Load persisted journal data from disk (if available)."""
        try:
            filter_state = self.model.load_state(self.persist_path)
            # If any data loaded, populate UI accordingly
            if self.model.transactions:
                # Populate account filter options
                acct_numbers = sorted({tx.account_number for tx in self.model.transactions})
                self.account_dropdown["values"] = ["all"] + acct_numbers
                # Restore filter state if available
                if filter_state:
                    self.account_var.set(filter_state.get('account', 'all'))
                    self.closed_only_var.set(filter_state.get('closed_only', False))
                    self.group_var.set(filter_state.get('group_by_symbol', True))
                    self.start_date_var.set(filter_state.get('start_date', ''))
                    self.end_date_var.set(filter_state.get('end_date', ''))
                    self.entry_strategy_filter_var.set(filter_state.get('entry_strategy_filter', 'all'))
                    self.exit_strategy_filter_var.set(filter_state.get('exit_strategy_filter', 'all'))
                    # Apply date filter if dates were set
                    if filter_state.get('start_date') or filter_state.get('end_date'):
                        self.apply_date_filter()
                else:
                    self.account_dropdown.set("all")
                # Populate table and summary
                self.populate_table()
                self.update_summary_and_chart()
                # Update chart tab symbol list
                self.update_chart_symbols()
                # Restore chart symbol and display if available
                if filter_state and filter_state.get('chart_symbol'):
                    try:
                        self.chart_symbol_var.set(filter_state.get('chart_symbol'))
                        self.on_chart_symbol_selected()
                    except Exception:
                        pass
        except Exception:
            pass

    def on_close(self) -> None:
        """Persist data and filter settings, then close the application."""
        try:
            # Gather current filter state
            filter_state = {
                'account': self.account_var.get(),
                'closed_only': self.closed_only_var.get(),
                'group_by_symbol': self.group_var.get(),
                'start_date': self.start_date_var.get(),
                'end_date': self.end_date_var.get(),
                'entry_strategy_filter': self.entry_strategy_filter_var.get(),
                'exit_strategy_filter': self.exit_strategy_filter_var.get(),
                'chart_symbol': self.chart_symbol_var.get(),
            }
            self.model.save_state(self.persist_path, filter_state)
        except Exception:
            pass
        self.root.destroy()

    def apply_date_filter(self) -> None:
        """Parse date filter inputs and refresh the table and summary."""
        start_str = self.start_date_var.get().strip()
        end_str = self.end_date_var.get().strip()
        self.start_date = None
        self.end_date = None
        # Parse start date
        if start_str:
            try:
                self.start_date = dt.datetime.strptime(start_str, "%Y-%m-%d").date()
            except ValueError:
                messagebox.showwarning("Invalid Date", f"Start date '{start_str}' is not in YYYY-MM-DD format.")
                return
        # Parse end date
        if end_str:
            try:
                self.end_date = dt.datetime.strptime(end_str, "%Y-%m-%d").date()
            except ValueError:
                messagebox.showwarning("Invalid Date", f"End date '{end_str}' is not in YYYY-MM-DD format.")
                return
        # If both dates provided, ensure start <= end
        if self.start_date and self.end_date and self.start_date > self.end_date:
            messagebox.showwarning("Invalid Range", "Start date cannot be after end date.")
            return
        # Refresh table and summary/chart
        self.populate_table()
        self.update_summary_and_chart()

    def clear_filters(self) -> None:
        """Reset all filter settings to their defaults and refresh the display.

        This method clears any date range, top‑N filter, account filter,
        and checkbox options (closed positions only and group by symbol).
        After resetting the variables, it repopulates the table and
        recomputes the summary and equity curve.
        """
        # Reset date text fields and internal date boundaries
        self.start_date_var.set("")
        self.end_date_var.set("")
        self.start_date = None
        self.end_date = None
        # Reset top N filter and filter type
        self.top_n_var.set("")
        if hasattr(self, 'top_filter_set'):
            self.top_filter_set = None
        self.top_filter_type_var.set("None")
        self.top_filter_metric_var.set("PnL")
        # Reset boolean filters
        self.closed_only_var.set(False)
        self.group_var.set(False)
        # Reset account filter to all
        self.account_var.set("all")
        self.account_dropdown.set("all")
        # Refresh table and summary/chart
        self.populate_table()
        self.update_summary_and_chart()

    def open_date_picker(self, date_var: tk.StringVar) -> None:
        """Open a simple date picker dialog to select a date and set it to the provided StringVar.

        The date picker defaults to the current month and year. When the user selects a day,
        the date is formatted as YYYY-MM-DD and assigned to ``date_var``. This function
        creates a modal top-level window with navigation to previous/next months.
        """
        # Inner class for date picker dialog
        class DatePicker(tk.Toplevel):
            def __init__(self, parent, var: tk.StringVar):
                super().__init__(parent)
                self.title("Select Date")
                self.resizable(False, False)
                self.var = var
                # Determine initial month/year from existing value or current date
                try:
                    current = dt.datetime.strptime(var.get(), "%Y-%m-%d").date()
                except Exception:
                    current = dt.date.today()
                self.year = current.year
                self.month = current.month
                # Build UI
                self.header = ttk.Frame(self)
                self.header.pack(fill=tk.X, pady=(5, 0))
                self.prev_btn = ttk.Button(self.header, text="<", width=2, command=self.prev_month)
                self.prev_btn.pack(side=tk.LEFT)
                self.title_lbl = ttk.Label(self.header, text="", width=15, anchor=tk.CENTER)
                self.title_lbl.pack(side=tk.LEFT, expand=True)
                self.next_btn = ttk.Button(self.header, text=">", width=2, command=self.next_month)
                self.next_btn.pack(side=tk.RIGHT)
                # Frame for calendar buttons
                self.days_frame = ttk.Frame(self)
                self.days_frame.pack(padx=5, pady=5)
                self.draw_calendar()

            def draw_calendar(self) -> None:
                # Clear existing day buttons
                for child in self.days_frame.winfo_children():
                    child.destroy()
                import calendar
                # Update header label
                month_name = calendar.month_name[self.month]
                self.title_lbl.config(text=f"{month_name} {self.year}")
                # Days of week header
                days_of_week = ['Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa', 'Su']
                for idx, day_name in enumerate(days_of_week):
                    lbl = ttk.Label(self.days_frame, text=day_name, width=3, anchor=tk.CENTER)
                    lbl.grid(row=0, column=idx, padx=1, pady=1)
                # Generate calendar matrix
                cal = calendar.Calendar(firstweekday=0)  # Monday as first day
                month_days = cal.monthdayscalendar(self.year, self.month)
                # Populate days
                for row_idx, week in enumerate(month_days, start=1):
                    for col_idx, day in enumerate(week):
                        if day == 0:
                            # Empty cell
                            lbl = ttk.Label(self.days_frame, text="", width=3)
                            lbl.grid(row=row_idx, column=col_idx, padx=1, pady=1)
                        else:
                            day_str = f"{day:02d}"
                            btn = ttk.Button(self.days_frame, text=day_str, width=3,
                                             command=lambda d=day: self.select_day(d))
                            btn.grid(row=row_idx, column=col_idx, padx=1, pady=1)

            def select_day(self, day: int) -> None:
                # Set selected date
                date_obj = dt.date(self.year, self.month, day)
                self.var.set(date_obj.strftime("%Y-%m-%d"))
                self.destroy()

            def prev_month(self) -> None:
                # Navigate to previous month
                if self.month == 1:
                    self.month = 12
                    self.year -= 1
                else:
                    self.month -= 1
                self.draw_calendar()

            def next_month(self) -> None:
                # Navigate to next month
                if self.month == 12:
                    self.month = 1
                    self.year += 1
                else:
                    self.month += 1
                self.draw_calendar()

        # Instantiate date picker and center it relative to parent
        picker = DatePicker(self.root, date_var)
        # Position the picker near the mouse pointer
        self.root.update_idletasks()
        x = self.root.winfo_pointerx()
        y = self.root.winfo_pointery()
        picker.geometry(f"+{x}+{y}")

    def on_account_filter_change(self, event: tk.Event) -> None:
        """Update summary and chart when account filter changes."""
        self.populate_table()
        self.update_summary_and_chart()

    def on_closed_filter_change(self) -> None:
        """Update table, summary and chart when the closed-only checkbox is toggled."""
        self.populate_table()
        self.update_summary_and_chart()

    def on_group_change(self) -> None:
        """Called when the group-by-symbol checkbox is toggled."""
        # Changing grouping may require switching between tree and non-tree view.
        # Re-populate the table but leave summary unchanged (summary uses model only).
        self.populate_table()
        # Summary and chart do not depend on grouping; update if closed filter changed
        self.update_summary_and_chart()

    def on_strategy_filter_change(self, event=None) -> None:
        """Handle entry/exit strategy filter changes with partial text matching."""
        self.populate_table()
        self.update_summary_and_chart()

    def on_sort(self, column: str) -> None:
        """Handle sorting when a column header is clicked."""
        # Toggle sort direction if clicking the same column; otherwise reset
        if self.sort_by == column:
            self.sort_descending = not self.sort_descending
        else:
            self.sort_by = column
            self.sort_descending = False
        # Re-populate the table according to the new sort order
        self.populate_table()


    def update_summary_and_chart(self) -> None:
        """Compute and display summary statistics and equity curve chart."""
        account_filter = self.account_var.get()
        closed_only = self.closed_only_var.get()
        entry_strategy_filter = self.entry_strategy_filter_var.get()
        exit_strategy_filter = self.exit_strategy_filter_var.get()
        # Check for top filter
        top_set = getattr(self, 'top_filter_set', None)
        
        # Helper function to check if a trade matches strategy filters
        def matches_strategy_filters(trade: TradeEntry) -> bool:
            key = self.model.compute_key(trade)
            if entry_strategy_filter and entry_strategy_filter != "all":
                trade_entry_strategy = self.model.entry_strategies.get(key, "")
                if entry_strategy_filter.lower() not in trade_entry_strategy.lower():
                    return False
            if exit_strategy_filter and exit_strategy_filter != "all":
                trade_exit_strategy = self.model.exit_strategies.get(key, "")
                if exit_strategy_filter.lower() not in trade_exit_strategy.lower():
                    return False
            return True
        
        # Check if any strategy filter is active
        has_strategy_filter = (entry_strategy_filter and entry_strategy_filter != "all") or (exit_strategy_filter and exit_strategy_filter != "all")
        
        # Determine summary - always compute manually if strategy filters are active or top_set is present
        if top_set is None and not has_strategy_filter:
            summary = self.model.compute_summary(account_filter, closed_only=closed_only,
                                                 start_date=self.start_date, end_date=self.end_date)
        else:
            # Compute summary from filtered trades
            total_pnl = 0.0
            num_trades = 0
            num_wins = 0
            num_losses = 0
            num_breakeven = 0
            total_hold = 0
            winner_pnl_sum = 0.0
            loser_pnl_sum = 0.0
            winner_pnl_pct_sum = 0.0
            loser_pnl_pct_sum = 0.0
            winner_hold_sum = 0
            loser_hold_sum = 0
            for idx, trade in enumerate(self.model.trades):
                if top_set is not None and idx not in top_set:
                    continue
                # Strategy filters
                if not matches_strategy_filters(trade):
                    continue
                # Only consider CLOSED trades (fully exited lots) with status == "CLOSED"
                if not trade.is_closed:
                    continue
                # Account filter
                if account_filter and account_filter != "all" and trade.account_number != account_filter:
                    continue
                # Date range filter on entry_date (inclusive)
                if self.start_date and trade.entry_date.date() < self.start_date:
                    continue
                if self.end_date and trade.entry_date.date() > self.end_date:
                    continue
                # Closed-only filter: skip trades whose originating buy is still open
                if closed_only:
                    if trade.buy_id < 0:
                        continue
                    if self.model.open_qty_by_buy_id.get(trade.buy_id, 0.0) > 1e-8:
                        continue
                pnl = trade.pnl or 0.0
                pnl_pct = trade.pnl_pct or 0.0
                total_pnl += pnl
                num_trades += 1
                if pnl > 1e-8:  # Win (PnL > 0)
                    num_wins += 1
                    winner_pnl_sum += pnl
                    winner_pnl_pct_sum += pnl_pct
                    winner_hold_sum += trade.hold_period or 0
                elif pnl < -1e-8:  # Loss (PnL < 0)
                    num_losses += 1
                    loser_pnl_sum += pnl
                    loser_pnl_pct_sum += pnl_pct
                    loser_hold_sum += trade.hold_period or 0
                else:  # Breakeven (PnL ≈ 0)
                    num_breakeven += 1
                total_hold += trade.hold_period or 0
            win_ratio = (num_wins / (num_wins + num_losses)) if (num_wins + num_losses) > 0 else 0.0
            avg_pnl = (total_pnl / num_trades) if num_trades else 0.0
            avg_hold = (total_hold / num_trades) if num_trades else 0.0
            avg_winner_pnl = (winner_pnl_sum / num_wins) if num_wins > 0 else 0.0
            avg_loser_pnl = (loser_pnl_sum / num_losses) if num_losses > 0 else 0.0
            avg_winner_pnl_pct = (winner_pnl_pct_sum / num_wins) if num_wins > 0 else 0.0
            avg_loser_pnl_pct = (loser_pnl_pct_sum / num_losses) if num_losses > 0 else 0.0
            avg_hold_winners = (winner_hold_sum / num_wins) if num_wins > 0 else 0.0
            avg_hold_losers = (loser_hold_sum / num_losses) if num_losses > 0 else 0.0
            # Profit Factor = sum(wins pnl) / abs(sum(losses pnl))
            profit_factor = 0.0
            if num_losses > 0 and loser_pnl_sum != 0:
                profit_factor = winner_pnl_sum / abs(loser_pnl_sum)
            # Expectancy = win_rate * avg_win + (1 - win_rate) * avg_loss
            expectancy = win_ratio * avg_winner_pnl + (1 - win_ratio) * avg_loser_pnl
            summary = {
                "total_pnl": total_pnl,
                "num_trades": num_trades,
                "num_wins": num_wins,
                "num_losses": num_losses,
                "num_breakeven": num_breakeven,
                "win_ratio": win_ratio,
                "avg_pnl": avg_pnl,
                "avg_hold": avg_hold,
                "avg_winner_pnl_pct": avg_winner_pnl_pct,
                "avg_loser_pnl_pct": avg_loser_pnl_pct,
                "avg_hold_winners": avg_hold_winners,
                "avg_hold_losers": avg_hold_losers,
                "profit_factor": profit_factor,
                "expectancy": expectancy,
            }
        # Build summary text
        if summary["num_trades"] == 0:
            summary_text = "No closed trades for selected account."
        else:
            summary_text = (
                f"Total P&L: {summary['total_pnl']:.2f}\n"
                f"Trades: {summary['num_trades']}\n"
                f"Wins: {summary['num_wins']}\n"
                f"Losses: {summary['num_losses']}\n"
                f"Breakeven: {summary['num_breakeven']}\n"
                f"Win Ratio: {summary['win_ratio']*100:.1f}%\n"
                f"Avg P&L: {summary['avg_pnl']:.2f}\n"
                f"Avg Hold (days): {summary['avg_hold']:.1f}\n"
                f"Profit Factor: {summary['profit_factor']:.2f}\n"
                f"Expectancy: {summary['expectancy']:.2f}\n"
                f"Avg W %: {summary['avg_winner_pnl_pct']:.2f}%\n"
                f"Avg L %: {summary['avg_loser_pnl_pct']:.2f}%\n"
                f"Avg Hold Days W: {summary['avg_hold_winners']:.1f}\n"
                f"Avg Hold Days L: {summary['avg_hold_losers']:.1f}"
            )
        self.summary_var.set(summary_text)
        # Compute equity curve DataFrame
        if top_set is None and not has_strategy_filter:
            eq_df = self.model.equity_curve(account_filter, closed_only=closed_only,
                                             start_date=self.start_date, end_date=self.end_date)
        else:
            # Build equity curve from filtered trades
            data: Dict[dt.date, float] = {}
            for idx, trade in enumerate(self.model.trades):
                if top_set is not None and idx not in top_set:
                    continue
                # Strategy filters
                if not matches_strategy_filters(trade):
                    continue
                if not trade.is_closed or trade.exit_date is None or trade.pnl is None:
                    continue
                # Account filter
                if account_filter and account_filter != "all" and trade.account_number != account_filter:
                    continue
                # Date range filter on exit date
                exit_date_dt = trade.exit_date.date()
                if self.start_date and exit_date_dt < self.start_date:
                    continue
                if self.end_date and exit_date_dt > self.end_date:
                    continue
                # Closed-only filter
                if closed_only:
                    if trade.buy_id < 0:
                        continue
                    if self.model.open_qty_by_buy_id.get(trade.buy_id, 0.0) > 1e-8:
                        continue
                data[exit_date_dt] = data.get(exit_date_dt, 0.0) + (trade.pnl or 0.0)
            dates = sorted(data.keys())
            equity_values = []
            cumulative = 0.0
            for d in dates:
                cumulative += data[d]
                equity_values.append(cumulative)
            eq_df = pd.DataFrame({"date": dates, "equity": equity_values})
        
        # Plot modern, aesthetic chart
        self.ax.clear()
        if not eq_df.empty:
            # Convert dates for matplotlib
            dates_dt = pd.to_datetime(eq_df["date"])
            y_values = eq_df["equity"].values
            
            # Plot line with gradient fill
            self.ax.plot(dates_dt, y_values, linewidth=2.5, color='#1f77b4', label='Cumulative P&L', zorder=3)
            self.ax.fill_between(dates_dt, y_values, alpha=0.25, color='#1f77b4', zorder=2)
            
            # Styling
            self.ax.set_xlabel('Date', fontsize=11, fontweight='bold', color='#333333')
            self.ax.set_ylabel('Cumulative P&L ($)', fontsize=11, fontweight='bold', color='#333333')
            self.ax.set_title('Equity Curve', fontsize=13, fontweight='bold', color='#333333', pad=15)
            
            # Professional grid
            self.ax.grid(True, linestyle='-', linewidth=0.6, alpha=0.3, color='#cccccc', zorder=1)
            self.ax.set_axisbelow(True)
            
            # Spine styling
            for spine in self.ax.spines.values():
                spine.set_edgecolor('#cccccc')
                spine.set_linewidth(1)
            
            # Format y-axis as currency
            from matplotlib.ticker import FuncFormatter
            def dollar_formatter(x, pos):
                return f'${x:,.0f}'
            self.ax.yaxis.set_major_formatter(FuncFormatter(dollar_formatter))
            
            # Rotate date labels for readability
            self.fig.autofmt_xdate(rotation=45, ha='right')
            
            # Tight layout
            self.fig.tight_layout()
        else:
            self.ax.text(0.5, 0.5, 'No equity data', transform=self.ax.transAxes, 
                        ha='center', va='center', fontsize=12, color='#999999')
            self.ax.set_title('Equity Curve', fontsize=13, fontweight='bold', color='#333333', pad=15)
        
        # Draw or refresh canvas (use equity_canvas for the equity curve)
        if self.equity_canvas is None:
            from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
            self.equity_canvas = FigureCanvasTkAgg(self.fig, master=self.chart_frame)
            canvas_widget = self.equity_canvas.get_tk_widget()
            canvas_widget.pack(fill=tk.BOTH, expand=True, padx=2, pady=2)
        self.equity_canvas.draw()

    def export_journal(self) -> None:
        """Export the current trade journal to CSV or Excel."""
        if not self.model.trades:
            messagebox.showinfo("No Data", "No trades to export.")
            return
        # Ask user for file path
        filetypes = [
            ("CSV File", "*.csv"),
            ("Excel File", "*.xlsx"),
        ]
        filepath = filedialog.asksaveasfilename(title="Export Journal", defaultextension=".csv", filetypes=filetypes)
        if not filepath:
            return
        # Build DataFrame from trades
        records = []
        for trade in self.model.trades:
            # Retrieve note based on the stable compute_key used for notes
            note_key = self.model.compute_key(trade)
            note_val = self.model.notes.get(note_key, "")
            records.append({
                "Account": trade.account_number,
                "Symbol": trade.symbol,
                "Entry Date": trade.entry_date,
                "Entry Price": trade.entry_price,
                "Exit Date": trade.exit_date,
                "Exit Price": trade.exit_price,
                "Quantity": trade.quantity,
                "P&L": trade.pnl,
                "P&L %": trade.pnl_pct,
                "Hold Days": trade.hold_period,
                "Note": note_val,
            })
        df = pd.DataFrame(records)
        try:
            if filepath.lower().endswith(".xlsx"):
                df.to_excel(filepath, index=False)
            else:
                df.to_csv(filepath, index=False)
        except Exception as e:
            messagebox.showerror("Export Error", f"Failed to export: {e}")
        else:
            messagebox.showinfo("Export Successful", f"Journal exported to {filepath}")


def main() -> None:
    root = tk.Tk()
    app = TradeJournalApp(root)
    root.mainloop()


if __name__ == "__main__":
    main()