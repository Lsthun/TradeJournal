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
import re
import shutil
import sqlite3
import subprocess
import sys
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional, Set

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


def format_strategy_for_table(strategy_str: str) -> str:
    """Format a strategy string for single-line table display.

    Splits on comma, CR/LF, semicolon, and tab, then joins with "; ".
    """
    if not strategy_str:
        return ""
    parts = [p.strip() for p in re.split(r"[,\r\n;\t]+", str(strategy_str)) if p.strip()]
    return "; ".join(parts)

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
        "top_filter_metric": "PnL",
        "compare_spy": False,
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


class Tooltip:
    """Create a tooltip for a Tkinter widget that shows on hover."""
    
    def __init__(self, widget, text: str, delay: int = 500):
        """
        Initialize tooltip.
        
        Args:
            widget: The Tkinter widget to attach the tooltip to
            text: The tooltip text to display
            delay: Delay in milliseconds before showing tooltip
        """
        self.widget = widget
        self.text = text
        self.delay = delay
        self.tipwindow = None
        self.id = None
        self.x = self.y = 0
        self.widget.bind("<Enter>", self.on_enter, add=True)
        self.widget.bind("<Leave>", self.on_leave, add=True)
        self.widget.bind("<Motion>", self.on_motion, add=True)
    
    def on_enter(self, event=None):
        """Schedule tooltip to appear on mouse enter."""
        if self.id:
            self.widget.after_cancel(self.id)
        self.id = self.widget.after(self.delay, self.show_tooltip)
    
    def on_leave(self, event=None):
        """Hide tooltip on mouse leave."""
        if self.id:
            self.widget.after_cancel(self.id)
            self.id = None
        self.hide_tooltip()
    
    def on_motion(self, event=None):
        """Update tooltip position on mouse motion."""
        if self.tipwindow:
            self.x = event.x_root + 10
            self.y = event.y_root + 10
            self.tipwindow.wm_geometry(f"+{self.x}+{self.y}")
    
    def show_tooltip(self):
        """Display the tooltip window."""
        if self.tipwindow or not self.text:
            return
        self.tipwindow = tw = tk.Toplevel(self.widget)
        tw.wm_overrideredirect(True)
        tw.wm_geometry(f"+{self.x}+{self.y}")
        label = tk.Label(tw, text=self.text, background="#ffffe0", relief=tk.SOLID, borderwidth=1, font=("TkDefaultFont", 9))
        label.pack(ipadx=1)
    
    def hide_tooltip(self):
        """Hide and destroy the tooltip window."""
        tw = self.tipwindow
        self.tipwindow = None
        if tw:
            tw.destroy()


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
        # Screenshots keyed by unique trade key (list of dicts with filepath and label)
        # Format: {key: [{"filepath": "path", "label": "description"}, ...]}
        self.screenshots: Dict[tuple, List[Dict[str, str]]] = {}
        # Set of unique transaction keys to detect duplicates across sessions.
        # Keys are stored as tuples: (run_date or run_date.date(), account_number, symbol, quantity, price, amount, action)
        # We store both datetime and date-only variants to tolerate files that omit times while still reducing false positives.
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

    def _save_trade_metadata_before_matching(self) -> dict:
        """Save metadata (notes, screenshots, strategies) indexed by entry-level properties.
        
        Returns a dict with two maps so we can restore even if quantity changes during
        partial fills: "strict" keyed by (acct, symbol, entry_date_iso, entry_price, qty)
        and "relaxed" keyed by (acct, symbol, entry_date_iso, entry_price)."""
        strict_map = {}
        relaxed_map = {}
        for trade in self.trades:
            entry_key = (
                trade.account_number or "",
                trade.symbol or "",
                trade.entry_date.isoformat(),
                round(trade.entry_price, 6),
                round(trade.quantity, 6),
            )
            relaxed_key = (
                trade.account_number or "",
                trade.symbol or "",
                trade.entry_date.isoformat(),
                round(trade.entry_price, 6),
            )
            full_key = self.compute_key(trade)

            metadata = {}
            if full_key in self.notes:
                metadata['note'] = self.notes[full_key]
            if full_key in self.entry_strategies:
                metadata['entry_strategy'] = self.entry_strategies[full_key]
            if full_key in self.exit_strategies:
                metadata['exit_strategy'] = self.exit_strategies[full_key]
            if full_key in self.screenshots:
                metadata['screenshots'] = self.screenshots[full_key]

            if metadata:
                strict_map[entry_key] = metadata
                # Only record a relaxed mapping if not already present to avoid collisions
                relaxed_map.setdefault(relaxed_key, metadata)
        return {"strict": strict_map, "relaxed": relaxed_map}

    def _restore_trade_metadata_after_matching(self, metadata_map: dict) -> None:
        """Restore metadata to trades after re-matching based on entry-level properties.
        
        Uses strict matching first, then falls back to relaxed matching (ignores quantity)
        to preserve strategies/notes for partial fills that change quantity during matching.
        """
        strict_map = metadata_map.get("strict", {}) if isinstance(metadata_map, dict) else {}
        relaxed_map = metadata_map.get("relaxed", {}) if isinstance(metadata_map, dict) else {}

        for trade in self.trades:
            entry_key = (
                trade.account_number or "",
                trade.symbol or "",
                trade.entry_date.isoformat(),
                round(trade.entry_price, 6),
                round(trade.quantity, 6),
            )
            relaxed_key = (
                trade.account_number or "",
                trade.symbol or "",
                trade.entry_date.isoformat(),
                round(trade.entry_price, 6),
            )

            saved_metadata = strict_map.get(entry_key)
            if saved_metadata is None:
                saved_metadata = relaxed_map.get(relaxed_key)

            if saved_metadata:
                full_key = self.compute_key(trade)
                if 'note' in saved_metadata:
                    self.notes[full_key] = saved_metadata['note']
                if 'entry_strategy' in saved_metadata:
                    self.entry_strategies[full_key] = saved_metadata['entry_strategy']
                if 'exit_strategy' in saved_metadata:
                    self.exit_strategies[full_key] = saved_metadata['exit_strategy']
                if 'screenshots' in saved_metadata:
                    self.screenshots[full_key] = saved_metadata['screenshots']

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
        # Rebuild duplicate keys from the currently loaded transactions to avoid stale entries
        # Keys use both datetime and date-only run_date to tolerate files with/without times
        # Round floats to avoid floating-point precision issues
        existing_keys: set = set()
        for tx in self.transactions:
            qty_r = round(tx.quantity, 6)
            price_r = round(tx.price, 6)
            amount_r = round(tx.amount, 6)
            k_dt = (tx.run_date, tx.account_number, tx.symbol, qty_r, price_r, amount_r, tx.action)
            existing_keys.add(k_dt)
            if isinstance(tx.run_date, dt.datetime):
                existing_keys.add((tx.run_date.date(), tx.account_number, tx.symbol, qty_r, price_r, amount_r, tx.action))
        # Reset seen_tx_keys to the rebuilt set (removes stale keys for deleted trades)
        self.seen_tx_keys = set(existing_keys)
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
                    # Handle Excel serial date format (integers like 46044)
                    if run_date is None:
                        try:
                            serial = int(run_date_str)
                            # Excel serial dates: day 1 = 1900-01-01, but Excel incorrectly treats 1900 as leap year
                            # For dates after Feb 28 1900, we need to subtract 2 from the ordinal
                            # datetime.date(1899, 12, 30) is the effective epoch for Excel dates
                            run_date = dt.datetime(1899, 12, 30) + dt.timedelta(days=serial)
                        except ValueError:
                            pass
                    if run_date is None:
                        continue
                    # Helper to safely get a cell string
                    def safe_get(idx: Optional[int]) -> str:
                        return row[idx].strip() if (idx is not None and idx < len(row)) else ""
                    
                    # Helper to convert scientific notation account numbers to proper integers
                    def normalize_account_number(s: str) -> str:
                        """Convert account numbers that may be in scientific notation (e.g., 6.53E+08) to integer strings."""
                        s = s.strip()
                        if not s:
                            return s
                        # Check if it looks like scientific notation
                        if 'e' in s.lower():
                            try:
                                # Parse as float and convert to integer string
                                return str(int(float(s)))
                            except (ValueError, OverflowError):
                                return s
                        return s
                    
                    account = safe_get(account_idx)
                    acct_num = normalize_account_number(safe_get(acct_num_idx))
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
                        # Handle Excel serial date format for settlement date
                        if settlement_date is None:
                            try:
                                serial = int(settle_str)
                                settlement_date = dt.datetime(1899, 12, 30) + dt.timedelta(days=serial)
                            except ValueError:
                                pass
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
                    # Compute duplicate keys (datetime and date-only)
                    # Round floats to avoid floating-point precision issues in comparisons
                    qty_r = round(qty, 6)
                    price_r = round(price, 6)
                    amount_r = round(amount, 6)
                    key_dt = (run_date, acct_num, symbol, qty_r, price_r, amount_r, action)
                    key_date = (run_date.date(), acct_num, symbol, qty_r, price_r, amount_r, action)
                    # Check duplicates both across sessions and within this load
                    if (key_dt in existing_keys or key_date in existing_keys or
                        key_dt in new_keys or key_date in new_keys):
                        self.duplicate_transactions.append(tx)
                        self.duplicate_count += 1
                        continue
                    # Accept this transaction and record keys for this file (store both variants)
                    new_keys.add(key_dt)
                    new_keys.add(key_date)
                    self.transactions.append(tx)
            # Finished reading file
            # Update global seen keys with new keys from this import
            self.seen_tx_keys.update(new_keys)
        except Exception as e:
            raise RuntimeError(f"Failed to load CSV: {e}")
        # Save metadata before re-matching
        metadata_map = self._save_trade_metadata_before_matching()
        # Re-match trades based on the updated transaction list
        self._match_trades()
        # Restore metadata to new trades after re-matching
        self._restore_trade_metadata_after_matching(metadata_map)

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
            # Normalize seen_tx_keys to include action and both datetime/date variants for robustness
            # Also round floats to 6 decimal places for consistent comparison
            normalized_keys = set()
            for k in self.seen_tx_keys:
                if not isinstance(k, tuple):
                    continue
                if len(k) >= 6:
                    run_part = k[0]
                    acct = k[1]
                    sym = k[2]
                    qty = round(k[3], 6) if isinstance(k[3], (int, float)) else k[3]
                    price = round(k[4], 6) if isinstance(k[4], (int, float)) else k[4]
                    amt = round(k[5], 6) if isinstance(k[5], (int, float)) else k[5]
                    action = k[6] if len(k) >= 7 else None
                    base_tuple = (run_part, acct, sym, qty, price, amt, action)
                    normalized_keys.add(base_tuple)
                    if isinstance(run_part, dt.datetime):
                        normalized_keys.add((run_part.date(), acct, sym, qty, price, amt, action))
                    elif isinstance(run_part, dt.date):
                        normalized_keys.add((run_part, acct, sym, qty, price, amt, action))
            self.seen_tx_keys = normalized_keys
            
            # Migrate old screenshot format (string) to new format (list of dicts)
            self._migrate_screenshots_format()
            
            # Reset buy id counter and re-match trades (metadata preservation not needed on load_state
            # since we're loading everything fresh, but we initialize trade list for safety)
            self.next_buy_id = 0
            self.trades = []
            self.open_positions = {}
            self._match_trades()
            return data.get('filter_state', {})
        except Exception:
            # If loading fails, silently ignore and start fresh
            self.clear()
            return {}
    
    def _migrate_screenshots_format(self) -> None:
        """Convert old screenshot format (string paths) to new format (list of dicts with labels)."""
        migrated = {}
        for key, value in self.screenshots.items():
            if isinstance(value, str):
                # Old format: single string path -> new format: list with one dict
                migrated[key] = [{"filepath": value, "label": os.path.basename(value)}]
            elif isinstance(value, list) and value and isinstance(value[0], str):
                # Old format: list of strings -> new format: list of dicts
                migrated[key] = [{"filepath": fp, "label": os.path.basename(fp)} for fp in value]
            else:
                # Already new format or empty
                migrated[key] = value if isinstance(value, list) else []
        self.screenshots = migrated

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
        # Sort by run_date first, then buys before sells (0 for buys, 1 for sells) to ensure
        # buys are processed before sells on the same day
        sorted_txs = sorted(self.transactions, key=lambda tx: (tx.run_date, 0 if tx.is_buy else 1))
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
                        start_date: Optional[dt.date] = None, end_date: Optional[dt.date] = None,
                        exit_start_date: Optional[dt.date] = None, exit_end_date: Optional[dt.date] = None) -> Dict[str, float]:
        """Compute summary statistics for trades.

        Filters trades by account_number (if ``account_filter`` is provided), by date range on entry_date
        (inclusive) and exit_date (inclusive). When ``closed_only`` is True, trades without an exit are
        skipped, but partial exits are still counted. Only trades with exit_date and status==CLOSED are
        counted as closed trades for statistics.
        
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
            # Date range filter on exit date (inclusive)
            if exit_start_date and trade.exit_date and trade.exit_date.date() < exit_start_date:
                continue
            if exit_end_date and trade.exit_date and trade.exit_date.date() > exit_end_date:
                continue
            # Closed-only filter now includes partial exits; only skip trades with no exit
            if closed_only and not trade.exit_date:
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
                     start_date: Optional[dt.date] = None, end_date: Optional[dt.date] = None,
                     exit_start_date: Optional[dt.date] = None, exit_end_date: Optional[dt.date] = None) -> pd.DataFrame:
        """Return a DataFrame representing the cumulative equity over time.

        Each closed trade contributes its P&L at the exit date. Trades are filtered by account,
        date range on entry date (inclusive) and exit date (inclusive). When ``closed_only`` is True,
        trades without an exit are skipped, but partial exits are still included. Dates outside the
        specified range are excluded. The DataFrame contains columns 'date' and 'equity', sorted chronologically.
        """
        data: Dict[dt.date, float] = {}
        for trade in self.trades:
            if not trade.is_closed:
                continue
            if account_filter and account_filter != "all" and trade.account_number != account_filter:
                continue
            # Date range filter on entry date (inclusive) - matches table filtering
            if start_date and trade.entry_date.date() < start_date:
                continue
            if end_date and trade.entry_date.date() > end_date:
                continue
            # Date range filter on exit date (inclusive)
            if exit_start_date and trade.exit_date and trade.exit_date.date() < exit_start_date:
                continue
            if exit_end_date and trade.exit_date and trade.exit_date.date() > exit_end_date:
                continue
            if closed_only and not trade.exit_date:
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
        # Text area sizing state (initialize before building UI so widgets can read defaults)
        self.entry_text_default_height = 3
        self.exit_text_default_height = 3
        self.entry_text_height = tk.IntVar(value=self.entry_text_default_height)
        self.exit_text_height = tk.IntVar(value=self.exit_text_default_height)
        # Symbol filter (comma-separated list) needs to exist before building UI
        self.symbol_filter_var = tk.StringVar(value="")
        # Analysis state
        self.analysis_top_n_var = tk.StringVar(value="10")
        self.analysis_min_trades_var = tk.StringVar(value="20")
        self.analysis_closed_only_var = tk.BooleanVar(value=True)
        self.analysis_attribution_var = tk.StringVar(value="Split")
        self.analysis_sort_entry_var = tk.StringVar(value="Total PnL")
        self.analysis_sort_exit_var = tk.StringVar(value="Total PnL")
        self.analysis_sort_combo_var = tk.StringVar(value="Total PnL")
        self.analysis_date_mode_var = tk.StringVar(value="Exit Date")
        self.analysis_include_unspecified_var = tk.BooleanVar(value=False)
        self.analysis_start_date_var = tk.StringVar(value="")
        self.analysis_end_date_var = tk.StringVar(value="")
        # Analysis Two state
        self.analysis2_starting_balances: Dict[str, Dict[int, float]] = {}
        self.analysis2_year_var = tk.StringVar(value="")
        self.analysis2_start_balance_var = tk.StringVar(value="")
        self.analysis2_account_label_var = tk.StringVar(value="all")
        self.analysis2_avg_visible = True
        self.analysis2_monthly_visible = True
        self.open_only_var = tk.BooleanVar(value=False)
        self.include_open_equity_var = tk.BooleanVar(value=False)
        # UI elements
        self._build_ui()
        # Sorting state: which column and whether descending
        self.sort_by: Optional[str] = None
        self.sort_descending: bool = False
        # Mapping from Treeview item id to trade key for notes/screenshots
        self.id_to_key: Dict[str, tuple] = {}
        # Mapping from group row id to list of trade indices (used for deletion)
        self.group_id_to_indices: Dict[str, List[int]] = {}
        self._analysis_selected: Optional[Tuple[str, dict]] = None
        # Date filter boundaries (dt.date objects)
        self.start_date: Optional[dt.date] = None
        self.end_date: Optional[dt.date] = None
        # Chart-related state
        self.current_chart_symbol: Optional[str] = None
        self.chart_canvas = None
        # Treeview tooltip state
        self._tree_tooltip_win = None
        self._tree_tooltip_label = None
        self._tree_tooltip_last = (None, None, None)  # (item_id, col_name, text)
        # Keep only one date picker window alive at a time
        self._date_picker_window: Optional[tk.Toplevel] = None
        self._date_picker_allowed_widgets: Set[tk.Widget] = set()
        # Accepted input date formats for typed filters
        self.accepted_date_formats = ["%Y-%m-%d", "%Y/%m/%d", "%m/%d/%Y", "%m/%d/%y"]
        # Load persisted data (if available)
        self.load_persisted_data()
        # Register handler to save on close
        self.root.protocol("WM_DELETE_WINDOW", self.on_close)
        # Global click handler to close date picker when clicking outside
        self.root.bind_all("<Button-1>", self._on_click_close_picker, add="+")

    def _build_ui(self) -> None:
        """Construct the user interface."""
        # Top frame for file actions and account filter - using grid for better responsive layout
        top_frame = ttk.Frame(self.root)
        top_frame.pack(side=tk.TOP, fill=tk.X, padx=5, pady=5)

        # Collapse button for top controls
        self.top_controls_visible = True
        collapse_btn = ttk.Button(top_frame, text="−", width=2, command=self._toggle_top_controls)
        collapse_btn.grid(row=0, column=0, padx=(0, 5), pady=2, sticky="w")
        self.top_collapse_btn = collapse_btn
        self.top_controls_frame = top_frame
        
        # Make all columns expand equally to use available space
        for i in range(13):
            top_frame.columnconfigure(i, weight=1)

        # Row 0: Main action buttons
        load_btn = ttk.Button(top_frame, text="Load CSV", command=self.load_csv)
        load_btn.grid(row=0, column=0, padx=(0, 5), pady=2)

        export_btn = ttk.Button(top_frame, text="Export Journal", command=self.export_journal)
        export_btn.grid(row=0, column=1, padx=(0, 5), pady=2)

        # Button to add a manual transaction
        add_tx_btn = ttk.Button(top_frame, text="Add Transaction", command=self.add_transaction_dialog)
        add_tx_btn.grid(row=0, column=2, padx=(0, 5), pady=2)

        # Button to edit selected transaction
        edit_tx_btn = ttk.Button(top_frame, text="Edit Transaction", command=self.edit_selected_transaction)
        edit_tx_btn.grid(row=0, column=3, padx=(0, 5), pady=2)

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

        # Global refresh for open positions pricing
        self.refresh_open_prices_global_btn = ttk.Button(top_frame, text="Refresh Open Prices", command=self.refresh_open_prices_global)
        self.refresh_open_prices_global_btn.grid(row=0, column=8, padx=(0, 5), pady=2)

        # Sync Alerts moved next to chart toggle on row 4

        # Row 1: Group toggle + filters + open controls
        self.group_var = tk.BooleanVar(value=True)
        group_check = ttk.Checkbutton(top_frame, text="Group by symbol", variable=self.group_var, command=self.on_group_change)
        group_check.grid(row=1, column=0, padx=(0, 4), pady=2, sticky="w")

        self.open_only_check = ttk.Checkbutton(top_frame, text="Open positions only", variable=self.open_only_var, command=self.on_open_only_change)
        self.open_only_check.grid(row=1, column=1, padx=(0, 4), pady=2, sticky="w")

        ttk.Label(top_frame, text="Top N:").grid(row=1, column=2, padx=(0, 2), pady=2, sticky="e")
        
        # Load saved settings
        saved_settings = load_chart_settings()
        
        self.top_n_var = tk.StringVar(value=saved_settings.get("top_n", ""))
        top_n_entry = ttk.Entry(top_frame, textvariable=self.top_n_var, width=5)
        top_n_entry.grid(row=1, column=3, padx=(0, 2), pady=2)
        # Filter type: None, Winners, Losers
        self.top_filter_type_var = tk.StringVar(value=saved_settings.get("top_filter_type", "None"))
        top_filter_combo = ttk.Combobox(top_frame, textvariable=self.top_filter_type_var,
                                         values=["None", "Winners", "Losers"], state="readonly", width=8)
        top_filter_combo.grid(row=1, column=4, padx=(0, 2), pady=2)
        # Metric for winners/losers: PnL or PnL %
        ttk.Label(top_frame, text="by:").grid(row=1, column=5, padx=(0, 2), pady=2, sticky="e")
        self.top_filter_metric_var = tk.StringVar(value=saved_settings.get("top_filter_metric", "PnL"))
        top_metric_combo = ttk.Combobox(top_frame, textvariable=self.top_filter_metric_var,
                                         values=["PnL", "PnL %"], state="readonly", width=8)
        top_metric_combo.grid(row=1, column=6, padx=(0, 2), pady=2)
        apply_top_btn = ttk.Button(top_frame, text="Apply", command=self.on_top_filter_change)
        apply_top_btn.grid(row=1, column=7, padx=(0, 5), pady=2)

        # Checkbox to show only fully closed positions
        self.closed_only_var = tk.BooleanVar(value=False)
        closed_check = ttk.Checkbutton(top_frame, text="Closed positions only", variable=self.closed_only_var, command=self.on_closed_filter_change)
        closed_check.grid(row=1, column=8, padx=(0, 5), pady=2, sticky="w")

        # Toggle to include open P&L in summary/equity
        include_open_top = ttk.Checkbutton(top_frame, text="Include Open P&L", variable=self.include_open_equity_var, command=self.on_include_open_equity_change)
        include_open_top.grid(row=1, column=9, padx=(0, 5), pady=2, sticky="w")

        # Row 2: Entry date filter fields
        ttk.Label(top_frame, text="Entry Start (preferred M/D/YYYY):").grid(row=2, column=0, padx=(0, 2), pady=2, sticky="e")
        self.start_date_var = tk.StringVar(value="")
        start_entry = ttk.Entry(top_frame, textvariable=self.start_date_var, width=12)
        # Bind a mouse click to open date picker
        start_entry.bind("<Button-1>", lambda e: self.open_date_picker(self.start_date_var, source_widgets=[start_entry, start_pick_btn]))
        start_entry.bind("<Return>", lambda e: self.apply_date_filter())
        start_entry.grid(row=2, column=1, padx=(0, 2), pady=2)
        # Button to open date picker explicitly
        start_pick_btn = ttk.Button(top_frame, text="📅", width=3, command=lambda: self.open_date_picker(self.start_date_var, source_widgets=[start_entry, start_pick_btn]))
        start_pick_btn.grid(row=2, column=2, padx=(0, 5), pady=2)
        ttk.Label(top_frame, text="Entry End (preferred M/D/YYYY):").grid(row=2, column=3, padx=(0, 2), pady=2, sticky="e")
        self.end_date_var = tk.StringVar(value="")
        end_entry = ttk.Entry(top_frame, textvariable=self.end_date_var, width=12)
        end_entry.bind("<Button-1>", lambda e: self.open_date_picker(self.end_date_var, source_widgets=[end_entry, end_pick_btn]))
        end_entry.bind("<Return>", lambda e: self.apply_date_filter())
        end_entry.grid(row=2, column=4, padx=(0, 2), pady=2)
        end_pick_btn = ttk.Button(top_frame, text="📅", width=3, command=lambda: self.open_date_picker(self.end_date_var, source_widgets=[end_entry, end_pick_btn]))
        end_pick_btn.grid(row=2, column=5, padx=(0, 5), pady=2)
        apply_date_btn = ttk.Button(top_frame, text="Apply Date Filter", command=self.apply_date_filter)
        apply_date_btn.grid(row=2, column=6, padx=(0, 5), pady=2)
        entry_clear_btn = ttk.Button(top_frame, text="✖", width=2, command=self.clear_entry_date_filter)
        entry_clear_btn.grid(row=2, column=7, padx=(0, 5), pady=2, sticky="w")

        # Row 3: Exit date filter fields
        ttk.Label(top_frame, text="Exit Start (preferred M/D/YYYY):").grid(row=3, column=0, padx=(0, 2), pady=2, sticky="e")
        self.exit_start_date_var = tk.StringVar(value="")
        exit_start_entry = ttk.Entry(top_frame, textvariable=self.exit_start_date_var, width=12)
        exit_start_entry.bind("<Button-1>", lambda e: self.open_date_picker(self.exit_start_date_var, source_widgets=[exit_start_entry, exit_start_pick_btn]))
        exit_start_entry.bind("<Return>", lambda e: self.apply_date_filter())
        exit_start_entry.grid(row=3, column=1, padx=(0, 2), pady=2)
        exit_start_pick_btn = ttk.Button(top_frame, text="📅", width=3, command=lambda: self.open_date_picker(self.exit_start_date_var, source_widgets=[exit_start_entry, exit_start_pick_btn]))
        exit_start_pick_btn.grid(row=3, column=2, padx=(0, 5), pady=2)
        ttk.Label(top_frame, text="Exit End (preferred M/D/YYYY):").grid(row=3, column=3, padx=(0, 2), pady=2, sticky="e")
        self.exit_end_date_var = tk.StringVar(value="")
        exit_end_entry = ttk.Entry(top_frame, textvariable=self.exit_end_date_var, width=12)
        exit_end_entry.bind("<Button-1>", lambda e: self.open_date_picker(self.exit_end_date_var, source_widgets=[exit_end_entry, exit_end_pick_btn]))
        exit_end_entry.bind("<Return>", lambda e: self.apply_date_filter())
        exit_end_entry.grid(row=3, column=4, padx=(0, 2), pady=2)
        exit_end_pick_btn = ttk.Button(top_frame, text="📅", width=3, command=lambda: self.open_date_picker(self.exit_end_date_var, source_widgets=[exit_end_entry, exit_end_pick_btn]))
        exit_end_pick_btn.grid(row=3, column=5, padx=(0, 5), pady=2)
        # Internal parsed date holders
        self.start_date = None
        self.end_date = None
        self.exit_start_date = None
        self.exit_end_date = None
        # Reuse the same apply button for both entry and exit date ranges
        ttk.Button(top_frame, text="Apply Date Filter", command=self.apply_date_filter).grid(row=3, column=6, padx=(0, 5), pady=2)
        exit_clear_btn = ttk.Button(top_frame, text="✖", width=2, command=self.clear_exit_date_filter)
        exit_clear_btn.grid(row=3, column=7, padx=(0, 5), pady=2, sticky="w")

        # Row 4: Strategy filters, clear filters and toggle table buttons
        ttk.Label(top_frame, text="Filter Entry:").grid(row=4, column=0, padx=(0, 2), pady=2, sticky="e")
        self.entry_strategy_filter_var = tk.StringVar(value="all")
        self.entry_strategy_filter_combo = ttk.Combobox(top_frame, textvariable=self.entry_strategy_filter_var, width=22)
        self.entry_strategy_filter_combo.grid(row=4, column=1, columnspan=2, padx=(0, 5), pady=2, sticky="ew")
        self.entry_strategy_filter_combo.bind("<<ComboboxSelected>>", self.on_strategy_filter_change)
        self.entry_strategy_filter_combo.bind("<KeyRelease>", lambda e: self._on_strategy_combo_keyrelease(e, is_entry=True))

        ttk.Label(top_frame, text="Filter Exit:").grid(row=4, column=3, padx=(0, 2), pady=2, sticky="e")
        self.exit_strategy_filter_var = tk.StringVar(value="all")
        self.exit_strategy_filter_combo = ttk.Combobox(top_frame, textvariable=self.exit_strategy_filter_var, width=22)
        self.exit_strategy_filter_combo.grid(row=4, column=4, columnspan=2, padx=(0, 5), pady=2, sticky="ew")
        self.exit_strategy_filter_combo.bind("<<ComboboxSelected>>", self.on_strategy_filter_change)
        self.exit_strategy_filter_combo.bind("<KeyRelease>", lambda e: self._on_strategy_combo_keyrelease(e, is_entry=False))

        clear_filter_btn = ttk.Button(top_frame, text="Clear Filters", command=self.clear_filters)
        clear_filter_btn.grid(row=4, column=6, padx=(0, 5), pady=2, sticky="w")
        
        self.table_visible = tk.BooleanVar(value=True)
        self.toggle_btn = ttk.Button(top_frame, text="Hide Table", command=self.toggle_table_visibility)
        self.toggle_btn.grid(row=4, column=7, padx=(0, 5), pady=2, sticky="w")
        self.chart_visible = tk.BooleanVar(value=True)
        self.toggle_chart_btn = ttk.Button(top_frame, text="Hide Chart", command=self.toggle_chart_visibility)
        self.toggle_chart_btn.grid(row=4, column=8, padx=(0, 5), pady=2, sticky="w")

        sync_alerts_btn = ttk.Button(top_frame, text="Sync Alerts", command=self.sync_alerts_to_entry_strategies)
        sync_alerts_btn.grid(row=4, column=9, padx=(0, 5), pady=2, sticky="w")

        # Row 5: Symbol filter (comma-separated, apply via button or Enter)
        ttk.Label(top_frame, text="Filter Symbol(s):").grid(row=5, column=0, padx=(0, 2), pady=2, sticky="e")
        symbol_entry = ttk.Entry(top_frame, textvariable=self.symbol_filter_var, width=22)
        symbol_entry.grid(row=5, column=1, columnspan=2, padx=(0, 5), pady=2, sticky="ew")
        symbol_entry.bind("<Return>", lambda e: self.apply_symbol_filter())
        apply_symbol_btn = ttk.Button(top_frame, text="Apply →", command=self.apply_symbol_filter)
        apply_symbol_btn.grid(row=5, column=3, padx=(0, 5), pady=2, sticky="w")
        ttk.Label(top_frame, text="Use commas: SPY,QQQ").grid(row=5, column=4, columnspan=3, padx=(0, 5), pady=2, sticky="w")

        # Main frame with notebook (tabs)
        main_frame = ttk.Frame(self.root)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)

        # Create notebook for tabs
        self.notebook = ttk.Notebook(main_frame)
        self.notebook.pack(fill=tk.BOTH, expand=True)
        self.notebook.bind("<<NotebookTabChanged>>", self._on_tab_changed)

        # TAB 1: Journal (original content)
        journal_frame = ttk.Frame(self.notebook)
        self.notebook.add(journal_frame, text="Journal")
        self.journal_tab = journal_frame

        # TAB 2: Charts
        chart_frame = ttk.Frame(self.notebook)
        self.notebook.add(chart_frame, text="Charts")
        self._build_chart_tab(chart_frame)

        # TAB 3: Analysis
        analysis_frame = ttk.Frame(self.notebook)
        self.notebook.add(analysis_frame, text="Analysis")
        self._build_analysis_tab(analysis_frame)

        # TAB 4: Analysis Two
        analysis_two_frame = ttk.Frame(self.notebook)
        self.notebook.add(analysis_two_frame, text="Analysis 2")
        self._build_analysis_two_tab(analysis_two_frame)

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
        
        # Entry Strategy label and resizable text
        ttk.Label(right_scrollable_frame, text="Entry Strategy:").pack(anchor="w")
        ttk.Label(right_scrollable_frame, text="(comma-separated for multiple)", font=("TkDefaultFont", 8), foreground="gray").pack(anchor="w")
        entry_container = ttk.Frame(right_scrollable_frame)
        entry_container.pack(fill=tk.X, pady=(0, 5))
        self.entry_strategy_text = tk.Text(entry_container, height=self.entry_text_height.get(), width=30, wrap="word")
        self.entry_strategy_text.pack(side="left", fill=tk.BOTH, expand=True)
        self.entry_strategy_text.bind("<KeyRelease>", lambda e: self._auto_save_fields())
        entry_grip = ttk.Sizegrip(entry_container)
        entry_grip.pack(side="right", anchor="se")
        def _entry_start_drag(event):
            entry_grip._start_y = event.y_root
        def _entry_drag(event):
            dy = event.y_root - getattr(entry_grip, "_start_y", event.y_root)
            lines = max(2, self.entry_text_height.get() + int(dy / 18))
            if lines != self.entry_text_height.get():
                self.entry_text_height.set(lines)
                self.entry_strategy_text.configure(height=lines)
                self._auto_save_fields()
            entry_grip._start_y = event.y_root
        entry_grip.bind("<ButtonPress-1>", _entry_start_drag)
        entry_grip.bind("<B1-Motion>", _entry_drag)
        
        # Exit Strategy label and resizable text
        ttk.Label(right_scrollable_frame, text="Exit Strategy:").pack(anchor="w")
        ttk.Label(right_scrollable_frame, text="(comma-separated for multiple)", font=("TkDefaultFont", 8), foreground="gray").pack(anchor="w")
        exit_container = ttk.Frame(right_scrollable_frame)
        exit_container.pack(fill=tk.X, pady=(0, 5))
        self.exit_strategy_text = tk.Text(exit_container, height=self.exit_text_height.get(), width=30, wrap="word")
        self.exit_strategy_text.pack(side="left", fill=tk.BOTH, expand=True)
        self.exit_strategy_text.bind("<KeyRelease>", lambda e: self._auto_save_fields())
        exit_grip = ttk.Sizegrip(exit_container)
        exit_grip.pack(side="right", anchor="se")
        def _exit_start_drag(event):
            exit_grip._start_y = event.y_root
        def _exit_drag(event):
            dy = event.y_root - getattr(exit_grip, "_start_y", event.y_root)
            lines = max(2, self.exit_text_height.get() + int(dy / 18))
            if lines != self.exit_text_height.get():
                self.exit_text_height.set(lines)
                self.exit_strategy_text.configure(height=lines)
                self._auto_save_fields()
            exit_grip._start_y = event.y_root
        exit_grip.bind("<ButtonPress-1>", _exit_start_drag)
        exit_grip.bind("<B1-Motion>", _exit_drag)
        
        reset_sizes_btn = ttk.Button(right_scrollable_frame, text="Reset Text Sizes", command=self._reset_text_sizes)
        reset_sizes_btn.pack(anchor="w", pady=(0, 5))
        
        # Note label and text
        ttk.Label(right_scrollable_frame, text="Trade Note:").pack(anchor="w")
        self.note_text = tk.Text(right_scrollable_frame, height=5, width=30)
        self.note_text.pack(fill=tk.X, pady=(0, 5))
        self.note_text.bind("<KeyRelease>", lambda e: self._auto_save_fields())
        # Button to add screenshot
        add_ss_btn = ttk.Button(right_scrollable_frame, text="Add Screenshot", command=self.add_screenshot)
        add_ss_btn.pack(anchor="w", pady=(5, 0))
        # Button to bulk scan screenshots from a folder
        scan_ss_btn = ttk.Button(right_scrollable_frame, text="Scan Screenshot Folder", command=self.scan_screenshot_folder)
        scan_ss_btn.pack(anchor="w", pady=(0, 0))
        # Button to bulk scan note text files from a folder
        scan_notes_btn = ttk.Button(right_scrollable_frame, text="Scan Notes Folder", command=self.scan_notes_folder)
        scan_notes_btn.pack(anchor="w", pady=(0, 0))
        # Label to display screenshot count
        ttk.Label(right_scrollable_frame, text="Screenshots:").pack(anchor="w", pady=(10, 0))
        self.screenshot_var = tk.StringVar(value="")
        self.screenshot_label = ttk.Label(right_scrollable_frame, textvariable=self.screenshot_var, foreground="blue")
        self.screenshot_label.pack(anchor="w")
        # Image preview label (for displaying the screenshot)
        self.screenshot_preview_label = ttk.Label(right_scrollable_frame)
        self.screenshot_preview_label.pack(anchor="w", pady=(5, 5))
        # Button to view screenshots in a zoomed window (placed directly under preview)
        view_ss_btn = ttk.Button(right_scrollable_frame, text="View/Remove Screenshots", command=self.view_screenshots)
        view_ss_btn.pack(anchor="w", pady=(0, 8))

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

        self.include_open_equity_check = ttk.Checkbutton(top_frame, text="Include Open P&L", variable=self.include_open_equity_var, command=self.update_summary_and_chart)
        self.include_open_equity_check.pack(side=tk.LEFT, padx=(0, 5))

        self.refresh_open_prices_btn = ttk.Button(top_frame, text="Refresh Open Prices", command=self.refresh_open_prices)
        self.refresh_open_prices_btn.pack(side=tk.LEFT, padx=(0, 5))

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

    def _build_analysis_tab(self, parent_frame: ttk.Frame) -> None:
        parent_frame.columnconfigure(0, weight=1)
        parent_frame.rowconfigure(1, weight=1)
        parent_frame.rowconfigure(2, weight=1)
        controls = ttk.LabelFrame(parent_frame, text="Filters")
        controls.grid(row=0, column=0, sticky="ew", padx=5, pady=5)
        for i in range(6):
            controls.columnconfigure(i, weight=1)

        ttk.Label(controls, text="Account:").grid(row=0, column=0, padx=2, pady=2, sticky="e")
        self.analysis_account_var = tk.StringVar(value="all")
        self.analysis_account_combo = ttk.Combobox(controls, textvariable=self.analysis_account_var, state="readonly", width=15)
        self.analysis_account_combo.grid(row=0, column=1, padx=2, pady=2, sticky="w")
        self.analysis_account_combo.bind("<<ComboboxSelected>>", lambda e: self.refresh_analysis_view())

        ttk.Label(controls, text="Start Date:").grid(row=0, column=2, padx=2, pady=2, sticky="e")
        ttk.Entry(controls, textvariable=self.analysis_start_date_var, width=12).grid(row=0, column=3, padx=2, pady=2, sticky="w")
        ttk.Label(controls, text="End Date:").grid(row=0, column=4, padx=2, pady=2, sticky="e")
        ttk.Entry(controls, textvariable=self.analysis_end_date_var, width=12).grid(row=0, column=5, padx=2, pady=2, sticky="w")

        ttk.Label(controls, text="Top N:").grid(row=1, column=0, padx=2, pady=2, sticky="e")
        ttk.Entry(controls, textvariable=self.analysis_top_n_var, width=6).grid(row=1, column=1, padx=2, pady=2, sticky="w")
        ttk.Label(controls, text="Min Trades:").grid(row=1, column=2, padx=2, pady=2, sticky="e")
        ttk.Entry(controls, textvariable=self.analysis_min_trades_var, width=6).grid(row=1, column=3, padx=2, pady=2, sticky="w")

        ttk.Checkbutton(controls, text="Closed only", variable=self.analysis_closed_only_var, command=self.refresh_analysis_view).grid(row=1, column=4, padx=2, pady=2, sticky="w")
        ttk.Checkbutton(controls, text="Include Unspecified", variable=self.analysis_include_unspecified_var, command=self.refresh_analysis_view).grid(row=1, column=5, padx=2, pady=2, sticky="w")

        ttk.Label(controls, text="Attribution:").grid(row=2, column=0, padx=2, pady=2, sticky="e")
        attrib_combo = ttk.Combobox(controls, textvariable=self.analysis_attribution_var, state="readonly", values=["Split", "Full", "Primary"], width=10)
        attrib_combo.grid(row=2, column=1, padx=2, pady=2, sticky="w")
        attrib_combo.bind("<<ComboboxSelected>>", lambda e: self.refresh_analysis_view())

        ttk.Label(controls, text="Date Mode:").grid(row=2, column=2, padx=2, pady=2, sticky="e")
        date_combo = ttk.Combobox(controls, textvariable=self.analysis_date_mode_var, state="readonly", values=["Exit Date", "Entry Date"], width=12)
        date_combo.grid(row=2, column=3, padx=2, pady=2, sticky="w")
        date_combo.bind("<<ComboboxSelected>>", lambda e: self.refresh_analysis_view())

        ttk.Label(controls, text="Sort Entry by:").grid(row=3, column=0, padx=2, pady=2, sticky="e")
        ttk.Label(controls, text="Sort Exit by:").grid(row=3, column=2, padx=2, pady=2, sticky="e")
        ttk.Label(controls, text="Sort Combo by:").grid(row=3, column=4, padx=2, pady=2, sticky="e")
        sort_choices = ["Total PnL", "Avg PnL", "Win Rate", "Profit Factor", "Expectancy", "Trades", "Avg Hold"]
        entry_sort = ttk.Combobox(controls, textvariable=self.analysis_sort_entry_var, state="readonly", values=sort_choices, width=12)
        exit_sort = ttk.Combobox(controls, textvariable=self.analysis_sort_exit_var, state="readonly", values=sort_choices, width=12)
        combo_sort = ttk.Combobox(controls, textvariable=self.analysis_sort_combo_var, state="readonly", values=sort_choices, width=12)
        entry_sort.grid(row=3, column=1, padx=2, pady=2, sticky="w")
        exit_sort.grid(row=3, column=3, padx=2, pady=2, sticky="w")
        combo_sort.grid(row=3, column=5, padx=2, pady=2, sticky="w")
        for widget in (entry_sort, exit_sort, combo_sort):
            widget.bind("<<ComboboxSelected>>", lambda e: self.refresh_analysis_view())

        refresh_btn = ttk.Button(controls, text="Refresh", command=self.refresh_analysis_view)
        refresh_btn.grid(row=4, column=5, padx=2, pady=4, sticky="e")

        trees_frame = ttk.Frame(parent_frame)
        trees_frame.grid(row=1, column=0, sticky="nsew", padx=5, pady=5)
        trees_frame.columnconfigure(0, weight=1)
        trees_frame.columnconfigure(1, weight=1)
        trees_frame.rowconfigure(0, weight=1)
        trees_frame.rowconfigure(1, weight=0)
        trees_frame.rowconfigure(2, weight=1)

        entry_frame, self.analysis_entry_tree = self._create_analysis_tree(trees_frame, "Entry Strategies")
        exit_frame, self.analysis_exit_tree = self._create_analysis_tree(trees_frame, "Exit Strategies")
        self.analysis_combo_frame, self.analysis_combo_tree = self._create_analysis_tree(trees_frame, "Entry -> Exit Combos")
        entry_frame.grid(row=0, column=0, sticky="nsew", padx=3, pady=3)
        exit_frame.grid(row=0, column=1, sticky="nsew", padx=3, pady=3)

        # Collapsible header for entry->exit combos
        combo_header = ttk.Frame(trees_frame)
        combo_header.grid(row=1, column=0, columnspan=2, sticky="ew", padx=3)
        combo_header.columnconfigure(1, weight=1)
        self.analysis_combo_visible = True
        self.analysis_combo_toggle_btn = ttk.Button(combo_header, text="−", width=2, command=self._toggle_analysis_combo)
        self.analysis_combo_toggle_btn.grid(row=0, column=0, sticky="w")
        ttk.Label(combo_header, text="Entry -> Exit Combos").grid(row=0, column=1, sticky="w")

        self.analysis_combo_frame.grid(row=2, column=0, columnspan=2, sticky="nsew", padx=3, pady=3)

        detail_frame = ttk.LabelFrame(parent_frame, text="Details")
        detail_frame.grid(row=2, column=0, sticky="nsew", padx=5, pady=5)
        detail_frame.columnconfigure(0, weight=1)
        detail_frame.rowconfigure(1, weight=1)
        self.analysis_detail_text = tk.Text(detail_frame, height=5, wrap="word", state="disabled")
        self.analysis_detail_text.grid(row=0, column=0, sticky="nsew", padx=2, pady=2)

        # Trade list showing contributing trades for the selected strategy
        self.analysis_trade_columns = ("idx", "account", "symbol", "entry", "entry_price", "exit", "exit_price", "qty", "pnl", "pnl_pct", "entry_strat", "exit_strat")
        self.analysis_trade_headings = {
            "idx": "#",
            "account": "Account",
            "symbol": "Symbol",
            "entry": "Entry",
            "entry_price": "Entry Px",
            "exit": "Exit",
            "exit_price": "Exit Px",
            "qty": "Qty",
            "pnl": "P&L",
            "pnl_pct": "P&L %",
            "entry_strat": "Entry Strategy",
            "exit_strat": "Exit Strategy",
        }
        self.analysis_detail_tree = ttk.Treeview(detail_frame, columns=self.analysis_trade_columns, show="headings", selectmode="browse")
        for c in self.analysis_trade_columns:
            anchor = tk.CENTER if c in {"qty", "pnl_pct"} else tk.E
            if c in {"entry", "exit"}:
                anchor = tk.W
            self.analysis_detail_tree.heading(c, text=self.analysis_trade_headings[c])
            self.analysis_detail_tree.column(c, width=90 if c not in {"entry_strat", "exit_strat"} else 160, anchor=anchor, stretch=True)
        vsb = ttk.Scrollbar(detail_frame, orient="vertical", command=self.analysis_detail_tree.yview)
        hsb = ttk.Scrollbar(detail_frame, orient="horizontal", command=self.analysis_detail_tree.xview)
        self.analysis_detail_tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.analysis_detail_tree.grid(row=1, column=0, sticky="nsew", padx=(2,0), pady=2)
        vsb.grid(row=1, column=1, sticky="ns", pady=2)
        hsb.grid(row=2, column=0, sticky="ew", padx=2)

        ttk.Button(detail_frame, text="Filter Journal to this Strategy", command=self._filter_journal_from_analysis).grid(row=3, column=0, sticky="e", padx=2, pady=2)

    def _build_analysis_two_tab(self, parent_frame: ttk.Frame) -> None:
        parent_frame.columnconfigure(0, weight=1)
        parent_frame.rowconfigure(1, weight=1)
        parent_frame.rowconfigure(2, weight=0)
        parent_frame.rowconfigure(3, weight=1)

        controls = ttk.LabelFrame(parent_frame, text="Year Settings")
        controls.grid(row=0, column=0, sticky="ew", padx=5, pady=5)
        for i in range(8):
            controls.columnconfigure(i, weight=1)

        ttk.Label(controls, text="Account:").grid(row=0, column=0, padx=2, pady=2, sticky="e")
        self.analysis2_account_combo = ttk.Combobox(controls, textvariable=self.account_var, state="readonly", width=15)
        self.analysis2_account_combo.grid(row=0, column=1, padx=2, pady=2, sticky="w")
        self.analysis2_account_combo.bind("<<ComboboxSelected>>", self.on_account_filter_change)

        ttk.Label(controls, text="Year:").grid(row=0, column=2, padx=2, pady=2, sticky="e")
        self.analysis2_year_combo = ttk.Combobox(controls, textvariable=self.analysis2_year_var, state="readonly", width=8)
        self.analysis2_year_combo.grid(row=0, column=3, padx=2, pady=2, sticky="w")
        self.analysis2_year_combo.bind("<<ComboboxSelected>>", lambda e: self.update_analysis_two_view())

        ttk.Label(controls, text="Starting Balance:").grid(row=0, column=4, padx=2, pady=2, sticky="e")
        self.analysis2_start_balance_entry = ttk.Entry(controls, textvariable=self.analysis2_start_balance_var, width=14)
        self.analysis2_start_balance_entry.grid(row=0, column=5, padx=2, pady=2, sticky="w")

        self.analysis2_save_balance_btn = ttk.Button(controls, text="Save", command=self._analysis2_save_starting_balance)
        self.analysis2_save_balance_btn.grid(row=0, column=6, padx=2, pady=2, sticky="w")
        self.analysis2_clear_balance_btn = ttk.Button(controls, text="Clear", command=self._analysis2_clear_starting_balance)
        self.analysis2_clear_balance_btn.grid(row=0, column=7, padx=2, pady=2, sticky="w")

        self.analysis2_monthly_frame = ttk.LabelFrame(parent_frame, text="Monthly Returns")
        self.analysis2_monthly_frame.grid(row=1, column=0, sticky="nsew", padx=5, pady=5)
        self.analysis2_monthly_frame.columnconfigure(0, weight=1)
        self.analysis2_monthly_frame.rowconfigure(1, weight=1)

        monthly_header = ttk.Frame(self.analysis2_monthly_frame)
        monthly_header.grid(row=0, column=0, sticky="ew")
        monthly_header.columnconfigure(0, weight=1)
        self.analysis2_monthly_toggle_btn = ttk.Button(monthly_header, text="−", width=2, command=self._toggle_analysis2_monthly)
        self.analysis2_monthly_toggle_btn.grid(row=0, column=1, sticky="e", padx=2, pady=2)

        monthly_cols = ("month", "month_return", "cum_return", "prorated_return")
        self.analysis2_monthly_tree = ttk.Treeview(self.analysis2_monthly_frame, columns=monthly_cols, show="headings", height=8)
        headings = {
            "month": "Month",
            "month_return": "Month Return",
            "cum_return": "Cumulative Return",
            "prorated_return": "Pro-Rated Return",
        }
        for c in monthly_cols:
            anchor = tk.W if c == "month" else tk.E
            self.analysis2_monthly_tree.heading(c, text=headings[c])
            self.analysis2_monthly_tree.column(c, width=110 if c != "month" else 90, anchor=anchor, stretch=True)
        m_vsb = ttk.Scrollbar(self.analysis2_monthly_frame, orient="vertical", command=self.analysis2_monthly_tree.yview)
        self.analysis2_monthly_tree.configure(yscrollcommand=m_vsb.set)
        self.analysis2_monthly_tree.grid(row=1, column=0, sticky="nsew")
        m_vsb.grid(row=1, column=1, sticky="ns")

        self.analysis2_avg_frame = ttk.LabelFrame(parent_frame, text="Averages")
        self.analysis2_avg_frame.grid(row=2, column=0, sticky="ew", padx=5, pady=5)
        self.analysis2_avg_frame.columnconfigure(0, weight=1)
        self.analysis2_avg_frame.rowconfigure(1, weight=1)
        avg_header = ttk.Frame(self.analysis2_avg_frame)
        avg_header.grid(row=0, column=0, sticky="ew")
        avg_header.columnconfigure(0, weight=1)
        self.analysis2_avg_toggle_btn = ttk.Button(avg_header, text="−", width=2, command=self._toggle_analysis2_avg)
        self.analysis2_avg_toggle_btn.grid(row=0, column=1, sticky="e", padx=2, pady=2)
        avg_cols = ("year", "avg_gain", "avg_loss", "win_pct", "loss_pct", "wins", "losses", "trades", "lg_gain", "lg_loss", "avg_days_gain", "avg_days_loss")
        self.analysis2_avg_tree = ttk.Treeview(self.analysis2_avg_frame, columns=avg_cols, show="headings", height=1)
        avg_headings = {
            "year": "Year",
            "avg_gain": "Avg. Gain %",
            "avg_loss": "Avg. Loss %",
            "win_pct": "Win %",
            "loss_pct": "Loss %",
            "wins": "Wins",
            "losses": "Losses",
            "trades": "# Trades",
            "lg_gain": "LG Gain",
            "lg_loss": "LG Loss",
            "avg_days_gain": "Avg. Days Gain",
            "avg_days_loss": "Avg. Days Loss",
        }
        for c in avg_cols:
            anchor = tk.W if c == "year" else tk.E
            self.analysis2_avg_tree.heading(c, text=avg_headings[c])
            self.analysis2_avg_tree.column(c, width=95 if c not in {"year", "avg_days_gain", "avg_days_loss"} else 100, anchor=anchor, stretch=True)
        self.analysis2_avg_tree.grid(row=1, column=0, sticky="ew")

        self.analysis2_detail_frame = ttk.LabelFrame(parent_frame, text="Monthly Stats")
        self.analysis2_detail_frame.grid(row=3, column=0, sticky="nsew", padx=5, pady=5)
        self.analysis2_detail_frame.columnconfigure(0, weight=1)
        self.analysis2_detail_frame.rowconfigure(0, weight=1)
        detail_cols = ("month", "avg_gain", "avg_loss", "win_pct", "loss_pct", "wins", "losses", "trades", "lg_gain", "lg_loss", "avg_days_gain", "avg_days_loss")
        self.analysis2_detail_tree = ttk.Treeview(self.analysis2_detail_frame, columns=detail_cols, show="headings", height=10, selectmode="extended")
        detail_headings = avg_headings.copy()
        detail_headings["month"] = "Month"
        for c in detail_cols:
            anchor = tk.W if c == "month" else tk.E
            self.analysis2_detail_tree.heading(c, text=detail_headings[c])
            self.analysis2_detail_tree.column(c, width=95 if c != "month" else 90, anchor=anchor, stretch=True)
        d_vsb = ttk.Scrollbar(self.analysis2_detail_frame, orient="vertical", command=self.analysis2_detail_tree.yview)
        self.analysis2_detail_tree.configure(yscrollcommand=d_vsb.set)
        self.analysis2_detail_tree.grid(row=0, column=0, sticky="nsew")
        d_vsb.grid(row=0, column=1, sticky="ns")

        filter_btn = ttk.Button(self.analysis2_detail_frame, text="Filter Journal to Selected Months", command=self._analysis2_filter_journal_to_selected_months)
        filter_btn.grid(row=1, column=0, sticky="e", padx=2, pady=4)
        clear_filter_btn = ttk.Button(self.analysis2_detail_frame, text="Clear Monthly Filter", command=self._analysis2_clear_month_filter)
        clear_filter_btn.grid(row=1, column=1, sticky="e", padx=2, pady=4)

        self.update_analysis_two_view()

    def _analysis2_filtered_trades(self) -> List[Tuple[int, TradeEntry]]:
        account_filter = self.account_var.get()
        closed_only = self.closed_only_var.get()
        open_only = getattr(self, "open_only_var", tk.BooleanVar(value=False)).get()
        entry_strategy_filter = self.entry_strategy_filter_var.get()
        exit_strategy_filter = self.exit_strategy_filter_var.get()
        exit_start_date = getattr(self, "exit_start_date", None)
        exit_end_date = getattr(self, "exit_end_date", None)
        symbol_filter_tokens = self._parsed_symbol_filter()
        top_set = getattr(self, "top_filter_set", None)

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

        filtered: List[Tuple[int, TradeEntry]] = []
        for idx, trade in enumerate(self.model.trades):
            if top_set is not None and idx not in top_set:
                continue
            if not trade.is_closed:
                continue
            if not matches_strategy_filters(trade):
                continue
            if symbol_filter_tokens and trade.symbol.upper() not in symbol_filter_tokens:
                continue
            if account_filter and account_filter != "all" and trade.account_number != account_filter:
                continue
            if self.start_date and trade.entry_date.date() < self.start_date:
                continue
            if self.end_date and trade.entry_date.date() > self.end_date:
                continue
            if exit_start_date or exit_end_date:
                if not trade.exit_date:
                    continue
                exit_date = trade.exit_date.date()
                if exit_start_date and exit_date < exit_start_date:
                    continue
                if exit_end_date and exit_date > exit_end_date:
                    continue
            if closed_only and not trade.exit_date:
                continue
            filtered.append((idx, trade))
        return filtered

    def _analysis2_get_starting_balance(self, year: int) -> Optional[float]:
        account = self.account_var.get()
        if account == "all":
            total = 0.0
            found = False
            for acct, year_map in self.analysis2_starting_balances.items():
                if year in year_map:
                    total += float(year_map[year])
                    found = True
            return total if found else None
        return self.analysis2_starting_balances.get(account, {}).get(year)

    @staticmethod
    def _analysis2_stats_for_trades(trades: List[TradeEntry]) -> dict:
        wins = [t for t in trades if (t.pnl or 0.0) > 1e-8]
        losses = [t for t in trades if (t.pnl or 0.0) < -1e-8]
        num_wins = len(wins)
        num_losses = len(losses)
        num_trades = len(trades)
        denom = (num_wins + num_losses)
        win_pct = (num_wins / denom * 100.0) if denom else 0.0
        loss_pct = (num_losses / denom * 100.0) if denom else 0.0
        win_pcts = [t.pnl_pct for t in wins if t.pnl_pct is not None]
        loss_pcts = [abs(t.pnl_pct) for t in losses if t.pnl_pct is not None]
        avg_gain = sum(win_pcts) / len(win_pcts) if win_pcts else 0.0
        avg_loss = sum(loss_pcts) / len(loss_pcts) if loss_pcts else 0.0
        lg_gain = max(win_pcts) if win_pcts else 0.0
        lg_loss = max(loss_pcts) if loss_pcts else 0.0
        win_days = [t.hold_period or 0 for t in wins]
        loss_days = [t.hold_period or 0 for t in losses]
        avg_days_gain = sum(win_days) / len(win_days) if win_days else 0.0
        avg_days_loss = sum(loss_days) / len(loss_days) if loss_days else 0.0
        return {
            "avg_gain": avg_gain,
            "avg_loss": avg_loss,
            "win_pct": win_pct,
            "loss_pct": loss_pct,
            "wins": num_wins,
            "losses": num_losses,
            "trades": num_trades,
            "lg_gain": lg_gain,
            "lg_loss": lg_loss,
            "avg_days_gain": avg_days_gain,
            "avg_days_loss": avg_days_loss,
        }

    def _toggle_analysis2_avg(self) -> None:
        if getattr(self, "analysis2_avg_visible", True):
            self.analysis2_avg_tree.grid_remove()
            self.analysis2_avg_toggle_btn.config(text="+")
            self.analysis2_avg_visible = False
        else:
            self.analysis2_avg_tree.grid()
            self.analysis2_avg_toggle_btn.config(text="−")
            self.analysis2_avg_visible = True

    def _toggle_analysis2_monthly(self) -> None:
        if getattr(self, "analysis2_monthly_visible", True):
            self.analysis2_monthly_tree.grid_remove()
            try:
                self.analysis2_monthly_frame.grid_slaves(row=1, column=1)[0].grid_remove()
            except Exception:
                pass
            self.analysis2_monthly_toggle_btn.config(text="+")
            self.analysis2_monthly_visible = False
        else:
            self.analysis2_monthly_tree.grid()
            try:
                self.analysis2_monthly_frame.grid_slaves(row=1, column=1)[0].grid()
            except Exception:
                pass
            self.analysis2_monthly_toggle_btn.config(text="−")
            self.analysis2_monthly_visible = True

    def _analysis2_set_balance_controls_state(self) -> None:
        account = self.account_var.get()
        state = "normal" if account != "all" else "disabled"
        self.analysis2_start_balance_entry.configure(state=state)
        self.analysis2_save_balance_btn.configure(state=state)
        self.analysis2_clear_balance_btn.configure(state=state)
        if account == "all":
            self.analysis2_start_balance_var.set("")

    def _analysis2_sync_start_balance_entry(self, year: int) -> None:
        bal = self._analysis2_get_starting_balance(year)
        if bal is None:
            self.analysis2_start_balance_var.set("")
        else:
            self.analysis2_start_balance_var.set(f"{bal:.2f}")

    def _analysis2_save_starting_balance(self) -> None:
        account = self.account_var.get()
        if account == "all":
            messagebox.showinfo("Select Account", "Select a specific account to save a starting balance.")
            return
        year_str = (self.analysis2_year_var.get() or "").strip()
        if not year_str:
            messagebox.showinfo("Select Year", "Select a year to save a starting balance.")
            return
        try:
            year = int(year_str)
        except ValueError:
            messagebox.showwarning("Invalid Year", "Year must be a number.")
            return
        raw = (self.analysis2_start_balance_var.get() or "").replace(",", "").strip()
        if not raw:
            messagebox.showinfo("Enter Balance", "Enter a starting balance before saving.")
            return
        try:
            balance = float(raw)
        except ValueError:
            messagebox.showwarning("Invalid Balance", "Starting balance must be a number.")
            return
        if balance <= 0:
            messagebox.showwarning("Invalid Balance", "Starting balance must be greater than 0.")
            return
        self.analysis2_starting_balances.setdefault(account, {})[year] = balance
        self.model.save_state(self.persist_path, filter_state=self._current_filter_state())
        self.update_analysis_two_view()

    def _analysis2_clear_starting_balance(self) -> None:
        account = self.account_var.get()
        year_str = (self.analysis2_year_var.get() or "").strip()
        if not year_str:
            return
        try:
            year = int(year_str)
        except ValueError:
            return
        if account in self.analysis2_starting_balances and year in self.analysis2_starting_balances[account]:
            del self.analysis2_starting_balances[account][year]
            if not self.analysis2_starting_balances[account]:
                del self.analysis2_starting_balances[account]
            self.model.save_state(self.persist_path, filter_state=self._current_filter_state())
        self.analysis2_start_balance_var.set("")
        self.update_analysis_two_view()

    def update_analysis_two_view(self) -> None:
        if not hasattr(self, "analysis2_monthly_tree"):
            return
        self._analysis2_set_balance_controls_state()

        if hasattr(self, "analysis2_account_combo"):
            values = list(self.account_dropdown["values"]) if hasattr(self, "account_dropdown") else []
            if not values:
                values = ["all"] + sorted({tx.account_number for tx in self.model.transactions})
            self.analysis2_account_combo["values"] = values
            if self.account_var.get() not in values:
                self.account_var.set("all")

        trade_items = self._analysis2_filtered_trades()
        trades_only = [t for _, t in trade_items]
        years = sorted({t.exit_date.year for t in trades_only if t.exit_date})
        year_values = [str(y) for y in years]
        self.analysis2_year_combo["values"] = year_values

        if not years:
            self.analysis2_year_var.set("")
            self.analysis2_start_balance_var.set("")
            for tree in (self.analysis2_monthly_tree, self.analysis2_avg_tree, self.analysis2_detail_tree):
                tree.delete(*tree.get_children())
            self.analysis2_monthly_frame.configure(text="Monthly Returns")
            return

        if self.analysis2_year_var.get() not in year_values:
            self.analysis2_year_var.set(str(years[-1]))
        year = int(self.analysis2_year_var.get())
        self.analysis2_monthly_frame.configure(text=f"Monthly Returns ({year})")
        self._analysis2_sync_start_balance_entry(year)

        year_trades = [t for t in trades_only if t.exit_date and t.exit_date.year == year]
        month_map: Dict[int, List[TradeEntry]] = {m: [] for m in range(1, 13)}
        self.analysis2_month_trade_indices: Dict[int, List[int]] = {m: [] for m in range(1, 13)}
        for idx, t in trade_items:
            if t.exit_date and t.exit_date.year == year:
                month_map[t.exit_date.month].append(t)
                self.analysis2_month_trade_indices[t.exit_date.month].append(idx)

        starting_balance = self._analysis2_get_starting_balance(year)
        cumulative_pnl = 0.0

        self.analysis2_monthly_tree.delete(*self.analysis2_monthly_tree.get_children())
        for m in range(1, 13):
            month_trades = month_map[m]
            month_pnl = sum(t.pnl or 0.0 for t in month_trades)
            cumulative_pnl += month_pnl

            month_label = dt.date(year, m, 1).strftime("%b")
            if starting_balance and starting_balance > 0:
                equity_start = starting_balance + (cumulative_pnl - month_pnl)
                month_return = (month_pnl / equity_start) if equity_start != 0 else 0.0
                cumulative_return = cumulative_pnl / starting_balance
                if cumulative_return <= -1:
                    prorated_return = -1.0
                else:
                    prorated_return = (1 + cumulative_return) ** (12 / m) - 1
                month_return_str = f"{month_return * 100:.2f}%"
                cum_return_str = f"{cumulative_return * 100:.2f}%"
                pro_return_str = f"{prorated_return * 100:.2f}%"
            else:
                month_return_str = "-"
                cum_return_str = "-"
                pro_return_str = "-"

            self.analysis2_monthly_tree.insert("", "end", values=(
                month_label,
                month_return_str,
                cum_return_str,
                pro_return_str,
            ))

        self.analysis2_avg_tree.delete(*self.analysis2_avg_tree.get_children())
        avg_stats = self._analysis2_stats_for_trades(year_trades)
        self.analysis2_avg_tree.insert("", "end", values=(
            year,
            f"{avg_stats['avg_gain']:.2f}%",
            f"{avg_stats['avg_loss']:.2f}%",
            f"{avg_stats['win_pct']:.1f}%",
            f"{avg_stats['loss_pct']:.1f}%",
            avg_stats["wins"],
            avg_stats["losses"],
            avg_stats["trades"],
            f"{avg_stats['lg_gain']:.2f}%",
            f"{avg_stats['lg_loss']:.2f}%",
            f"{avg_stats['avg_days_gain']:.1f}",
            f"{avg_stats['avg_days_loss']:.1f}",
        ))

        self.analysis2_detail_tree.delete(*self.analysis2_detail_tree.get_children())
        for m in range(1, 13):
            month_trades = month_map[m]
            stats = self._analysis2_stats_for_trades(month_trades)
            month_label = dt.date(year, m, 1).strftime("%b-%y")
            self.analysis2_detail_tree.insert("", "end", iid=f"{year}-{m:02d}", values=(
                month_label,
                f"{stats['avg_gain']:.2f}%",
                f"{stats['avg_loss']:.2f}%",
                f"{stats['win_pct']:.1f}%",
                f"{stats['loss_pct']:.1f}%",
                stats["wins"],
                stats["losses"],
                stats["trades"],
                f"{stats['lg_gain']:.2f}%",
                f"{stats['lg_loss']:.2f}%",
                f"{stats['avg_days_gain']:.1f}",
                f"{stats['avg_days_loss']:.1f}",
            ))

    def _analysis2_filter_journal_to_selected_months(self) -> None:
        if not hasattr(self, "analysis2_detail_tree"):
            return
        selected = self.analysis2_detail_tree.selection()
        if not selected:
            return
        month_keys: Set[Tuple[int, int]] = set()
        for item_id in selected:
            try:
                year_str, month_str = item_id.split("-")
                month_keys.add((int(year_str), int(month_str)))
            except Exception:
                continue
        if not month_keys:
            return
        indices: Set[int] = set()
        for idx, trade in self._analysis2_filtered_trades():
            if not trade.exit_date:
                continue
            key = (trade.exit_date.year, trade.exit_date.month)
            if key in month_keys:
                indices.add(idx)
        if not indices:
            return
        self.top_filter_set = indices
        self.populate_table()
        self.update_summary_and_chart()
        try:
            self.notebook.select(self.journal_tab)
        except Exception:
            pass

    def _analysis2_clear_month_filter(self) -> None:
        if hasattr(self, 'top_filter_set'):
            self.top_filter_set = None
        self.populate_table()
        self.update_summary_and_chart()
        try:
            self.notebook.select(self.journal_tab)
        except Exception:
            pass

    def _create_analysis_tree(self, parent: ttk.Frame, title: str) -> Tuple[ttk.LabelFrame, ttk.Treeview]:
        frame = ttk.LabelFrame(parent, text=title)
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(0, weight=1)
        cols = ("rank", "name", "trades", "total_pnl", "avg_pnl", "win_rate", "profit_factor", "expectancy", "avg_hold")
        headings = {
            "rank": "#",
            "name": "Strategy",
            "trades": "Trades",
            "total_pnl": "Total PnL",
            "avg_pnl": "Avg PnL",
            "win_rate": "Win %",
            "profit_factor": "PF",
            "expectancy": "Expectancy",
            "avg_hold": "Avg Hold (d)",
        }
        tree = ttk.Treeview(frame, columns=cols, show="headings", height=8, selectmode="browse")
        for c in cols:
            anchor = tk.E if c != "name" else tk.W
            tree.heading(c, text=headings[c])
            tree.column(c, width=90 if c != "name" else 180, anchor=anchor, stretch=True)
        vsb = ttk.Scrollbar(frame, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=vsb.set)
        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        tree.bind("<<TreeviewSelect>>", self._on_analysis_select)
        tree.bind("<Double-1>", self._on_analysis_open_dialog)
        return frame, tree

    def _toggle_analysis_combo(self) -> None:
        if getattr(self, "analysis_combo_visible", True):
            self.analysis_combo_frame.grid_remove()
            self.analysis_combo_toggle_btn.config(text="+")
            self.analysis_combo_visible = False
        else:
            self.analysis_combo_frame.grid()
            self.analysis_combo_toggle_btn.config(text="−")
            self.analysis_combo_visible = True

    def _on_tab_changed(self, event: tk.Event) -> None:
        try:
            tab_text = self.notebook.tab(self.notebook.select(), "text")
            if tab_text == "Analysis" and hasattr(self, "analysis_account_combo"):
                self.refresh_analysis_accounts()
                self.refresh_analysis_view()
            if tab_text == "Analysis 2":
                self.update_analysis_two_view()
        except Exception:
            pass

    def refresh_analysis_accounts(self) -> None:
        acct_numbers = sorted({tx.account_number for tx in self.model.transactions})
        values = ["all"] + acct_numbers
        self.analysis_account_combo["values"] = values
        if self.analysis_account_var.get() not in values:
            self.analysis_account_var.set("all")

    def _parse_strategy_tags(self, raw: str) -> List[str]:
        tokens = [re.sub(r"\s+", " ", t.strip()) for t in (raw or "").split(",")]
        return [t for t in tokens if t]

    def _analysis_date_bounds(self) -> Tuple[Optional[dt.date], Optional[dt.date]]:
        start = self.analysis_start_date_var.get().strip()
        end = self.analysis_end_date_var.get().strip()
        start_date = None
        end_date = None
        if start:
            try:
                start_date = self._parse_date_input(start)
            except Exception:
                messagebox.showwarning("Invalid Date", "Start date is not recognized.")
                return None, None
        if end:
            try:
                end_date = self._parse_date_input(end)
            except Exception:
                messagebox.showwarning("Invalid Date", "End date is not recognized.")
                return None, None
        if start_date and end_date and start_date > end_date:
            messagebox.showwarning("Invalid Range", "Start date cannot be after end date for Analysis.")
            return None, None
        return start_date, end_date

    def _filtered_trades_for_analysis(self) -> List[Tuple[int, TradeEntry, str, str]]:
        start_date, end_date = self._analysis_date_bounds()
        account_filter = self.analysis_account_var.get()
        closed_only = self.analysis_closed_only_var.get()
        date_mode_exit = self.analysis_date_mode_var.get() == "Exit Date"
        include_unspec = self.analysis_include_unspecified_var.get()
        trades: List[Tuple[int, TradeEntry, str, str]] = []
        for idx, trade in enumerate(self.model.trades):
            if closed_only and not trade.exit_date:
                continue
            if account_filter != "all" and trade.account_number != account_filter:
                continue
            if date_mode_exit:
                if not trade.exit_date:
                    continue
                d = trade.exit_date.date()
            else:
                d = trade.entry_date.date()
            if start_date and d < start_date:
                continue
            if end_date and d > end_date:
                continue
            key = self.model.compute_key(trade)
            entry_raw = self.model.entry_strategies.get(key, trade.entry_strategy or "")
            exit_raw = self.model.exit_strategies.get(key, trade.exit_strategy or "")
            entry_tags = self._parse_strategy_tags(entry_raw)
            exit_tags = self._parse_strategy_tags(exit_raw)
            if include_unspec:
                if not entry_tags:
                    entry_tags = ["Unspecified"]
                if not exit_tags:
                    exit_tags = ["Unspecified"]
            trades.append((idx, trade, ", ".join(entry_tags), ", ".join(exit_tags)))
        return trades

    def refresh_analysis_view(self) -> None:
        try:
            top_n = int(self.analysis_top_n_var.get() or 0)
        except ValueError:
            top_n = 10
            self.analysis_top_n_var.set("10")
        try:
            min_trades = int(self.analysis_min_trades_var.get() or 0)
        except ValueError:
            min_trades = 20
            self.analysis_min_trades_var.set("20")
        attrib = self.analysis_attribution_var.get()
        include_unspec = self.analysis_include_unspecified_var.get()
        trades = self._filtered_trades_for_analysis()
        entry_data = self._compute_strategy_leaderboard(trades, group_type="entry", top_n=top_n, min_trades=min_trades, attribution=attrib, include_unspecified=include_unspec, sort_by=self.analysis_sort_entry_var.get())
        exit_data = self._compute_strategy_leaderboard(trades, group_type="exit", top_n=top_n, min_trades=min_trades, attribution=attrib, include_unspecified=include_unspec, sort_by=self.analysis_sort_exit_var.get())
        combo_data = self._compute_strategy_leaderboard(trades, group_type="combo", top_n=top_n, min_trades=min_trades, attribution=attrib, include_unspecified=include_unspec, sort_by=self.analysis_sort_combo_var.get())
        self._populate_analysis_tree(self.analysis_entry_tree, entry_data)
        self._populate_analysis_tree(self.analysis_exit_tree, exit_data)
        self._populate_analysis_tree(self.analysis_combo_tree, combo_data)
        self._analysis_selected = None
        self._set_analysis_detail(None)

    def _compute_strategy_leaderboard(self, trades: List[Tuple[int, TradeEntry, str, str]], *, group_type: str, top_n: int, min_trades: int, attribution: str, include_unspecified: bool, sort_by: str) -> List[dict]:
        groups: Dict[str, dict] = {}

        def add_contribution(key: str, display: str, contrib: float, trade_id: int, hold: Optional[int]):
            g = groups.setdefault(key, {
                "name": display,
                "pnl": 0.0,
                "wins": 0,
                "losses": 0,
                "breakeven": 0,
                "trade_ids": set(),
                "pos_sum": 0.0,
                "neg_sum": 0.0,
                "hold_sum": 0.0,
                "hold_n": 0,
            })
            g["pnl"] += contrib
            if contrib > 1e-8:
                g["wins"] += 1
                g["pos_sum"] += contrib
            elif contrib < -1e-8:
                g["losses"] += 1
                g["neg_sum"] += contrib
            else:
                g["breakeven"] += 1
            g["trade_ids"].add(trade_id)
            if hold is not None:
                g["hold_sum"] += hold
                g["hold_n"] += 1

        for idx, trade, entry_str, exit_str in trades:
            if trade.pnl is None:
                continue
            entry_tags_raw = self._parse_strategy_tags(entry_str)
            exit_tags_raw = self._parse_strategy_tags(exit_str)
            entry_tags = [t.lower() for t in entry_tags_raw] or (["unspecified"] if include_unspecified else [])
            exit_tags = [t.lower() for t in exit_tags_raw] or (["unspecified"] if include_unspecified else [])
            entry_disp_map = {t.lower(): t for t in entry_tags_raw}
            exit_disp_map = {t.lower(): t for t in exit_tags_raw}
            pnl = trade.pnl
            hold = trade.hold_period

            if group_type == "entry":
                if not entry_tags:
                    continue
                tags = entry_tags
                if attribution == "Primary":
                    tags = tags[:1]
                denom = len(tags) if attribution == "Split" and len(tags) > 0 else 1
                contrib_base = pnl / denom if denom else pnl
                for t in tags:
                    add_contribution(t, entry_disp_map.get(t, t.title()), contrib_base if attribution != "Full" else pnl, idx, hold)
            elif group_type == "exit":
                if not exit_tags:
                    continue
                tags = exit_tags
                if attribution == "Primary":
                    tags = tags[:1]
                denom = len(tags) if attribution == "Split" and len(tags) > 0 else 1
                contrib_base = pnl / denom if denom else pnl
                for t in tags:
                    add_contribution(t, exit_disp_map.get(t, t.title()), contrib_base if attribution != "Full" else pnl, idx, hold)
            else:
                if not entry_tags or not exit_tags:
                    continue
                e_tags = entry_tags[:1] if attribution == "Primary" else entry_tags
                x_tags = exit_tags[:1] if attribution == "Primary" else exit_tags
                denom = (len(e_tags) * len(x_tags)) if attribution == "Split" and e_tags and x_tags else 1
                contrib_base = pnl / denom if denom else pnl
                for e_tag in e_tags:
                    for x_tag in x_tags:
                        combo_key = f"{e_tag} -> {x_tag}"
                        display = f"{entry_disp_map.get(e_tag, e_tag.title())} -> {exit_disp_map.get(x_tag, x_tag.title())}"
                        add_contribution(combo_key, display, contrib_base if attribution != "Full" else pnl, idx, hold)

        rows: List[dict] = []
        for key, g in groups.items():
            trade_count = len(g["trade_ids"])
            if trade_count < min_trades:
                continue
            pos_sum = g["pos_sum"]
            neg_sum = g["neg_sum"]
            wins = g["wins"]
            losses = g["losses"]
            total = g["pnl"]
            avg_pnl = total / trade_count if trade_count else 0.0
            win_rate = wins / (wins + losses) if (wins + losses) > 0 else 0.0
            pf = float("inf") if neg_sum == 0 and pos_sum > 0 else (pos_sum / abs(neg_sum) if neg_sum < 0 else 0.0)
            avg_hold = (g["hold_sum"] / g["hold_n"]) if g["hold_n"] > 0 else None
            avg_win = pos_sum / wins if wins > 0 else 0.0
            avg_loss_abs = abs(neg_sum) / losses if losses > 0 else 0.0
            expectancy = win_rate * avg_win - (1 - win_rate) * avg_loss_abs
            rows.append({
                "key": key,
                "name": g["name"],
                "trades": trade_count,
                "total_pnl": total,
                "avg_pnl": avg_pnl,
                "win_rate": win_rate,
                "profit_factor": pf,
                "expectancy": expectancy,
                "avg_hold": avg_hold,
                "wins": wins,
                "losses": losses,
                "breakeven": g["breakeven"],
                "trade_ids": list(g["trade_ids"]),
            })

        def sort_key(row: dict):
            metric = sort_by
            if metric == "Avg PnL":
                return row["avg_pnl"]
            if metric == "Win Rate":
                return row["win_rate"]
            if metric == "Profit Factor":
                return row["profit_factor"]
            if metric == "Expectancy":
                return row["expectancy"]
            if metric == "Trades":
                return row["trades"]
            if metric == "Avg Hold":
                return row["avg_hold"] if row["avg_hold"] is not None else float('-inf')
            return row["total_pnl"]

        rows.sort(key=sort_key, reverse=True)
        return rows[:top_n] if top_n > 0 else rows

    def _format_analysis_row(self, row: dict, rank: int) -> Tuple:
        pf = row["profit_factor"]
        pf_str = "∞" if pf == float('inf') else f"{pf:.2f}"
        avg_hold = "" if row["avg_hold"] is None else f"{row['avg_hold']:.1f}"
        return (
            rank,
            row["name"],
            row["trades"],
            f"{row['total_pnl']:.2f}",
            f"{row['avg_pnl']:.2f}",
            f"{row['win_rate']*100:.2f}%",
            pf_str,
            f"{row['expectancy']:.2f}",
            avg_hold,
        )

    def _populate_analysis_tree(self, tree: ttk.Treeview, rows: List[dict]) -> None:
        for item in tree.get_children():
            tree.delete(item)
        if not rows:
            tree.insert("", "end", values=("-", "No strategies meet filters", "", "", "", "", "", "", ""))
            tree._analysis_rows = {}  # type: ignore
            return
        for i, row in enumerate(rows, start=1):
            tree.insert("", "end", iid=row["key"], values=self._format_analysis_row(row, i))
        tree._analysis_rows = {r["key"]: r for r in rows}  # type: ignore

    def _on_analysis_select(self, event: tk.Event) -> None:
        widget: ttk.Treeview = event.widget  # type: ignore
        rows = getattr(widget, "_analysis_rows", {})
        sel = widget.selection()
        if not sel:
            self._set_analysis_detail(None)
            self._analysis_selected = None
            return
        key = sel[0]
        row = rows.get(key)
        label = widget.master.cget("text")
        self._analysis_selected = (label, row) if row else None
        self._set_analysis_detail(row, label=label)

    def _on_analysis_open_dialog(self, event: tk.Event) -> None:
        widget: ttk.Treeview = event.widget  # type: ignore
        row_id = widget.identify_row(event.y)
        if not row_id:
            return
        rows = getattr(widget, "_analysis_rows", {})
        row = rows.get(row_id)
        if not row:
            return
        widget.selection_set(row_id)
        label = widget.master.cget("text")
        self._open_analysis_trades_dialog(label, row)

    def _open_analysis_trades_dialog(self, label: str, row: dict) -> None:
        trades = self._analysis_trades_from_row(row, label=label)
        dialog = tk.Toplevel(self.root)
        dialog.title(f"{label}: {row.get('name', '')}")
        dialog.geometry("1100x520")
        dialog.bind("<Escape>", lambda e: dialog.destroy())

        tree = ttk.Treeview(dialog, columns=self.analysis_trade_columns, show="headings", selectmode="browse")
        for c in self.analysis_trade_columns:
            anchor = tk.CENTER if c in {"qty", "pnl_pct"} else tk.E
            if c in {"entry", "exit", "entry_strat", "exit_strat"}:
                anchor = tk.W
            tree.heading(c, text=self.analysis_trade_headings[c])
            tree.column(c, width=90 if c not in {"entry_strat", "exit_strat"} else 180, anchor=anchor, stretch=True)
        vsb = ttk.Scrollbar(dialog, orient="vertical", command=tree.yview)
        hsb = ttk.Scrollbar(dialog, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        dialog.grid_columnconfigure(0, weight=1)
        dialog.grid_rowconfigure(0, weight=1)

        self._fill_analysis_trade_tree(tree, trades)

    def _set_analysis_detail(self, row: Optional[dict], *, label: Optional[str] = None) -> None:
        self.analysis_detail_text.configure(state="normal")
        self.analysis_detail_text.delete("1.0", tk.END)
        if not row:
            self.analysis_detail_text.insert(tk.END, "Select a strategy to see details.")
        else:
            avg_hold_str = "n/a" if row.get("avg_hold") is None else f"{row['avg_hold']:.1f}"
            self.analysis_detail_text.insert(tk.END, (
                f"Strategy: {row['name']}\n"
                f"Trades: {row['trades']} (W {row['wins']} / L {row['losses']} / B {row['breakeven']})\n"
                f"Total PnL: {row['total_pnl']:.2f}\n"
                f"Avg PnL: {row['avg_pnl']:.2f}\n"
                f"Win Rate: {row['win_rate']*100:.2f}%\n"
                f"Profit Factor: {'∞' if row['profit_factor']==float('inf') else f'{row['profit_factor']:.2f}'}\n"
                f"Expectancy: {row['expectancy']:.2f}\n"
                f"Avg Hold: {avg_hold_str}\n"
            ))
        self.analysis_detail_text.configure(state="disabled")
        self._populate_analysis_detail_trades(row, label=label)

    def _analysis_trades_from_row(self, row: Optional[dict], *, label: Optional[str] = None) -> List[dict]:
        if not row:
            return []
        trade_ids = row.get("trade_ids")
        collected: List[Tuple[int, TradeEntry, str, str]] = []
        if trade_ids:
            for tidx in trade_ids:
                if tidx is None or not isinstance(tidx, int):
                    continue
                if tidx < 0 or tidx >= len(self.model.trades):
                    continue
                trade = self.model.trades[tidx]
                collected.append((tidx, trade, "", ""))
        if not collected:
            # Fallback: rebuild from filtered trades that match the selected strategy label
            label_norm = (label or "").lower()
            name_norm = (row.get("name", "") or "").lower()
            for idx, trade, entry_tags, exit_tags in self._filtered_trades_for_analysis():
                entry_list = [t.lower() for t in self._parse_strategy_tags(entry_tags)]
                exit_list = [t.lower() for t in self._parse_strategy_tags(exit_tags)]
                if label_norm.startswith("entry"):
                    if name_norm in entry_list:
                        collected.append((idx, trade, entry_tags, exit_tags))
                elif label_norm.startswith("exit"):
                    if name_norm in exit_list:
                        collected.append((idx, trade, entry_tags, exit_tags))
                else:
                    if "->" in name_norm:
                        e_name, x_name = [p.strip() for p in name_norm.split("->", 1)]
                        if e_name in entry_list and x_name in exit_list:
                            collected.append((idx, trade, entry_tags, exit_tags))
        results: List[dict] = []
        for _, trade, entry_tags, exit_tags in collected:
            key = self.model.compute_key(trade)
            entry_strat = self.model.entry_strategies.get(key, trade.entry_strategy or entry_tags or "")
            exit_strat = self.model.exit_strategies.get(key, trade.exit_strategy or exit_tags or "")
            results.append({
                "account": trade.account_number,
                "symbol": trade.symbol,
                "entry": trade.entry_date.strftime("%Y-%m-%d"),
                "entry_price": trade.entry_price,
                "exit": trade.exit_date.strftime("%Y-%m-%d") if trade.exit_date else "",
                "exit_price": trade.exit_price,
                "qty": trade.quantity,
                "pnl": trade.pnl,
                "pnl_pct": trade.pnl_pct,
                "entry_strat": entry_strat,
                "exit_strat": exit_strat,
            })
        return results

    def _populate_analysis_detail_trades(self, row: Optional[dict], *, label: Optional[str] = None) -> None:
        tree = getattr(self, "analysis_detail_tree", None)
        if tree is None:
            return
        self._fill_analysis_trade_tree(tree, self._analysis_trades_from_row(row, label=label))

    def _fill_analysis_trade_tree(self, tree: ttk.Treeview, trades: List[dict]) -> None:
        for item in tree.get_children():
            tree.delete(item)
        if not trades:
            return
        for i, t in enumerate(trades, start=1):
            tree.insert(
                "",
                "end",
                values=(
                    i,
                    t.get("account", ""),
                    t.get("symbol", ""),
                    t.get("entry", ""),
                    f"{t['entry_price']:.2f}" if t.get("entry_price") is not None else "",
                    t.get("exit", ""),
                    f"{t['exit_price']:.2f}" if t.get("exit_price") is not None else "",
                    f"{t['qty']:.2f}" if t.get("qty") is not None else "",
                    f"{t['pnl']:.2f}" if t.get("pnl") is not None else "",
                    f"{t['pnl_pct']:.2f}%" if t.get("pnl_pct") is not None else "",
                    t.get("entry_strat", ""),
                    t.get("exit_strat", ""),
                ),
            )

    def _filter_journal_from_analysis(self) -> None:
        sel = self._analysis_selected
        if not sel or not sel[1]:
            return
        label, row = sel
        name = row.get("name", "")
        if label == "Entry Strategies":
            self.entry_strategy_filter_var.set(name)
            self.exit_strategy_filter_var.set("all")
        elif label == "Exit Strategies":
            self.exit_strategy_filter_var.set(name)
            self.entry_strategy_filter_var.set("all")
        else:
            parts = name.split("->")
            if len(parts) == 2:
                self.entry_strategy_filter_var.set(parts[0].strip())
                self.exit_strategy_filter_var.set(parts[1].strip())
        acct = self.analysis_account_var.get()
        self.account_var.set(acct if acct in self.account_dropdown["values"] else "all")
        start_date, end_date = self._analysis_date_bounds()
        if start_date:
            self.start_date_var.set(self._format_date_preferred(start_date))
        if end_date:
            self.end_date_var.set(self._format_date_preferred(end_date))
        self.closed_only_var.set(self.analysis_closed_only_var.get())
        self.apply_date_filter()
        self.populate_table()
        self.update_summary_and_chart()
        try:
            self.notebook.select(self.journal_tab)
        except Exception:
            pass

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

    def _on_strategy_combo_keyrelease(self, event: tk.Event, *, is_entry: bool) -> None:
        """Filter strategy dropdown values as user types (substring match, case-insensitive)."""
        combo = self.entry_strategy_filter_combo if is_entry else self.exit_strategy_filter_combo
        all_values = getattr(self, "entry_strategy_all_values" if is_entry else "exit_strategy_all_values", ["all"])
        typed = combo.get().strip()
        if not typed:
            combo['values'] = all_values
        else:
            low = typed.lower()
            filtered = [v for v in all_values if low in v.lower() or v == "all"]
            # Deduplicate while preserving order
            seen = set()
            ordered = []
            for v in filtered:
                if v not in seen:
                    seen.add(v)
                    ordered.append(v)
            combo['values'] = ordered if ordered else all_values
        # Apply filter to table on each key stroke
        self.on_strategy_filter_change(None)

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
            # Get trades for this symbol (case-insensitive)
            trades_for_symbol = [t for t in self.model.trades if t.symbol.upper() == symbol.upper()]

            # Determine required price range based on trades (pads 90 days before first entry and up to today/last exit)
            if trades_for_symbol:
                first_entry = min(t.entry_date for t in trades_for_symbol).date()
                last_exit = max((t.exit_date for t in trades_for_symbol if t.exit_date), default=None)
                latest_trade_date = max((t.exit_date or t.entry_date for t in trades_for_symbol)).date()
                has_open_trade = any(t.exit_date is None for t in trades_for_symbol)
                required_start = first_entry - dt.timedelta(days=90)
                # Pad 7 days past the latest trade to ensure yfinance's exclusive end range includes it
                padded_latest = latest_trade_date + dt.timedelta(days=7)
                required_end = min((last_exit + dt.timedelta(days=90)).date(), dt.date.today()) if last_exit else dt.date.today()
                required_end = max(required_end, padded_latest)
                # If any position is still open, always fetch through (today + 1) to cover the current candle
                if has_open_trade:
                    required_end = max(required_end, dt.date.today() + dt.timedelta(days=1))
            else:
                required_start = dt.date.today() - dt.timedelta(days=180)
                required_end = dt.date.today()

            # Check cached metadata to see if we need to refresh to include new (open) trades
            metadata = self.price_manager.get_metadata(symbol)
            needs_refresh = False
            meta_start = None
            meta_end = None
            if metadata:
                meta_start = dt.datetime.fromisoformat(metadata['start_date']).date()
                meta_end = dt.datetime.fromisoformat(metadata['end_date']).date()
                if meta_start > required_start or meta_end < required_end:
                    needs_refresh = True
            else:
                needs_refresh = True

            df: Optional[pd.DataFrame] = None
            if needs_refresh:
                if not HAS_YFINANCE:
                    messagebox.showerror("Missing Dependency",
                                       "yfinance is required to refresh price data. Install with: pip install yfinance")
                    return
                self.chart_status_var.set(f"Updating price data for {symbol}...")
                self.root.update()
                fetch_result = self.price_manager.fetch_and_store(symbol, required_start, required_end)
                if fetch_result is None or fetch_result.empty:
                    self.chart_status_var.set(f"No price data available for {symbol}")
                    return
                df = self.price_manager.get_price_data(symbol, required_start, required_end)
                meta_start, meta_end = required_start, required_end
            else:
                meta_start = meta_start or required_start
                meta_end = meta_end or required_end
                df = self.price_manager.get_price_data(symbol, meta_start, meta_end)

            if df is None or df.empty:
                self.chart_status_var.set(f"No price data available for {symbol}")
                return

            # If any trade dates are missing from the price data, extend the range and refetch once
            trade_dates = {t.entry_date.date() for t in trades_for_symbol} | {t.exit_date.date() for t in trades_for_symbol if t.exit_date}
            missing_dates = [d for d in trade_dates if d not in df.index.date]
            if missing_dates and HAS_YFINANCE:
                extend_to = max(required_end, max(missing_dates) + dt.timedelta(days=7))
                if any(t.exit_date is None for t in trades_for_symbol):
                    extend_to = max(extend_to, dt.date.today() + dt.timedelta(days=1))
                self.chart_status_var.set(f"Extending price data for {symbol}...")
                self.root.update()
                fetch_result = self.price_manager.fetch_and_store(symbol, required_start, extend_to)
                if fetch_result is not None and not fetch_result.empty:
                    df = self.price_manager.get_price_data(symbol, required_start, extend_to)
                    meta_end = extend_to
                else:
                    self.chart_status_var.set(f"Price data missing around trade dates for {symbol}")
            
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

            compare_spy_var = tk.BooleanVar(value=saved_settings.get("compare_spy", False))

            # Update button with save functionality
            def update_and_save():
                self._update_chart_indicators(symbol, df, trades_for_symbol, 
                                             ema1_var, ema2_var, ema3_var,
                                             ema1_type_var, ema2_type_var, ema3_type_var,
                                             ema1_color_var, ema2_color_var, ema3_color_var,
                                             ema1_enabled_var, ema2_enabled_var, ema3_enabled_var,
                                             compare_spy_var)
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
                        "compare_spy": compare_spy_var.get(),
                    }
                )
            
            ttk.Checkbutton(self.chart_controls_frame, text="Compare SPY", variable=compare_spy_var, command=update_and_save).pack(side=tk.LEFT, padx=(10, 4))
            ttk.Button(self.chart_controls_frame, text="Update", command=update_and_save).pack(side=tk.LEFT, padx=5)

            # Build candlestick chart with initial values
            self._plot_candlestick_with_indicators(symbol, df, trades_for_symbol, 
                                                   int(ema1_var.get()), int(ema2_var.get()), int(ema3_var.get()),
                                                   ema1_type_var.get(), ema2_type_var.get(), ema3_type_var.get(),
                                                   ema1_color_var.get(), ema2_color_var.get(), ema3_color_var.get(),
                                                   ema1_enabled_var.get(), ema2_enabled_var.get(), ema3_enabled_var.get(),
                                                   compare_spy_var.get())

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
                                 ema1_enabled_var: tk.BooleanVar, ema2_enabled_var: tk.BooleanVar, ema3_enabled_var: tk.BooleanVar,
                                 compare_spy_var: tk.BooleanVar) -> None:
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
                                               ema1_enabled_var.get(), ema2_enabled_var.get(), ema3_enabled_var.get(),
                                               compare_spy_var.get())

    def _get_comparison_data(self, base_df: pd.DataFrame, symbol: str) -> Optional[pd.DataFrame]:
        """Fetch or load comparison symbol data spanning the base_df date range."""
        start_date = base_df.index.min().date()
        end_date = base_df.index.max().date()
        # Ensure metadata covers range; fetch if needed
        needs_fetch = False
        meta = self.price_manager.get_metadata(symbol)
        if meta:
            try:
                meta_start = dt.datetime.fromisoformat(meta['start_date']).date()
                meta_end = dt.datetime.fromisoformat(meta['end_date']).date()
                if meta_start > start_date or meta_end < end_date:
                    needs_fetch = True
            except Exception:
                needs_fetch = True
        else:
            needs_fetch = True

        if needs_fetch and HAS_YFINANCE:
            try:
                self.chart_status_var.set(f"Updating {symbol} for comparison...")
                self.root.update()
                self.price_manager.fetch_and_store(symbol, start_date, end_date)
            except Exception:
                return None

        return self.price_manager.get_price_data(symbol, start_date, end_date)

    def _plot_candlestick_with_indicators(self, symbol: str, df: pd.DataFrame, trades_for_symbol: list,
                                          ema1_period: int, ema2_period: int, ema3_period: int,
                                          ema1_type: str = "EMA", ema2_type: str = "EMA", ema3_type: str = "EMA",
                                          ema1_color: str = "blue", ema2_color: str = "orange", ema3_color: str = "purple",
                                          ema1_enabled: bool = True, ema2_enabled: bool = True, ema3_enabled: bool = False,
                                          compare_spy: bool = False) -> None:
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

        spy_df = None
        if compare_spy:
            spy_df = self._get_comparison_data(df, "SPY")
            if spy_df is None or spy_df.empty:
                self.chart_status_var.set("Could not load SPY data for comparison.")
                spy_df = None
            else:
                # Align SPY to primary index to avoid mplfinance length errors
                try:
                    spy_df = spy_df.reindex(df.index).ffill()
                except Exception:
                    spy_df = None

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

        if compare_spy and spy_df is not None:
            spy_close = spy_df['close']
            if not spy_close.isna().all():
                apds.append(mpf.make_addplot(spy_close, color="#666666", width=1.3, linestyle='--', secondary_y=True))
        
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
        title_text = f"{symbol} Price Chart" + (" (SPY comparison)" if compare_spy and spy_df is not None else "")

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
        if compare_spy and spy_df is not None:
            legend_elements.append(Line2D([0], [0], color="#666666", lw=1.5, ls='--', label='SPY'))
        
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
        self.refresh_analysis_accounts()
        # Populate table and update summary
        self.populate_table()
        self.update_summary_and_chart()
        # Update chart tab symbol list
        self.update_chart_symbols()
        # Build import results dialog (added first, then duplicates)
        added_txs = self.model.transactions[prev_count:]
        dupe_list = getattr(self.model, 'duplicate_transactions', [])
        try:
            self.show_import_results(added_txs, dupe_list)
        except Exception:
            pass
        # Inform user about duplicates
        try:
            if dupes:
                # Show a summary message and detailed view of duplicates
                messagebox.showinfo("Duplicates Skipped", f"{dupes} duplicate transactions were skipped.")
                # Present the details of duplicates in a separate window
                self.show_duplicate_transactions()
        except Exception:
            pass

    def sync_alerts_to_entry_strategies(self) -> None:
        """Run alert extraction/matching and apply SAME_DAY_ENTRY strategies to trades."""
        scripts_dir = Path(__file__).resolve().parent / "messages-trade-matcher"
        extract_script = scripts_dir / "extract_alerts.py"
        match_script = scripts_dir / "match_alerts_to_trades.py"

        start_prompt = simpledialog.askstring("Sync Alerts", "Start date (YYYY-MM-DD):", initialvalue=self.start_date_var.get() or "")
        if start_prompt is None:
            return
        end_prompt = simpledialog.askstring("Sync Alerts", "End date (YYYY-MM-DD):", initialvalue=self.end_date_var.get() or "")
        if end_prompt is None:
            return

        try:
            start_date = dt.datetime.strptime(start_prompt.strip(), "%Y-%m-%d").date() if start_prompt and start_prompt.strip() else None
            end_date = dt.datetime.strptime(end_prompt.strip(), "%Y-%m-%d").date() if end_prompt and end_prompt.strip() else None
        except ValueError:
            messagebox.showerror("Sync Alerts", "Dates must use YYYY-MM-DD format.")
            return
        if start_date and end_date and start_date > end_date:
            messagebox.showerror("Sync Alerts", "Start date must be on or before end date.")
            return

        if not scripts_dir.exists() or not extract_script.exists() or not match_script.exists():
            messagebox.showerror("Sync Alerts", f"Could not find alert scripts in {scripts_dir}")
            return

        source_db = Path.home() / "Library" / "Messages" / "chat.db"
        dest_db = Path.home() / "Desktop" / "chat_backup.db"
        try:
            dest_db.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(source_db, dest_db)
        except PermissionError:
            if dest_db.exists():
                # If a manual copy already exists, continue without blocking
                messagebox.showinfo(
                    "Sync Alerts",
                    "Could not read Messages database due to macOS privacy settings, using existing Desktop/chat_backup.db instead."
                )
            else:
                messagebox.showerror(
                    "Sync Alerts",
                    "Permission denied reading ~/Library/Messages/chat.db.\n"
                    "Grant Full Disk Access to Python (or VS Code), or manually copy chat.db to Desktop as chat_backup.db and retry."
                )
                return
        except FileNotFoundError:
            messagebox.showerror("Sync Alerts", f"Messages database not found at {source_db}")
            return
        except Exception as e:
            messagebox.showerror("Sync Alerts", f"Failed to copy chat.db: {e}")
            return

        # Persist current journal so matcher sees latest trades
        self.model.save_state(self.persist_path, filter_state=self._current_filter_state())

        for script_path, label in [(extract_script, "Extract alerts"), (match_script, "Match alerts to trades")]:
            try:
                result = subprocess.run([sys.executable, str(script_path)], cwd=scripts_dir, capture_output=True, text=True)
            except Exception as e:
                messagebox.showerror("Sync Alerts", f"{label} failed: {e}")
                return
            if result.returncode != 0:
                err_text = result.stderr.strip() or result.stdout.strip() or "Unknown error"
                messagebox.showerror("Sync Alerts", f"{label} failed (exit {result.returncode}).\n{err_text}")
                return

        alerts_path = scripts_dir / "alerts_matched.csv"
        if not alerts_path.exists():
            messagebox.showerror("Sync Alerts", f"alerts_matched.csv not found at {alerts_path}")
            return
        try:
            df = pd.read_csv(alerts_path)
        except Exception as e:
            messagebox.showerror("Sync Alerts", f"Could not read alerts_matched.csv: {e}")
            return

        required_cols = {"symbol", "strategy", "trade_entry_date", "match_type"}
        if not required_cols.issubset(set(df.columns)):
            missing = required_cols - set(df.columns)
            messagebox.showerror("Sync Alerts", f"alerts_matched.csv is missing columns: {', '.join(sorted(missing))}")
            return

        allowed_match_types = {"SAME_DAY_ENTRY", "TRADE_1D_AFTER_ALERT", "TRADE_2D_AFTER_ALERT"}
        df = df[df["match_type"].isin(allowed_match_types)].copy()
        if df.empty:
            messagebox.showinfo("Sync Alerts", "No SAME_DAY_ENTRY or 1-2 day pre-alert rows found.")
            return

        df["trade_entry_date"] = pd.to_datetime(df["trade_entry_date"], errors="coerce").dt.date
        df["alert_date_parsed"] = pd.to_datetime(df.get("alert_date"), errors="coerce").dt.date
        df = df.dropna(subset=["trade_entry_date", "alert_date_parsed"])
        df["strategy"] = df["strategy"].fillna("").astype(str)
        df["symbol"] = df["symbol"].fillna("").astype(str).str.upper()

        def clean_strategy_name(raw: str) -> str:
            s = raw or ""
            # Strip known noisy prefixes
            bad_chunks = [
                "breakfutpennies-holdingsactual-",
                "breakfutpennies-holdings-",
                "breakfutpennies-hold-",
                "breakfutpennies-",
            ]
            for chunk in bad_chunks:
                s = re.sub(chunk, "", s, flags=re.IGNORECASE)
            # Drop ticker-like tokens (short all-caps/digits) that may have bled into the strategy field
            tokens = re.split(r"[\s,;]+", s)
            kept = [t for t in tokens if not re.fullmatch(r"[A-Z0-9]{1,6}", t)]
            cleaned = " ".join([t for t in kept if t]).strip(" -_,;")
            return cleaned or s.strip()

        df["strategy"] = df["strategy"].apply(clean_strategy_name)

        if start_date:
            df = df[df["trade_entry_date"] >= start_date]
        if end_date:
            df = df[df["trade_entry_date"] <= end_date]
        # Ensure alert is on or before entry and within 2 days gap
        df = df[(df["trade_entry_date"] - df["alert_date_parsed"] <= pd.Timedelta(days=2)) &
                (df["trade_entry_date"] >= df["alert_date_parsed"])]
        df = df[df["strategy"].str.strip() != ""]
        if df.empty:
            messagebox.showinfo("Sync Alerts", "No matching rows after date filtering.")
            return

        trade_lookup: Dict[Tuple[str, dt.date], List[TradeEntry]] = {}
        for trade in self.model.trades:
            trade_lookup.setdefault((trade.symbol.upper(), trade.entry_date.date()), []).append(trade)

        def merge_strategies(existing: str, additions: List[str]) -> str:
            existing_parts = [p.strip() for p in re.split(r"[,\r\n;\t]+", existing) if p.strip()] if existing else []
            combined = list(existing_parts)
            seen_lower = {p.lower() for p in existing_parts}
            for raw in additions:
                clean = raw.strip()
                if not clean:
                    continue
                lower = clean.lower()
                if lower not in seen_lower:
                    combined.append(clean)
                    seen_lower.add(lower)
            return ", ".join(combined)

        updated = 0
        updated_keys = set()
        for _, row in df.iterrows():
            symbol_key = row["symbol"]
            entry_dt = row["trade_entry_date"]
            strategy_text = row["strategy"]
            for trade in trade_lookup.get((symbol_key, entry_dt), []):
                trade_key = self.model.compute_key(trade)
                merged = merge_strategies(self.model.entry_strategies.get(trade_key, ""), [strategy_text])
                if merged != self.model.entry_strategies.get(trade_key, ""):
                    self.model.entry_strategies[trade_key] = merged
                    updated += 1
                    updated_keys.add(trade_key)

        if not updated:
            messagebox.showinfo("Sync Alerts", "No trades were updated for the selected range.")
            return

        self.populate_table()
        self.model.save_state(self.persist_path, filter_state=self._current_filter_state())
        # Build a short sample list to help locate updated rows
        samples = []
        for trade in self.model.trades:
            key = self.model.compute_key(trade)
            if key in updated_keys:
                samples.append(f"{trade.symbol} @ {trade.entry_date.date()} → {self.model.entry_strategies.get(key, '')}")
                if len(samples) >= 5:
                    break
        sample_text = "\n".join(samples)
        extra_hint = "\n\nIf you don't see them, clear date filters or expand grouped symbols." if samples else ""
        messagebox.showinfo(
            "Sync Alerts",
            f"Updated entry strategies on {len(updated_keys)} trade(s).{extra_hint}" + (f"\n\nExamples:\n{sample_text}" if samples else "")
        )

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

    def toggle_chart_visibility(self) -> None:
        """Toggle the visibility of the equity curve pane."""
        panes = list(self.left_paned.panes())
        chart_id = str(self.chart_frame)
        if self.chart_visible.get():
            # Hide chart if present
            if chart_id in panes:
                try:
                    self.left_paned.remove(self.chart_frame)
                except Exception:
                    pass
            self.chart_visible.set(False)
            self.toggle_chart_btn.config(text="Show Chart")
        else:
            # Show chart at the end if not already present
            panes = list(self.left_paned.panes())
            if chart_id not in panes:
                try:
                    self.left_paned.add(self.chart_frame, weight=1)
                except Exception:
                    pass
            self.chart_visible.set(True)
            self.toggle_chart_btn.config(text="Hide Chart")
        # Redraw chart only when visible
        if self.chart_visible.get():
            self.update_summary_and_chart()
        # Persist visibility state
        try:
            self.model.save_state(self.persist_path, filter_state=self._current_filter_state())
        except Exception:
            pass

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
        # Split strategies on commas, carriage returns, semicolons, and tabs
        def extract_individual_strategies(strategy_str: str) -> set:
            """Extract individual strategies from string with multiple delimiters (comma, CR, semicolon, tab)."""
            if not strategy_str:
                return set()
            # Split on comma, carriage return (\r), newline (\n), semicolon, and tab
            strategies = re.split(r'[,\r\n;\t]+', strategy_str)
            return {s.strip() for s in strategies if s.strip()}
        
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
        self.entry_strategy_all_values = ["all"] + sorted(list(entry_strategies))
        self.exit_strategy_all_values = ["all"] + sorted(list(exit_strategies))
        self.entry_strategy_filter_combo['values'] = self.entry_strategy_all_values
        self.exit_strategy_filter_combo['values'] = self.exit_strategy_all_values
        # Determine filters
        closed_only = self.closed_only_var.get()
        open_only = self.open_only_var.get()
        account_filter = self.account_var.get()
        group_by_symbol = self.group_var.get()
        entry_strategy_filter = self.entry_strategy_filter_var.get()
        exit_strategy_filter = self.exit_strategy_filter_var.get()
        symbol_filter_tokens = self._parsed_symbol_filter()
        # Determine sort parameters
        sort_by = self.sort_by
        descending = self.sort_descending

        # Helper to determine if a trade should be shown based on filters
        def parse_strategies(strategy_str: str) -> list:
            """Parse strategies with multiple delimiters (comma, CR, semicolon, tab) into a list, trimmed and lowercased."""
            if not strategy_str:
                return []
            # Split on comma, carriage return (\r), newline (\n), semicolon, and tab
            strategies = re.split(r'[,\r\n;\t]+', strategy_str)
            return [s.strip().lower() for s in strategies if s.strip()]
        
        def trade_visible(index: int, trade: TradeEntry) -> bool:
            # Apply top filter set if present
            if hasattr(self, 'top_filter_set') and self.top_filter_set is not None:
                if index not in self.top_filter_set:
                    return False
            # Account filter
            if account_filter and account_filter != "all" and trade.account_number != account_filter:
                return False
            # Symbol filter (comma-separated tokens)
            if symbol_filter_tokens and trade.symbol.upper() not in symbol_filter_tokens:
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
            # Closed-only / Open-only filter
            if closed_only:
                if not trade.is_closed:
                    return False
                if trade.buy_id < 0:
                    return False
            if open_only:
                if trade.is_closed:
                    return False
            # Date filter on entry_date (inclusive)
            if self.start_date and trade.entry_date.date() < self.start_date:
                return False
            if self.end_date and trade.entry_date.date() > self.end_date:
                return False
            # Date filter on exit_date (inclusive)
            if self.exit_start_date or self.exit_end_date:
                if not trade.exit_date:
                    return False
                exit_date = trade.exit_date.date()
                if self.exit_start_date and exit_date < self.exit_start_date:
                    return False
                if self.exit_end_date and exit_date > self.exit_end_date:
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
                    entry_strategy_display = format_strategy_for_table(entry_strategy_str)
                    exit_strategy_display = format_strategy_for_table(exit_strategy_str)
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
                        entry_strategy_display,
                        exit_strategy_display,
                        note_str,
                    )
                    # Use the numeric index as iid for child to allow mapping notes back
                    row_id = str(idx)
                    self.tree.insert(group_id, "end", iid=row_id, text="", values=row)
                    self.id_to_key[row_id] = key
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
                entry_strategy_display = format_strategy_for_table(entry_strategy_str)
                exit_strategy_display = format_strategy_for_table(exit_strategy_str)
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
                    entry_strategy_display,
                    exit_strategy_display,
                    note_str,
                )
                row_id = str(idx)
                self.tree.insert("", "end", iid=row_id, values=row)
                self.id_to_key[row_id] = self.model.compute_key(trade)
        
        # Auto-fit columns to content
        self.autofit_columns()
        
        # Bind tooltip functionality for strategy columns
        self.setup_tree_tooltips()

    def setup_tree_tooltips(self) -> None:
        """Setup motion-based tooltips for entry and exit strategy columns in the tree."""
        # Bind fresh (avoid stacking multiple bindings across repaints)
        self.tree.unbind("<Motion>")
        self.tree.unbind("<Leave>")
        self.tree.bind("<Motion>", self.on_tree_motion)
        self.tree.bind("<Leave>", self.on_tree_leave)

    def _hide_tree_tooltip(self) -> None:
        if self._tree_tooltip_win is not None:
            try:
                self._tree_tooltip_win.destroy()
            except Exception:
                pass
        self._tree_tooltip_win = None
        self._tree_tooltip_label = None
        self._tree_tooltip_last = (None, None, None)

    def _show_tree_tooltip(self, text: str, x_root: int, y_root: int) -> None:
        if not text:
            self._hide_tree_tooltip()
            return
        if self._tree_tooltip_win is None:
            win = tk.Toplevel(self.root)
            win.wm_overrideredirect(True)
            label = tk.Label(
                win,
                text=text,
                background="#ffffe0",
                foreground="#000000",  # Force dark text for readability
                relief=tk.SOLID,
                borderwidth=1,
                font=("TkDefaultFont", 9),
                justify=tk.LEFT,
                anchor="nw",
                wraplength=400,  # Wrap text at 400 pixels
            )
            label.pack(ipadx=6, ipady=3)
            self._tree_tooltip_win = win
            self._tree_tooltip_label = label
        else:
            # Update text if it changed
            if self._tree_tooltip_label is not None:
                self._tree_tooltip_label.configure(text=text, foreground="#000000")
        # Force geometry after text update so the window resizes correctly
        self._tree_tooltip_win.update_idletasks()
        self._tree_tooltip_win.wm_geometry(f"+{x_root}+{y_root}")
    
    def on_tree_motion(self, event: tk.Event) -> None:
        """Show tooltip when hovering over entry/exit strategy cells."""
        try:
            # Only show tooltips over data cells (not headings/separators)
            region = self.tree.identify_region(event.x, event.y)
            if region != "cell":
                self._hide_tree_tooltip()
                return

            item_id = self.tree.identify_row(event.y)
            col_id = self.tree.identify_column(event.x)  # '#1'..'#N' for data columns
            if not item_id or not col_id or col_id == "#0":
                self._hide_tree_tooltip()
                return

            try:
                col_index = int(col_id[1:]) - 1
            except Exception:
                self._hide_tree_tooltip()
                return

            columns = list(self.tree["columns"])  # data columns only
            if col_index < 0 or col_index >= len(columns):
                self._hide_tree_tooltip()
                return

            col_name = columns[col_index]
            if col_name not in {"entry_strategy", "exit_strategy"}:
                self._hide_tree_tooltip()
                return

            # Get the key for this row from our id_to_key mapping
            key = self.id_to_key.get(item_id)
            
            # Try to get the original strategy text from the model
            strategy_text = ""
            if key:
                if col_name == "entry_strategy":
                    strategy_text = self.model.entry_strategies.get(key, "")
                else:  # exit_strategy
                    strategy_text = self.model.exit_strategies.get(key, "")
            
            # If we have no strategy text, get the displayed text from the tree
            if not strategy_text:
                cell_text = str(self.tree.set(item_id, col_name) or "").strip()
                strategy_text = cell_text
            
            # If still no text, hide tooltip
            if not strategy_text:
                self._hide_tree_tooltip()
                return
            
            # Format multi-strategy text as one-per-line for tooltip display
            # Split on common delimiters (comma, CR/LF, semicolon, tab, and multiple spaces)
            # This handles: "item1, item2" or "item1;item2" or "item1\nitem2" or "item1   item2" etc.
            tooltip_lines = [p.strip() for p in re.split(r"[,\r\n;\t]|[ ]{2,}", strategy_text) if p.strip()]
            if tooltip_lines:
                tooltip_text = "\n".join(tooltip_lines)
            else:
                tooltip_text = strategy_text.strip()
            
            if not tooltip_text:
                self._hide_tree_tooltip()
                return

            last_item, last_col, last_text = self._tree_tooltip_last
            if (item_id, col_name, tooltip_text) != (last_item, last_col, last_text):
                self._tree_tooltip_last = (item_id, col_name, tooltip_text)
                self._show_tree_tooltip(tooltip_text, event.x_root + 12, event.y_root + 12)
            else:
                # Just move the tooltip with the mouse
                if self._tree_tooltip_win is not None:
                    self._tree_tooltip_win.wm_geometry(f"+{event.x_root + 12}+{event.y_root + 12}")
        except Exception as e:
            # Silently ignore tooltip errors to avoid disrupting the main UI
            self._hide_tree_tooltip()
    
    def on_tree_leave(self, event: tk.Event = None) -> None:
        """Hide tooltip when mouse leaves the tree."""
        self._hide_tree_tooltip()

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
                resolved_path = self._resolve_screenshot_path(ss_list[0]["filepath"])
                self._update_screenshot_preview(resolved_path)
            else:
                self.screenshot_var.set("(none)")
                # Explicitly clear the preview image to prevent persistence
                self.screenshot_preview_label.configure(image="")
                self.screenshot_preview_label.image = None
        else:
            # Aggregated or unknown row selected
            self.note_text.delete("1.0", tk.END)
            self.entry_strategy_text.delete("1.0", tk.END)
            self.exit_strategy_text.delete("1.0", tk.END)
            self.screenshot_var.set("(none)")
            # Explicitly clear the preview image to prevent persistence
            self.screenshot_preview_label.configure(image="")
            self.screenshot_preview_label.image = None
    
    def on_tree_double_click(self, event: tk.Event) -> None:
        """Handle double-click: view screenshots on screenshot column, otherwise open edit dialog."""
        item_id = self.tree.identify("item", event.x, event.y)
        column = self.tree.identify("column", event.x, event.y)
        
        if not item_id or not column:
            return
        
        # The columns are: account(#1), symbol(#2), entry_date(#3), entry_price(#4), 
        # exit_date(#5), exit_price(#6), quantity(#7), pnl(#8), pnl_pct(#9), hold_period(#10),
        # screenshot(#11), entry_strategy(#12), exit_strategy(#13), note(#14)
        if column == "#11":
            key = self.id_to_key.get(item_id)
            if key is None:
                return
            if key in self.model.screenshots and self.model.screenshots[key]:
                self.view_screenshots()
            return

        # For non-screenshot columns, open edit dialog on a trade row
        if item_id in self.id_to_key:
            try:
                self.edit_selected_transaction()
            except Exception:
                pass

    def _auto_save_fields(self) -> None:
        """Auto-save note and strategies whenever fields are modified."""
        selected = self.tree.selection()
        if not selected:
            return
        item_id = selected[0]
        key = self.id_to_key.get(item_id)
        if key is None:
            return
        
        # Auto-save note
        note = self.note_text.get("1.0", tk.END).strip()
        if note:
            self.model.notes[key] = note
        elif key in self.model.notes:
            del self.model.notes[key]
        
        # Auto-save entry strategy
        entry_strategy = self.entry_strategy_text.get("1.0", tk.END).strip()
        if entry_strategy:
            self.model.entry_strategies[key] = entry_strategy
        elif key in self.model.entry_strategies:
            del self.model.entry_strategies[key]
        
        # Auto-save exit strategy
        exit_strategy = self.exit_strategy_text.get("1.0", tk.END).strip()
        if exit_strategy:
            self.model.exit_strategies[key] = exit_strategy
        elif key in self.model.exit_strategies:
            del self.model.exit_strategies[key]

        # Refresh the visible row in the tree so changes show immediately
        entry_display = format_strategy_for_table(entry_strategy)
        exit_display = format_strategy_for_table(exit_strategy)
        self.tree.set(item_id, "entry_strategy", entry_display)
        self.tree.set(item_id, "exit_strategy", exit_display)
        self.tree.set(item_id, "note", note)
        
        # Persist changes to disk
        self.model.save_state(self.persist_path, filter_state=self._current_filter_state())

    def _current_filter_state(self) -> dict:
        """Return current UI filter state for persistence."""
        return {
            "account": self.account_var.get(),
            "account_filter": self.account_var.get(),
            "symbol_filter": self.symbol_filter_var.get(),
            "start_date": self.start_date_var.get(),
            "end_date": self.end_date_var.get(),
            "exit_start_date": self.exit_start_date_var.get(),
            "exit_end_date": self.exit_end_date_var.get(),
            "closed_only": self.closed_only_var.get(),
            "group_by_symbol": self.group_var.get(),
            "entry_strategy_filter": self.entry_strategy_filter_var.get(),
            "exit_strategy_filter": self.exit_strategy_filter_var.get(),
            "chart_visible": self.chart_visible.get(),
            "analysis2_starting_balances": self.analysis2_starting_balances,
            "analysis2_year": self.analysis2_year_var.get(),
        }

    def _reset_text_sizes(self) -> None:
        """Reset entry/exit strategy text boxes to default heights."""
        self.entry_text_height.set(self.entry_text_default_height)
        self.exit_text_height.set(self.exit_text_default_height)
        self.entry_strategy_text.configure(height=self.entry_text_default_height)
        self.exit_strategy_text.configure(height=self.exit_text_default_height)

    def _resolve_screenshot_path(self, filepath: str) -> str:
        """Resolve a screenshot filepath (relative or absolute) to absolute path.
        
        If the path is relative, resolve it relative to the journal directory.
        If it's absolute and doesn't exist, try to resolve it as-is.
        """
        # If path is absolute and exists, use it
        if os.path.isabs(filepath) and os.path.exists(filepath):
            return filepath
        
        # Try relative to journal directory (using persist_path as reference)
        journal_dir = os.path.dirname(os.path.abspath(self.persist_path))
        rel_path = os.path.join(journal_dir, filepath)
        if os.path.exists(rel_path):
            return rel_path
        
        # Fall back to original (may not exist, but let the caller handle the error)
        return filepath
    
    def _make_screenshot_path_relative(self, filepath: str) -> str:
        """Convert an absolute screenshot path to relative (relative to journal directory)."""
        try:
            journal_dir = os.path.dirname(os.path.abspath(self.persist_path))
            # Try to make it relative
            rel_path = os.path.relpath(filepath, journal_dir)
            # If the relative path uses .., it means the file is outside the journal dir
            # In that case, keep it absolute for now (user can organize later)
            if not rel_path.startswith(".."):
                return rel_path
        except (ValueError, TypeError):
            pass
        return filepath
    
    def _collect_existing_screenshot_paths(self) -> Set[str]:
        """Return a set of absolute paths for all attached screenshots (dedupe guard)."""
        existing: Set[str] = set()
        for entries in self.model.screenshots.values():
            for s in entries:
                resolved = self._resolve_screenshot_path(s.get("filepath", ""))
                existing.add(os.path.abspath(resolved))
        return existing

    def _parse_screenshot_filename(self, filename: str, full_path: str) -> Tuple[Optional[str], dt.date]:
        """Extract symbol and date from filename; fall back to file modified date for date.

        Expected patterns include SYMBOL_YYYY-MM-DD_HHMM or SYMBOLYYYYMMDD.*
        Symbol is taken as the leading alphabetic token. Date formats supported: YYYY-MM-DD,
        YYYYMMDD, MM-DD-YYYY, MMDDYYYY. If no date is found, use file modified date.
        """
        base = os.path.splitext(os.path.basename(filename))[0]
        tokens = re.split(r'[^A-Za-z0-9]+', base)
        symbol: Optional[str] = None
        if tokens and tokens[0].isalpha():
            symbol = tokens[0].upper()

        date: Optional[dt.date] = None
        # Try date patterns in order
        date_patterns = [
            r'(20\d{2})[-_ ]?(\d{2})[-_ ]?(\d{2})',      # YYYY-MM-DD or YYYYMMDD
            r'(\d{2})[-_ ]?(\d{2})[-_ ]?(20\d{2})',      # MM-DD-YYYY or MMDDYYYY
        ]
        for pat in date_patterns:
            m = re.search(pat, base)
            if not m:
                continue
            try:
                if pat.startswith('('):
                    if len(m.groups()) == 3:
                        g1, g2, g3 = m.groups()
                        if pat.startswith('(20'):  # YYYY-MM-DD
                            y, mm, dd = int(g1), int(g2), int(g3)
                        else:  # MM-DD-YYYY
                            mm, dd, y = int(g1), int(g2), int(g3)
                        date = dt.date(y, mm, dd)
                        break
            except Exception:
                continue
        if date is None:
            try:
                ts = os.path.getmtime(full_path)
                date = dt.datetime.fromtimestamp(ts).date()
            except Exception:
                date = dt.date.today()
        return symbol, date

    def _attach_screenshot_to_trade(self, trade_key: tuple, filepath: str, label: str) -> bool:
        """Attach screenshot to a trade unless that trade already has it (per-trade dedupe by path or filename)."""
        stored_path = self._make_screenshot_path_relative(filepath)
        new_base = os.path.basename(stored_path).lower()
        if trade_key not in self.model.screenshots:
            self.model.screenshots[trade_key] = []
        for s in self.model.screenshots[trade_key]:
            existing_path = s.get("filepath", "")
            if not existing_path:
                continue
            if existing_path == stored_path:
                return False
            if os.path.basename(existing_path).lower() == new_base:
                return False
        self.model.screenshots[trade_key].append({"filepath": stored_path, "label": label})
        return True

    def add_screenshot(self) -> None:
        """Add a screenshot file to the selected trade with optional label and notes."""
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
        
        # Show preview dialog with image, label, and notes
        preview_win = tk.Toplevel(self.root)
        preview_win.title("Screenshot Preview & Trade Notes")
        preview_win.geometry("600x800")
        
        # Load and display the image
        photo = None
        try:
            from PIL import Image, ImageTk  # type: ignore
            img = Image.open(filepath)
            img.thumbnail((450, 300))
            photo = ImageTk.PhotoImage(img)
        except Exception:
            try:
                photo = tk.PhotoImage(file=filepath)
            except Exception:
                photo = None
        
        if photo:
            img_label = tk.Label(preview_win, image=photo)
            img_label.image = photo
            img_label.pack(pady=10, padx=10, fill=tk.BOTH, expand=True)
        else:
            error_label = tk.Label(preview_win, text="Could not load image preview", fg="red")
            error_label.pack(pady=10)
        
        # Screenshot label input frame
        label_frame = ttk.LabelFrame(preview_win, text="Screenshot Label (optional)")
        label_frame.pack(fill=tk.X, padx=10, pady=(10, 5))
        
        label_entry = tk.Text(label_frame, height=2, width=60)
        label_entry.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Screenshot notes frame
        notes_frame = ttk.LabelFrame(preview_win, text="Screenshot Notes (optional)")
        notes_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        notes_entry = tk.Text(notes_frame, height=6, width=60)
        notes_entry.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # No existing per-screenshot notes yet; leave blank
        
        # Buttons
        button_frame = ttk.Frame(preview_win)
        button_frame.pack(fill=tk.X, padx=10, pady=10)
        
        def save_on_close():
            """Auto-save screenshot and notes when dialog closes."""
            label = label_entry.get("1.0", tk.END).strip()
            notes = notes_entry.get("1.0", tk.END).strip()
            
            # Add screenshot to list (initialize if needed)
            if key not in self.model.screenshots:
                self.model.screenshots[key] = []
            
            # Store path as relative if possible (for portability across machines)
            stored_path = self._make_screenshot_path_relative(filepath)
            
            # Create screenshot entry with filepath, label, and optional note
            screenshot_entry = {
                "filepath": stored_path,
                "label": label if label else os.path.basename(filepath),
                "note": notes,
            }
            
            # Check if this file is already attached
            if not any(s["filepath"] == stored_path for s in self.model.screenshots[key]):
                self.model.screenshots[key].append(screenshot_entry)
            
            # Update note for this screenshot if we just added it
            if notes and any(s.get("filepath") == stored_path for s in self.model.screenshots[key]):
                for s in self.model.screenshots[key]:
                    if s.get("filepath") == stored_path:
                        s["note"] = notes
                        break

            # Append to trade-level notes (continuous field)
            if notes:
                existing = (self.model.notes.get(key, "") or "").rstrip()
                new_note = existing
                if new_note:
                    if not new_note.endswith("\n"):
                        new_note += "\n\n"
                    else:
                        new_note += "\n"
                new_note += notes
                self.model.notes[key] = new_note
            
            # Update screenshot display
            num_screenshots = len(self.model.screenshots[key])
            self.screenshot_var.set(f"{num_screenshots} screenshot(s)")
            # Load preview of the first screenshot
            if self.model.screenshots[key]:
                resolved_path = self._resolve_screenshot_path(self.model.screenshots[key][0]["filepath"])
                self._update_screenshot_preview(resolved_path)
            
            # Refresh the main display to show updated notes and everything
            # If this trade is still selected, reload all its data
            current_selection = self.tree.selection()
            if current_selection and item_id in current_selection:
                # Trigger the on_tree_select logic to refresh all displays
                self.on_tree_select(None)

        def cancel_it():
            preview_win.destroy()

        # Save automatically on close
        preview_win.protocol("WM_DELETE_WINDOW", lambda: (save_on_close(), preview_win.destroy()))

        ttk.Button(button_frame, text="Add Screenshot", command=lambda: (save_on_close(), preview_win.destroy())).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Cancel", command=cancel_it).pack(side=tk.LEFT, padx=5)

    def scan_screenshot_folder(self) -> None:
        """Scan a folder for chart screenshots and auto-attach to trades by symbol and date.

        Rules:
        - Symbol comes from the leading token in the filename (letters only, uppercased).
        - Date comes from the filename (YYYY-MM-DD / YYYYMMDD / MM-DD-YYYY / MMDDYYYY); if missing,
          falls back to file modified date.
        - Matches trades on same symbol (case-insensitive) and same entry or exit date.
        - Balances attachments across matching trades by choosing the trade with the fewest screenshots
          so far (per-trade dedupe only).
        """
        # Remember selection so we can restore focus after refreshing the table
        selected_key = None
        selected_item = None
        current_selection = self.tree.selection()
        if current_selection:
            selected_item = current_selection[0]
            selected_key = self.id_to_key.get(selected_item)

        folder = filedialog.askdirectory(title="Select screenshot folder to scan")
        if not folder:
            return

        exts = {".png", ".jpg", ".jpeg", ".gif", ".bmp"}
        added = 0
        skipped_duplicate = 0
        skipped_no_symbol = 0
        skipped_no_match = 0

        # Precompute trade keys and dates for matching
        trade_info: List[Tuple[tuple, str, dt.date, Optional[dt.date]]] = []
        screenshot_counts: Dict[tuple, int] = {}
        for trade in self.model.trades:
            key = self.model.compute_key(trade)
            entry_d = trade.entry_date.date()
            exit_d = trade.exit_date.date() if trade.exit_date else None
            trade_info.append((key, trade.symbol.upper(), entry_d, exit_d))
            screenshot_counts[key] = len(self.model.screenshots.get(key, []))

        for root, _, files in os.walk(folder):
            for fname in files:
                ext = os.path.splitext(fname)[1].lower()
                if ext not in exts:
                    continue
                full_path = os.path.abspath(os.path.join(root, fname))
                symbol, shot_date = self._parse_screenshot_filename(fname, full_path)
                if not symbol:
                    skipped_no_symbol += 1
                    continue

                symbol_upper = symbol.upper()
                candidates: List[Tuple[tuple, bool, bool]] = []  # (trade_key, is_entry_match, is_exit_match)
                for trade_key, trade_symbol, entry_d, exit_d in trade_info:
                    if trade_symbol != symbol_upper:
                        continue
                    is_exit_match = exit_d is not None and shot_date == exit_d
                    is_entry_match = shot_date == entry_d
                    if not (is_entry_match or is_exit_match):
                        continue
                    candidates.append((trade_key, is_entry_match, is_exit_match))

                if not candidates:
                    skipped_no_match += 1
                    continue

                # Prefer entry-date matches, then fewer screenshots (balances multi-lot days)
                candidates.sort(key=lambda item: (0 if item[1] else 1, screenshot_counts.get(item[0], 0), item[0]))
                target_key = candidates[0][0]

                if self._attach_screenshot_to_trade(target_key, full_path, os.path.basename(fname)):
                    added += 1
                    screenshot_counts[target_key] = screenshot_counts.get(target_key, 0) + 1
                else:
                    skipped_duplicate += 1

        # Rebuild the table so screenshot indicators update immediately
        self.populate_table()

        # Restore the previously selected trade if possible
        if selected_key is not None:
            for iid, key in self.id_to_key.items():
                if key == selected_key:
                    self.tree.selection_set(iid)
                    self.tree.focus(iid)
                    self.tree.see(iid)
                    break
        elif selected_item:
            # If a non-trade row was selected, keep the first row highlighted for continuity
            children = self.tree.get_children("")
            if children:
                self.tree.selection_set(children[0])
                self.tree.focus(children[0])

        if self.tree.selection():
            self.on_tree_select(None)

        messagebox.showinfo(
            "Scan Complete",
            (
                f"Added: {added}\n"
                f"Skipped duplicates: {skipped_duplicate}\n"
                f"Skipped (no symbol in name): {skipped_no_symbol}\n"
                f"Skipped (no matching trade on that date): {skipped_no_match}"
            ),
        )

    def scan_notes_folder(self) -> None:
        """Scan a folder for text note files and append them to matching trades by symbol and date.

        Matching rules mirror screenshot parsing:
        - Symbol comes from the leading token in the filename (letters only, uppercased).
        - Date comes from the filename (YYYY-MM-DD / YYYYMMDD / MM-DD-YYYY / MMDDYYYY); if missing,
          falls back to file modified date.
        - A match occurs when the date equals a trade's entry date or exit date (if present) and the symbol matches.
        - Notes are appended only if their text is not already present in the trade's existing notes.
        """
        # Preserve current selection to restore after table refresh
        selected_key = None
        selected_item = None
        current_selection = self.tree.selection()
        if current_selection:
            selected_item = current_selection[0]
            selected_key = self.id_to_key.get(selected_item)

        folder = filedialog.askdirectory(title="Select notes folder to scan")
        if not folder:
            return

        exts = {".txt"}
        added = 0
        skipped_no_symbol = 0
        skipped_no_match = 0
        skipped_empty = 0
        skipped_duplicate = 0

        # Precompute trade info for matching
        trade_info: List[Tuple[tuple, str, dt.date, Optional[dt.date]]] = []
        for trade in self.model.trades:
            key = self.model.compute_key(trade)
            entry_d = trade.entry_date.date()
            exit_d = trade.exit_date.date() if trade.exit_date else None
            trade_info.append((key, trade.symbol.upper(), entry_d, exit_d))

        for root, _, files in os.walk(folder):
            for fname in files:
                ext = os.path.splitext(fname)[1].lower()
                if ext not in exts:
                    continue
                full_path = os.path.abspath(os.path.join(root, fname))
                symbol, note_date = self._parse_screenshot_filename(fname, full_path)
                if not symbol:
                    skipped_no_symbol += 1
                    continue

                symbol_upper = symbol.upper()
                candidates: List[tuple] = []
                for trade_key, trade_symbol, entry_d, exit_d in trade_info:
                    if trade_symbol != symbol_upper:
                        continue
                    is_entry_match = note_date == entry_d
                    is_exit_match = exit_d is not None and note_date == exit_d
                    if not (is_entry_match or is_exit_match):
                        continue
                    candidates.append(trade_key)

                if not candidates:
                    skipped_no_match += 1
                    continue

                # Load note text
                try:
                    with open(full_path, "r", encoding="utf-8", errors="ignore") as f:
                        note_text = f.read().strip()
                except Exception:
                    skipped_empty += 1
                    continue

                if not note_text:
                    skipped_empty += 1
                    continue

                # Append note to each candidate trade, avoiding duplicates and stamping with note date
                stamp = note_date.strftime("%Y-%m-%d") if note_date else dt.date.today().strftime("%Y-%m-%d")
                stamped_note = f"[{stamp}]\n{note_text}"
                for trade_key in candidates:
                    existing = self.model.notes.get(trade_key, "") or ""
                    if stamped_note in existing:
                        skipped_duplicate += 1
                        continue
                    new_note = existing.rstrip()
                    if new_note:
                        if not new_note.endswith("\n"):
                            new_note += "\n\n"
                        else:
                            new_note += "\n"
                    new_note += stamped_note
                    self.model.notes[trade_key] = new_note
                    added += 1

        # Refresh table so notes column updates
        self.populate_table()

        # Restore previous selection if possible
        if selected_key is not None:
            for iid, key in self.id_to_key.items():
                if key == selected_key:
                    self.tree.selection_set(iid)
                    self.tree.focus(iid)
                    self.tree.see(iid)
                    break
        elif selected_item:
            children = self.tree.get_children("")
            if children:
                self.tree.selection_set(children[0])
                self.tree.focus(children[0])

        if self.tree.selection():
            self.on_tree_select(None)

        # Persist updated notes
        try:
            self.model.save_state(self.persist_path, filter_state=self._current_filter_state())
        except Exception:
            pass

        messagebox.showinfo(
            "Scan Complete",
            (
                f"Notes appended: {added}\n"
                f"Skipped (duplicate note text): {skipped_duplicate}\n"
                f"Skipped (empty or unreadable file): {skipped_empty}\n"
                f"Skipped (no symbol in name): {skipped_no_symbol}\n"
                f"Skipped (no matching trade on that date): {skipped_no_match}"
            ),
        )
    
    def _update_screenshot_preview(self, filepath: str) -> None:
        """Load and display a preview of the given screenshot."""
        # Resolve the filepath (handles relative paths)
        resolved_path = self._resolve_screenshot_path(filepath)
        
        photo = None
        try:
            from PIL import Image, ImageTk  # type: ignore
            img = Image.open(resolved_path)
            img.thumbnail((200, 200))
            photo = ImageTk.PhotoImage(img)
        except Exception:
            try:
                photo = tk.PhotoImage(file=resolved_path)
            except Exception:
                photo = None
        if photo:
            self.screenshot_preview_label.configure(image=photo)
            self.screenshot_preview_label.image = photo
        else:
            self.screenshot_preview_label.configure(image="")
            self.screenshot_preview_label.image = None

    def _rebuild_trade_notes_from_screenshots(self, trade_key: tuple) -> None:
        """Append per-screenshot notes into the trade-level notes without erasing existing text."""
        shots = self.model.screenshots.get(trade_key, []) or []
        existing = (self.model.notes.get(trade_key, "") or "").rstrip()
        new_note = existing
        for idx, s in enumerate(shots, start=1):
            note = (s.get("note") or "").strip()
            if not note:
                continue
            label = (s.get("label") or f"Screenshot {idx}").strip()
            block = f"[{label}]\n{note}"
            if block in new_note:
                continue
            if new_note:
                if not new_note.endswith("\n"):
                    new_note += "\n\n"
                else:
                    new_note += "\n"
            new_note += block
        # Only update if there's anything to store; never delete existing notes here
        if new_note:
            self.model.notes[trade_key] = new_note

    def view_screenshots(self) -> None:
        """Open a window showing all screenshots for the selected trade with labels, notes, and removal option."""
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
        ss_window.geometry("900x800")
        ss_window.bind("<Escape>", lambda e: (save_changes(), ss_window.destroy()))
        
        screenshots = self.model.screenshots[key]
        
        # Create a frame for navigation buttons and image display
        nav_frame = ttk.Frame(ss_window)
        nav_frame.pack(fill=tk.X, padx=10, pady=10)
        
        # Track current screenshot index
        current_index = [0]
        if not hasattr(self, "screenshot_zoom_level"):
            self.screenshot_zoom_level = 1.0
        zoom_level = [self.screenshot_zoom_level]
        
        def update_image():
            """Update the displayed image, label, and notes."""
            screenshot_data = screenshots[current_index[0]]
            filepath = screenshot_data["filepath"]
            resolved_path = self._resolve_screenshot_path(filepath)
            label = screenshot_data.get("label", "")

            try:
                from PIL import Image, ImageTk  # type: ignore
                img = Image.open(resolved_path)
                w, h = img.size
                # Base scale to fit visible canvas area
                max_w = max(1, img_canvas.winfo_width())
                max_h = max(1, img_canvas.winfo_height())
                base_scale = min(max_w / w, max_h / h)
                scale = max(0.1, min(4.0, base_scale * zoom_level[0]))
                img = img.resize((int(w * scale), int(h * scale)), Image.Resampling.LANCZOS)
                photo = ImageTk.PhotoImage(img)
                img_canvas.delete("all")
                img_canvas.create_image(0, 0, image=photo, anchor="nw")
                img_canvas.image = photo
                img_canvas.configure(scrollregion=(0, 0, img.width, img.height))
            except Exception as e:
                img_canvas.delete("all")
                img_canvas.create_text(10, 10, text=f"Could not load image: {e}", anchor="nw", fill="white")
            
            # Update labels
            counter_text = f"Screenshot {current_index[0] + 1} of {len(screenshots)}"
            counter_label.config(text=counter_text)

            # Update label field
            label_text.config(state=tk.NORMAL)
            label_text.delete("1.0", tk.END)
            label_text.insert("1.0", label)
            label_text.config(state=tk.NORMAL)

            # Update notes field (continuous trade-level notes)
            notes = self.model.notes.get(key, "") or ""
            notes_text.config(state=tk.NORMAL)
            notes_text.delete("1.0", tk.END)
            notes_text.insert("1.0", notes)
            notes_text.config(state=tk.NORMAL)
        
        def prev_image():
            if current_index[0] > 0:
                save_notes_only()
                current_index[0] -= 1
                update_image()
        
        def next_image():
            if current_index[0] < len(screenshots) - 1:
                save_notes_only()
                current_index[0] += 1
                update_image()
        
        def remove_current():
            """Remove the currently displayed screenshot."""
            if messagebox.askyesno("Remove Screenshot", f"Remove this screenshot ('{screenshots[current_index[0]].get('label', 'Untitled')}')?"):
                save_notes_only()
                screenshots.pop(current_index[0])
                if not screenshots:
                    # No more screenshots, close window
                    messagebox.showinfo("Removed", "All screenshots have been removed.")
                    ss_window.destroy()
                    # Clear preview in main window
                    self.screenshot_var.set("(none)")
                    self.screenshot_preview_label.configure(image="")
                    self.screenshot_preview_label.image = None
                else:
                    # Adjust index if needed
                    if current_index[0] >= len(screenshots):
                        current_index[0] = len(screenshots) - 1
                    update_image()
        
        def save_notes_only():
            """Persist current trade-level notes from the text widget."""
            new_notes = notes_text.get("1.0", tk.END).strip()
            self.model.notes[key] = new_notes
            screenshots[current_index[0]]["note"] = new_notes

        def save_changes():
            """Auto-save label and notes changes."""
            # Get label from text widget
            new_label = label_text.get("1.0", tk.END).strip()
            screenshots[current_index[0]]["label"] = new_label if new_label else os.path.basename(screenshots[current_index[0]]["filepath"])

            save_notes_only()
            
            # Refresh main tree display if this trade is still selected
            current_selection = self.tree.selection()
            if current_selection and item_id in current_selection:
                self.on_tree_select(None)
        
        # Setup auto-save when dialog closes
        ss_window.protocol("WM_DELETE_WINDOW", lambda: (save_changes(), ss_window.destroy()))
        
        # Navigation buttons
        prev_btn = ttk.Button(nav_frame, text="← Previous", command=prev_image)
        prev_btn.pack(side=tk.LEFT, padx=5)
        
        counter_label = ttk.Label(nav_frame, text="")
        counter_label.pack(side=tk.LEFT, padx=10)
        
        next_btn = ttk.Button(nav_frame, text="Next →", command=next_image)
        next_btn.pack(side=tk.LEFT, padx=5)
        
        def zoom_in():
            zoom_level[0] = min(4.0, zoom_level[0] * 1.25)
            self.screenshot_zoom_level = zoom_level[0]
            update_image()

        def zoom_out():
            zoom_level[0] = max(0.1, zoom_level[0] / 1.25)
            self.screenshot_zoom_level = zoom_level[0]
            update_image()

        def zoom_reset():
            zoom_level[0] = 1.0
            self.screenshot_zoom_level = zoom_level[0]
            update_image()

        ttk.Button(nav_frame, text="Zoom +", command=zoom_in).pack(side=tk.LEFT, padx=5)
        ttk.Button(nav_frame, text="Zoom -", command=zoom_out).pack(side=tk.LEFT, padx=5)
        ttk.Button(nav_frame, text="Reset", command=zoom_reset).pack(side=tk.LEFT, padx=5)

        remove_btn = ttk.Button(nav_frame, text="Remove This", command=remove_current)
        remove_btn.pack(side=tk.LEFT, padx=5)
        
        # Split layout: image on left with scrollbars, notes on right
        content_frame = ttk.Frame(ss_window)
        content_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        content_frame.columnconfigure(0, weight=3)
        content_frame.columnconfigure(1, weight=2)
        content_frame.rowconfigure(0, weight=1)

        # Image canvas with scrollbars
        img_canvas = tk.Canvas(content_frame, background="#2b2b2b", highlightthickness=0)
        img_canvas.grid(row=0, column=0, sticky="nsew")
        v_scroll = ttk.Scrollbar(content_frame, orient="vertical", command=img_canvas.yview)
        h_scroll = ttk.Scrollbar(content_frame, orient="horizontal", command=img_canvas.xview)
        img_canvas.configure(yscrollcommand=v_scroll.set, xscrollcommand=h_scroll.set)
        v_scroll.grid(row=0, column=2, sticky="ns")
        h_scroll.grid(row=1, column=0, sticky="ew")

        # Right-side notes panel
        notes_panel = ttk.Frame(content_frame)
        notes_panel.grid(row=0, column=1, sticky="nsew", padx=(10, 0))
        notes_panel.columnconfigure(0, weight=1)
        notes_panel.rowconfigure(1, weight=1)

        # Label frame
        label_frame = ttk.LabelFrame(notes_panel, text="Screenshot Label")
        label_frame.grid(row=0, column=0, sticky="ew", pady=(0, 8))
        label_frame.columnconfigure(0, weight=1)
        label_text = tk.Text(label_frame, height=2, wrap=tk.WORD)
        label_text.grid(row=0, column=0, sticky="ew", padx=5, pady=5)

        # Notes frame (continuous trade notes)
        notes_frame = ttk.LabelFrame(notes_panel, text="Trade Notes")
        notes_frame.grid(row=1, column=0, sticky="nsew")
        notes_frame.columnconfigure(0, weight=1)
        notes_frame.rowconfigure(0, weight=1)
        notes_text = tk.Text(notes_frame, wrap=tk.WORD)
        notes_text.grid(row=0, column=0, sticky="nsew", padx=5, pady=5)
        
        # Display first image
        update_image()

        # Refresh image sizing after layout
        ss_window.after(50, update_image)
    
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
                'exit_start_date': self.exit_start_date_var.get(),
                'exit_end_date': self.exit_end_date_var.get(),
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
        dup_win.bind("<Escape>", lambda e: dup_win.destroy())
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

    def show_import_results(self, added: List[Transaction], duplicates: List[Transaction]) -> None:
        """Display transactions added in the latest import along with any duplicates."""
        if not added and not duplicates:
            return
        win = tk.Toplevel(self.root)
        win.title("Import Results")
        win.geometry("750x350")
        win.bind("<Escape>", lambda e: win.destroy())
        cols = ("status", "run_date", "account_number", "symbol", "quantity", "price", "amount")
        tree = ttk.Treeview(win, columns=cols, show="headings")
        headers = {
            "status": "Status",
            "run_date": "Run Date",
            "account_number": "Account Number",
            "symbol": "Symbol",
            "quantity": "Quantity",
            "price": "Price",
            "amount": "Amount",
        }
        widths = {
            "status": 80,
            "run_date": 140,
            "account_number": 120,
            "symbol": 90,
            "quantity": 90,
            "price": 80,
            "amount": 90,
        }
        for col in cols:
            tree.heading(col, text=headers[col])
            tree.column(col, width=widths.get(col, 100), anchor=tk.CENTER if col == "status" else tk.W)
        def insert_items(items: List[Transaction], status_label: str) -> None:
            for idx, tx in enumerate(items):
                run_date_str = tx.run_date.strftime("%Y-%m-%d %H:%M") if isinstance(tx.run_date, dt.datetime) else str(tx.run_date)
                tree.insert("", "end", iid=f"{status_label}_{idx}", values=(
                    status_label,
                    run_date_str,
                    tx.account_number,
                    tx.symbol,
                    f"{tx.quantity:.2f}",
                    f"{tx.price:.2f}",
                    f"{tx.amount:.2f}",
                ))
        insert_items(added, "Added")
        insert_items(duplicates, "Duplicate")
        tree.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        vsb = ttk.Scrollbar(win, orient="vertical", command=tree.yview)
        tree.configure(yscrollcommand=vsb.set)
        vsb.pack(side=tk.RIGHT, fill=tk.Y)
        ttk.Button(win, text="Close", command=win.destroy).pack(pady=(0, 6))

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
        dialog.bind("<Escape>", lambda e: dialog.destroy())
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

        ttk.Label(dialog, text="Date (preferred M/D/YY):").grid(row=5, column=0, sticky="e", padx=5, pady=5)
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
            # Parse date/time using accepted formats (M/D/YY, M/D/YYYY, YYYY-MM-DD, YYYY/MM/DD)
            try:
                date_obj = self._parse_date_input(date_str, label="Date")
            except ValueError:
                return
            if time_str:
                try:
                    time_obj = dt.datetime.strptime(time_str, "%H:%M").time()
                except ValueError:
                    messagebox.showwarning("Invalid Time", "Time must be HH:MM in 24-hour format.")
                    return
            else:
                time_obj = dt.time(0, 0)
            run_dt = dt.datetime.combine(date_obj, time_obj)
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
            # Compute duplicate keys (datetime and date-only) including action
            key_dt = (tx.run_date, tx.account_number, tx.symbol, tx.quantity, tx.price, tx.amount, tx.action)
            key_date = (tx.run_date.date(), tx.account_number, tx.symbol, tx.quantity, tx.price, tx.amount, tx.action)
            # Check if duplicate across sessions (using model.seen_tx_keys). If so, ignore.
            if key_dt in self.model.seen_tx_keys or key_date in self.model.seen_tx_keys:
                messagebox.showinfo("Duplicate", "This transaction already exists in the journal and will be ignored.")
                dialog.destroy()
                return
            # Otherwise, add to model
            self.model.transactions.append(tx)
            # Record this key so future imports consider it existing
            self.model.seen_tx_keys.add(key_dt)
            self.model.seen_tx_keys.add(key_date)
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
                    'symbol_filter': self.symbol_filter_var.get(),
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

    def edit_selected_transaction(self) -> None:
        """Edit the underlying transaction(s) for the selected trade row."""
        selected = self.tree.selection()
        if not selected:
            messagebox.showinfo("No Selection", "Select a single trade row to edit.")
            return
        if len(selected) > 1:
            messagebox.showinfo("Multiple Selections", "Please edit one trade at a time.")
            return
        item_id = selected[0]
        if item_id.startswith("g"):
            messagebox.showinfo("Group Selected", "Expand the group and select a specific trade to edit.")
            return
        try:
            trade_index = int(item_id)
        except ValueError:
            messagebox.showinfo("Invalid Selection", "Could not resolve the selected trade.")
            return
        if trade_index < 0 or trade_index >= len(self.model.trades):
            messagebox.showinfo("Invalid Selection", "Could not resolve the selected trade.")
            return
        trade = self.model.trades[trade_index]
        def looks_like_futures_symbol(sym: str) -> bool:
            """Lightweight heuristic: futures tickers usually mix letters and digits."""
            sym = (sym or "").upper()
            has_alpha = any(c.isalpha() for c in sym)
            has_digit = any(c.isdigit() for c in sym)
            return has_alpha and has_digit

        # Default to treating unmatched sells as missing buys; only assume short for futures-like symbols
        entry_should_be_buy = True
        if trade.buy_id < 0 and trade.exit_date is None and looks_like_futures_symbol(trade.symbol):
            entry_should_be_buy = False

        def find_tx(target_dt: dt.datetime, price: float, want_buy: bool) -> Optional[Transaction]:
            """Find a matching transaction for the trade.

            Matching logic (best-first):
            1) Exact datetime match (to the second) with price within tolerance.
            2) Same calendar date (time-insensitive) with price within tolerance.
            Always requires account, symbol, and sufficient quantity.
            """
            date_only = target_dt.date()
            price_eps = max(0.01, abs(price) * 1e-3)  # allow small broker rounding
            best: Optional[Tuple[int, float, Transaction]] = None  # (priority, price_delta, tx)
            for tx in self.model.transactions:
                if bool(tx.is_buy) != want_buy:
                    continue
                if tx.account_number != trade.account_number or tx.symbol != trade.symbol:
                    continue
                if abs(tx.quantity) + 1e-8 < abs(trade.quantity):
                    continue
                price_delta = abs(tx.price - price)
                if price_delta > price_eps:
                    continue

                priority = 0 if abs((tx.run_date - target_dt).total_seconds()) <= 1e-6 else 1 if tx.run_date.date() == date_only else 2
                if priority == 2:
                    continue
                cand = (priority, price_delta, tx)
                if best is None or cand < best:
                    best = cand
            return best[2] if best else None

        entry_tx = find_tx(trade.entry_date, trade.entry_price, entry_should_be_buy)
        exit_tx = None
        if trade.exit_date and trade.exit_price is not None:
            exit_tx = find_tx(trade.exit_date, trade.exit_price, not entry_should_be_buy)

        entry_placeholder = entry_tx is None
        exit_placeholder = trade.exit_date is not None and trade.exit_price is not None and exit_tx is None

        def _guess_amount(is_buy: bool, price_val: float, qty_abs: float) -> float:
            base = abs(price_val * qty_abs)
            return -base if is_buy else base

        missing_parts = []
        if entry_placeholder:
            entry_qty_signed = abs(trade.quantity) if entry_should_be_buy else -abs(trade.quantity)
            entry_amount_guess = _guess_amount(entry_should_be_buy, trade.entry_price, abs(trade.quantity))
            entry_tx = Transaction(
                run_date=trade.entry_date,
                account=trade.account or trade.account_number,
                account_number=trade.account_number,
                symbol=trade.symbol,
                action="Entry" if entry_should_be_buy else "Sell to Open",
                price=trade.entry_price,
                quantity=entry_qty_signed,
                amount=entry_amount_guess,
                settlement_date=None,
            )
            missing_parts.append("entry")
        if exit_placeholder:
            exit_qty_signed = -abs(trade.quantity) if entry_should_be_buy else abs(trade.quantity)
            exit_amount_guess = _guess_amount(not entry_should_be_buy, trade.exit_price, abs(trade.quantity))  # type: ignore
            exit_tx = Transaction(
                run_date=trade.exit_date,  # type: ignore
                account=trade.account or trade.account_number,
                account_number=trade.account_number,
                symbol=trade.symbol,
                action="Exit" if entry_should_be_buy else "Buy to Cover",
                price=trade.exit_price,  # type: ignore
                quantity=exit_qty_signed,
                amount=exit_amount_guess,
                settlement_date=None,
            )
            missing_parts.append("exit")

        if missing_parts:
            parts_label = " and ".join(missing_parts)
            messagebox.showwarning(
                "Not Found",
                f"Could not locate the {parts_label} transaction for this trade. The form is prefilled from the trade data; saving will create/update the missing side(s).",
            )

        dialog = tk.Toplevel(self.root)
        dialog.title("Edit Transaction")
        dialog.resizable(False, False)
        dialog.bind("<Escape>", lambda e: dialog.destroy())

        ttk.Label(dialog, text="Entry (Buy)", font=("TkDefaultFont", 10, "bold")).grid(row=0, column=0, columnspan=3, padx=5, pady=(8, 2), sticky="w")
        entry_acct_var = tk.StringVar(value=entry_tx.account_number)
        entry_symbol_var = tk.StringVar(value=entry_tx.symbol)
        entry_action_var = tk.StringVar(value=entry_tx.action or "")
        entry_qty_var = tk.StringVar(value=f"{abs(entry_tx.quantity):.6g}")
        entry_price_var = tk.StringVar(value=f"{entry_tx.price:.6g}")
        entry_date_var = tk.StringVar(value=entry_tx.run_date.strftime("%Y-%m-%d"))
        entry_time_str = entry_tx.run_date.strftime("%H:%M") if entry_tx.run_date.time() != dt.time(0, 0) else ""
        entry_time_var = tk.StringVar(value=entry_time_str)
        trade_key = self.model.compute_key(trade)

        ttk.Label(dialog, text="Account Number:").grid(row=1, column=0, sticky="e", padx=5, pady=2)
        ttk.Entry(dialog, textvariable=entry_acct_var).grid(row=1, column=1, padx=5, pady=2)
        ttk.Label(dialog, text="Symbol:").grid(row=2, column=0, sticky="e", padx=5, pady=2)
        ttk.Entry(dialog, textvariable=entry_symbol_var).grid(row=2, column=1, padx=5, pady=2)
        ttk.Label(dialog, text="Action Text:").grid(row=3, column=0, sticky="e", padx=5, pady=2)
        ttk.Entry(dialog, textvariable=entry_action_var).grid(row=3, column=1, padx=5, pady=2)
        ttk.Label(dialog, text="Quantity:").grid(row=4, column=0, sticky="e", padx=5, pady=2)
        ttk.Entry(dialog, textvariable=entry_qty_var).grid(row=4, column=1, padx=5, pady=2)
        ttk.Label(dialog, text="Price:").grid(row=5, column=0, sticky="e", padx=5, pady=2)
        ttk.Entry(dialog, textvariable=entry_price_var).grid(row=5, column=1, padx=5, pady=2)
        ttk.Label(dialog, text="Date (M/D/YYYY ok):").grid(row=6, column=0, sticky="e", padx=5, pady=2)
        ttk.Entry(dialog, textvariable=entry_date_var).grid(row=6, column=1, padx=5, pady=2)
        ttk.Label(dialog, text="Time (HH:MM 24h):").grid(row=7, column=0, sticky="e", padx=5, pady=2)
        ttk.Entry(dialog, textvariable=entry_time_var).grid(row=7, column=1, padx=5, pady=2)

        # Entry/Exit strategies and trade note
        entry_strategy_var = tk.StringVar(value=self.model.entry_strategies.get(trade_key, ""))
        ttk.Label(dialog, text="Entry Strategy:").grid(row=8, column=0, sticky="e", padx=5, pady=(10, 2))
        ttk.Entry(dialog, textvariable=entry_strategy_var).grid(row=8, column=1, padx=5, pady=(10, 2))

        exit_strategy_var = tk.StringVar(value=self.model.exit_strategies.get(trade_key, ""))
        ttk.Label(dialog, text="Exit Strategy:").grid(row=9, column=0, sticky="e", padx=5, pady=2)
        ttk.Entry(dialog, textvariable=exit_strategy_var).grid(row=9, column=1, padx=5, pady=2)

        ttk.Label(dialog, text="Trade Note:").grid(row=10, column=0, sticky="ne", padx=5, pady=(10, 2))
        note_text = tk.Text(dialog, width=40, height=6, wrap="word")
        note_text.grid(row=10, column=1, columnspan=2, padx=5, pady=(10, 2), sticky="we")
        existing_note = self.model.notes.get(trade_key, "")
        note_text.insert("1.0", existing_note)

        exit_section_row = 11
        if exit_tx:
            ttk.Label(dialog, text="Exit (Sell)", font=("TkDefaultFont", 10, "bold")).grid(row=exit_section_row, column=0, columnspan=3, padx=5, pady=(10, 2), sticky="w")
            exit_acct_var = tk.StringVar(value=exit_tx.account_number)
            exit_symbol_var = tk.StringVar(value=exit_tx.symbol)
            exit_action_var = tk.StringVar(value=exit_tx.action or "")
            exit_qty_var = tk.StringVar(value=f"{abs(exit_tx.quantity):.6g}")
            exit_price_var = tk.StringVar(value=f"{exit_tx.price:.6g}")
            exit_date_var = tk.StringVar(value=exit_tx.run_date.strftime("%Y-%m-%d"))
            exit_time_str = exit_tx.run_date.strftime("%H:%M") if exit_tx.run_date.time() != dt.time(0, 0) else ""
            exit_time_var = tk.StringVar(value=exit_time_str)

            ttk.Label(dialog, text="Account Number:").grid(row=exit_section_row + 1, column=0, sticky="e", padx=5, pady=2)
            ttk.Entry(dialog, textvariable=exit_acct_var).grid(row=exit_section_row + 1, column=1, padx=5, pady=2)
            ttk.Label(dialog, text="Symbol:").grid(row=exit_section_row + 2, column=0, sticky="e", padx=5, pady=2)
            ttk.Entry(dialog, textvariable=exit_symbol_var).grid(row=exit_section_row + 2, column=1, padx=5, pady=2)
            ttk.Label(dialog, text="Action Text:").grid(row=exit_section_row + 3, column=0, sticky="e", padx=5, pady=2)
            ttk.Entry(dialog, textvariable=exit_action_var).grid(row=exit_section_row + 3, column=1, padx=5, pady=2)
            ttk.Label(dialog, text="Quantity:").grid(row=exit_section_row + 4, column=0, sticky="e", padx=5, pady=2)
            ttk.Entry(dialog, textvariable=exit_qty_var).grid(row=exit_section_row + 4, column=1, padx=5, pady=2)
            ttk.Label(dialog, text="Price:").grid(row=exit_section_row + 5, column=0, sticky="e", padx=5, pady=2)
            ttk.Entry(dialog, textvariable=exit_price_var).grid(row=exit_section_row + 5, column=1, padx=5, pady=2)
            ttk.Label(dialog, text="Date (M/D/YYYY ok):").grid(row=exit_section_row + 6, column=0, sticky="e", padx=5, pady=2)
            ttk.Entry(dialog, textvariable=exit_date_var).grid(row=exit_section_row + 6, column=1, padx=5, pady=2)
            ttk.Label(dialog, text="Time (HH:MM 24h):").grid(row=exit_section_row + 7, column=0, sticky="e", padx=5, pady=2)
            ttk.Entry(dialog, textvariable=exit_time_var).grid(row=exit_section_row + 7, column=1, padx=5, pady=2)

        def parse_dt(date_text: str, time_text: str, label: str) -> dt.datetime:
            try:
                date_obj = self._parse_date_input(date_text, label=label)
            except ValueError:
                raise
            if date_obj is None:
                raise ValueError(f"{label} is required")
            time_text = (time_text or "").strip()
            if time_text:
                try:
                    time_obj = dt.datetime.strptime(time_text, "%H:%M").time()
                except ValueError:
                    messagebox.showwarning("Invalid Time", f"{label} time must be HH:MM in 24-hour format.")
                    raise
            else:
                time_obj = dt.time()
            return dt.datetime.combine(date_obj, time_obj)

        def signed_amount(price_val: float, qty_val: float, old_amount: float) -> float:
            base = abs(price_val * qty_val)
            if old_amount < 0:
                return -base
            if old_amount > 0:
                return base
            return price_val * qty_val

        def apply_changes() -> None:
            try:
                new_entry_dt = parse_dt(entry_date_var.get(), entry_time_var.get(), "Entry date")
                new_entry_qty_abs = float(entry_qty_var.get())
                if new_entry_qty_abs <= 0:
                    raise ValueError("Entry quantity must be positive")
                new_entry_price = float(entry_price_var.get())
            except ValueError as e:
                if "Entry" in str(e):
                    messagebox.showwarning("Invalid Entry", str(e))
                else:
                    messagebox.showwarning("Invalid Entry", "Entry fields must be valid numbers and dates.")
                return

            entry_action = (entry_action_var.get() or entry_tx.action or "Entry").strip()
            entry_qty = new_entry_qty_abs if entry_tx.is_buy else -new_entry_qty_abs
            entry_amount = signed_amount(new_entry_price, entry_qty, entry_tx.amount)

            exit_action = None
            exit_qty = None
            exit_price = None
            new_exit_dt = None
            if exit_tx:
                try:
                    new_exit_dt = parse_dt(exit_date_var.get(), exit_time_var.get(), "Exit date")
                    new_exit_qty_abs = float(exit_qty_var.get())
                    if new_exit_qty_abs <= 0:
                        raise ValueError("Exit quantity must be positive")
                    exit_price = float(exit_price_var.get())
                except ValueError as e:
                    if "Exit" in str(e):
                        messagebox.showwarning("Invalid Exit", str(e))
                    else:
                        messagebox.showwarning("Invalid Exit", "Exit fields must be valid numbers and dates.")
                    return
                exit_action = (exit_action_var.get() or exit_tx.action or "Exit").strip()
                exit_qty = new_exit_qty_abs if exit_tx.is_buy else -new_exit_qty_abs
                exit_amount = signed_amount(exit_price, exit_qty, exit_tx.amount)
            else:
                exit_amount = None

            old_entry_keys: Set[tuple] = set()
            if not entry_placeholder:
                old_entry_keys = {
                    (entry_tx.run_date, entry_tx.account_number, entry_tx.symbol, entry_tx.quantity, entry_tx.price, entry_tx.amount, entry_tx.action),
                    (entry_tx.run_date.date(), entry_tx.account_number, entry_tx.symbol, entry_tx.quantity, entry_tx.price, entry_tx.amount, entry_tx.action),
                }
            old_exit_keys: Set[tuple] = set()
            if exit_tx and not exit_placeholder:
                old_exit_keys = {
                    (exit_tx.run_date, exit_tx.account_number, exit_tx.symbol, exit_tx.quantity, exit_tx.price, exit_tx.amount, exit_tx.action),
                    (exit_tx.run_date.date(), exit_tx.account_number, exit_tx.symbol, exit_tx.quantity, exit_tx.price, exit_tx.amount, exit_tx.action),
                }

            new_entry_keys = {
                (new_entry_dt, entry_acct_var.get().strip(), entry_symbol_var.get().strip().upper(), entry_qty, new_entry_price, entry_amount, entry_action),
                (new_entry_dt.date(), entry_acct_var.get().strip(), entry_symbol_var.get().strip().upper(), entry_qty, new_entry_price, entry_amount, entry_action),
            }
            new_exit_keys: Set[tuple] = set()
            if exit_tx and exit_qty is not None and exit_price is not None and new_exit_dt is not None and exit_amount is not None:
                new_exit_keys = {
                    (new_exit_dt, exit_acct_var.get().strip(), exit_symbol_var.get().strip().upper(), exit_qty, exit_price, exit_amount, exit_action),
                    (new_exit_dt.date(), exit_acct_var.get().strip(), exit_symbol_var.get().strip().upper(), exit_qty, exit_price, exit_amount, exit_action),
                }

            existing_keys = set(self.model.seen_tx_keys)
            existing_keys.difference_update(old_entry_keys)
            existing_keys.difference_update(old_exit_keys)
            if any(k in existing_keys for k in new_entry_keys):
                messagebox.showwarning("Duplicate", "The edited entry would duplicate an existing transaction.")
                return
            if new_exit_keys and any(k in existing_keys for k in new_exit_keys):
                messagebox.showwarning("Duplicate", "The edited exit would duplicate an existing transaction.")
                return

            # Apply edits to transactions
            entry_tx.run_date = new_entry_dt
            entry_tx.account = entry_acct_var.get().strip()
            entry_tx.account_number = entry_acct_var.get().strip()
            entry_tx.symbol = entry_symbol_var.get().strip().upper()
            entry_tx.action = entry_action
            entry_tx.price = new_entry_price
            entry_tx.quantity = entry_qty
            entry_tx.amount = entry_amount

            if exit_tx and new_exit_keys:
                exit_tx.run_date = new_exit_dt  # type: ignore
                exit_tx.account = exit_acct_var.get().strip()
                exit_tx.account_number = exit_acct_var.get().strip()
                exit_tx.symbol = exit_symbol_var.get().strip().upper()
                exit_tx.action = exit_action
                exit_tx.price = exit_price  # type: ignore
                exit_tx.quantity = exit_qty  # type: ignore
                exit_tx.amount = exit_amount  # type: ignore

            # If we synthesized missing transactions, add them to the model now
            if entry_placeholder and entry_tx not in self.model.transactions:
                self.model.transactions.append(entry_tx)
            if exit_placeholder and exit_tx and exit_tx not in self.model.transactions:
                self.model.transactions.append(exit_tx)

            # Update seen transaction keys (remove old, add new)
            self.model.seen_tx_keys.difference_update(old_entry_keys)
            self.model.seen_tx_keys.difference_update(old_exit_keys)
            self.model.seen_tx_keys.update(new_entry_keys)
            self.model.seen_tx_keys.update(new_exit_keys)

            # Preserve metadata, rematch, and restore metadata
            metadata_map = self.model._save_trade_metadata_before_matching()
            self.model.reset_matching()
            self.model._match_trades()
            self.model._restore_trade_metadata_after_matching(metadata_map)

            # Update strategies and notes on the rematched trade key
            updated_key = None
            for t in self.model.trades:
                if (
                    t.account_number == entry_acct_var.get().strip()
                    and t.symbol == entry_symbol_var.get().strip().upper()
                    and t.entry_date == new_entry_dt
                    and abs(t.entry_price - new_entry_price) < 1e-6
                    and abs(t.quantity - entry_qty) < 1e-6
                ):
                    updated_key = self.model.compute_key(t)
                    break
            if updated_key is None:
                updated_key = trade_key

            entry_strategy_val = entry_strategy_var.get().strip()
            exit_strategy_val = exit_strategy_var.get().strip()
            note_val = note_text.get("1.0", tk.END).strip()
            if entry_strategy_val:
                self.model.entry_strategies[updated_key] = entry_strategy_val
            else:
                self.model.entry_strategies.pop(updated_key, None)
            if exit_strategy_val:
                self.model.exit_strategies[updated_key] = exit_strategy_val
            else:
                self.model.exit_strategies.pop(updated_key, None)
            if note_val:
                self.model.notes[updated_key] = note_val
            else:
                self.model.notes.pop(updated_key, None)

            # Refresh UI and persisted state
            acct_numbers = sorted({tx.account_number for tx in self.model.transactions})
            self.account_dropdown["values"] = ["all"] + acct_numbers
            if self.account_var.get() not in ["all"] + acct_numbers:
                self.account_var.set("all")
            self.populate_table()
            self.update_summary_and_chart()
            self.update_chart_symbols()
            self.model.save_state(self.persist_path, filter_state=self._current_filter_state())
            dialog.destroy()

        button_row = exit_section_row + (8 if exit_tx else 0)
        ttk.Button(dialog, text="Save Changes", command=apply_changes).grid(row=button_row, column=0, padx=5, pady=(12, 8), sticky="e")
        ttk.Button(dialog, text="Cancel", command=dialog.destroy).grid(row=button_row, column=1, padx=5, pady=(12, 8), sticky="w")

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
        # Build a reliable mapping from trade key -> underlying transaction indices
        trade_key_to_tx_indices: Dict[tuple, set] = {}
        # Use a sorted copy of transactions (with original indices) to replay matching
        sorted_txs = sorted(list(enumerate(self.model.transactions)), key=lambda item: item[1].run_date)
        open_positions: Dict[Tuple[str, str], List[Dict[str, object]]] = {}
        next_buy_id = 1

        for tx_index, tx in sorted_txs:
            key = (tx.account_number, tx.symbol)
            if tx.is_buy:
                buy_id = next_buy_id
                next_buy_id += 1
                if key not in open_positions:
                    open_positions[key] = []
                open_positions[key].append({
                    "qty": tx.quantity,
                    "price": tx.price,
                    "date": tx.run_date,
                    "id": buy_id,
                    "tx_index": tx_index,
                })
            elif tx.is_sell:
                remaining = abs(tx.quantity)
                if key not in open_positions or not open_positions[key]:
                    # Unmatched sell
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
                        status="OPEN",
                    )
                    tkey = self.model.compute_key(trade)
                    trade_key_to_tx_indices.setdefault(tkey, set()).add(tx_index)
                    continue
                while remaining > 1e-8:
                    if not open_positions[key]:
                        # No buys left; unmatched sell portion
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
                            status="OPEN",
                        )
                        tkey = self.model.compute_key(trade)
                        trade_key_to_tx_indices.setdefault(tkey, set()).add(tx_index)
                        break
                    buy = open_positions[key][0]
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
                        status="CLOSED",
                    )
                    tkey = self.model.compute_key(trade)
                    trade_key_to_tx_indices.setdefault(tkey, set()).update({buy["tx_index"], tx_index})
                    remaining -= matched_qty
                    buy["qty"] -= matched_qty
                    if buy["qty"] <= 1e-8:
                        open_positions[key].pop(0)

        # Record remaining open buys as open trades
        for key, buys in open_positions.items():
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
                    status="OPEN",
                )
                tkey = self.model.compute_key(trade)
                trade_key_to_tx_indices.setdefault(tkey, set()).add(buy["tx_index"])

        # Collect transaction indices to delete for selected trades
        tx_indices_to_remove: set = set()
        for key in unique_keys:
            tx_indices_to_remove.update(trade_key_to_tx_indices.get(key, set()))

        # Remove transactions by index
        new_transactions: List[Transaction] = []
        for idx, tx in enumerate(self.model.transactions):
            if idx not in tx_indices_to_remove:
                new_transactions.append(tx)
        self.model.transactions = new_transactions

        # Rebuild seen_tx_keys from remaining transactions
        rebuilt_seen: set = set()
        for tx in self.model.transactions:
            key_dt = (tx.run_date, tx.account_number, tx.symbol, tx.quantity, tx.price, tx.amount, tx.action)
            key_date = (tx.run_date.date(), tx.account_number, tx.symbol, tx.quantity, tx.price, tx.amount, tx.action)
            rebuilt_seen.add(key_dt)
            rebuilt_seen.add(key_date)
        self.model.seen_tx_keys = rebuilt_seen
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
                'symbol_filter': self.symbol_filter_var.get(),
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
        symbol_filter_tokens = self._parsed_symbol_filter()
        for idx, trade in enumerate(self.model.trades):
            # Only consider trades with an exit date (closed) and a P&L value
            if not trade.is_closed or trade.pnl is None:
                continue
            # Account filter
            if account_filter and account_filter != "all" and trade.account_number != account_filter:
                continue
            # Symbol filter
            if symbol_filter_tokens and trade.symbol.upper() not in symbol_filter_tokens:
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
                    self.exit_start_date_var.set(filter_state.get('exit_start_date', ''))
                    self.exit_end_date_var.set(filter_state.get('exit_end_date', ''))
                    self.symbol_filter_var.set(filter_state.get('symbol_filter', ''))
                    self.entry_strategy_filter_var.set(filter_state.get('entry_strategy_filter', 'all'))
                    self.exit_strategy_filter_var.set(filter_state.get('exit_strategy_filter', 'all'))
                    if isinstance(filter_state.get('analysis2_starting_balances'), dict):
                        self.analysis2_starting_balances = filter_state.get('analysis2_starting_balances', {})
                    if filter_state.get('analysis2_year'):
                        self.analysis2_year_var.set(str(filter_state.get('analysis2_year')))
                    if 'chart_visible' in filter_state:
                        self.chart_visible.set(bool(filter_state.get('chart_visible')))
                    # Apply date filter if dates were set
                    if filter_state.get('start_date') or filter_state.get('end_date') or filter_state.get('exit_start_date') or filter_state.get('exit_end_date'):
                        self.apply_date_filter()
                else:
                    self.account_dropdown.set("all")
                # Populate table and summary
                self.populate_table()
                self.update_summary_and_chart()
                # Apply chart visibility after layout
                if not self.chart_visible.get():
                    try:
                        self.left_paned.remove(self.chart_frame)
                        self.toggle_chart_btn.config(text="Show Chart")
                    except Exception:
                        pass
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
                    'exit_start_date': self.exit_start_date_var.get(),
                    'exit_end_date': self.exit_end_date_var.get(),
                'symbol_filter': self.symbol_filter_var.get(),
                'entry_strategy_filter': self.entry_strategy_filter_var.get(),
                'exit_strategy_filter': self.exit_strategy_filter_var.get(),
                'chart_symbol': self.chart_symbol_var.get(),
                'chart_visible': self.chart_visible.get(),
                'analysis2_starting_balances': self.analysis2_starting_balances,
                'analysis2_year': self.analysis2_year_var.get(),
            }
            self.model.save_state(self.persist_path, filter_state)
        except Exception:
            pass
        self.root.destroy()

    def apply_date_filter(self) -> None:
        """Parse date filter inputs and refresh the table and summary."""
        start_str = self.start_date_var.get().strip()
        end_str = self.end_date_var.get().strip()
        exit_start_str = self.exit_start_date_var.get().strip()
        exit_end_str = self.exit_end_date_var.get().strip()
        self.start_date = None
        self.end_date = None
        self.exit_start_date = None
        self.exit_end_date = None
        try:
            self.start_date = self._parse_date_input(start_str, label="Start date")
            self.end_date = self._parse_date_input(end_str, label="End date")
            self.exit_start_date = self._parse_date_input(exit_start_str, label="Exit start date")
            self.exit_end_date = self._parse_date_input(exit_end_str, label="Exit end date")
        except ValueError:
            return
        # If both dates provided, ensure start <= end
        if self.start_date and self.end_date and self.start_date > self.end_date:
            messagebox.showwarning("Invalid Range", "Start date cannot be after end date.")
            return
        if self.exit_start_date and self.exit_end_date and self.exit_start_date > self.exit_end_date:
            messagebox.showwarning("Invalid Range", "Exit start date cannot be after exit end date.")
            return
        # Normalize displayed text to preferred M/D/YYYY format
        if self.start_date:
            self.start_date_var.set(self._format_date_preferred(self.start_date))
        if self.end_date:
            self.end_date_var.set(self._format_date_preferred(self.end_date))
        if self.exit_start_date:
            self.exit_start_date_var.set(self._format_date_preferred(self.exit_start_date))
        if self.exit_end_date:
            self.exit_end_date_var.set(self._format_date_preferred(self.exit_end_date))
        # Refresh table and summary/chart
        self.populate_table()
        self.update_summary_and_chart()

    def clear_entry_date_filter(self) -> None:
        """Clear entry date range fields and refresh filters."""
        self.start_date_var.set("")
        self.end_date_var.set("")
        self.start_date = None
        self.end_date = None
        self.apply_date_filter()

    def clear_exit_date_filter(self) -> None:
        """Clear exit date range fields and refresh filters."""
        self.exit_start_date_var.set("")
        self.exit_end_date_var.set("")
        self.exit_start_date = None
        self.exit_end_date = None
        self.apply_date_filter()

    def _parse_date_input(self, value: str, *, label: Optional[str] = None) -> Optional[dt.date]:
        """Parse a date string using multiple accepted formats.

        Accepted formats: YYYY-MM-DD, YYYY/MM/DD, M/D/YYYY, M/D/YY. Raises ValueError if
        parsing fails and a label is provided (to show a warning).
        """
        value = value.strip()
        if not value:
            return None
        for fmt in self.accepted_date_formats:
            try:
                return dt.datetime.strptime(value, fmt).date()
            except ValueError:
                continue
        if label:
            messagebox.showwarning(
                "Invalid Date",
                f"{label} '{value}' is not in an accepted format. Try YYYY-MM-DD, YYYY/MM/DD, M/D/YYYY, or M/D/YY.",
            )
            raise ValueError(f"Invalid date for {label}")
        return None

    @staticmethod
    def _format_date_preferred(date_obj: dt.date) -> str:
        """Return date as M/D/YYYY (no zero padding) for display consistency."""
        return f"{date_obj.month}/{date_obj.day}/{date_obj.year}"

    def _parsed_symbol_filter(self) -> Set[str]:
        """Return uppercase symbol tokens from the symbol filter entry."""
        raw = self.symbol_filter_var.get() or ""
        tokens = [tok.strip().upper() for tok in re.split(r"[,\s]+", raw) if tok.strip()]
        return set(tokens)

    def apply_symbol_filter(self) -> None:
        """Apply symbol filter and refresh table/summary."""
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
        self.exit_start_date_var.set("")
        self.exit_end_date_var.set("")
        self.exit_start_date = None
        self.exit_end_date = None
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
        # Reset symbol filter
        self.symbol_filter_var.set("")
        # Refresh table and summary/chart
        self.populate_table()
        self.update_summary_and_chart()

    def open_date_picker(self, date_var: tk.StringVar, source_widgets: Optional[List[tk.Widget]] = None) -> None:
        """Open a simple date picker dialog to select a date and set it to the provided StringVar.

        The date picker defaults to the current month and year. When the user selects a day,
        the date is formatted as YYYY-MM-DD and assigned to ``date_var``. This function
        creates a modal top-level window with navigation to previous/next months.
        """
        # Close any existing picker to avoid stacking multiple calendars
        if self._date_picker_window is not None and self._date_picker_window.winfo_exists():
            try:
                self._date_picker_window.destroy()
            except Exception:
                pass
            self._date_picker_window = None
        # Track widgets related to this picker so outside clicks can close it
        self._date_picker_allowed_widgets = set(source_widgets or [])
        parse_date = self._parse_date_input
        # Inner class for date picker dialog
        class DatePicker(tk.Toplevel):
            def __init__(self, parent, var: tk.StringVar):
                super().__init__(parent)
                self.title("Select Date")
                self.resizable(False, False)
                self.bind("<Escape>", lambda e: self.destroy())
                self.var = var
                # Determine initial month/year from existing value or current date
                current = parse_date(var.get()) or dt.date.today()
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
                self.var.set(TradeJournalApp._format_date_preferred(date_obj))
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
        self._date_picker_window = picker
        picker.bind("<Destroy>", self._on_date_picker_destroy)
        picker.bind("<FocusOut>", lambda e: picker.after(50, self._close_picker_if_inactive))
        # Position the picker near the mouse pointer
        self.root.update_idletasks()
        x = self.root.winfo_pointerx()
        y = self.root.winfo_pointery()
        picker.geometry(f"+{x}+{y}")

    def _on_date_picker_destroy(self, event: Optional[tk.Event] = None) -> None:
        """Clear the stored date picker reference when the window is closed."""
        if self._date_picker_window is None:
            return
        if event is None or event.widget == self._date_picker_window:
            self._date_picker_window = None
            self._date_picker_allowed_widgets.clear()

    def _close_picker_if_inactive(self) -> None:
        """Close the date picker if focus has moved outside it."""
        picker = self._date_picker_window
        if picker is None or not picker.winfo_exists():
            self._date_picker_window = None
            self._date_picker_allowed_widgets.clear()
            return
        try:
            focus_widget = picker.focus_get()
        except (KeyError, tk.TclError):
            # Focus resolution may fail if widgets are being destroyed
            picker.destroy()
            self._date_picker_window = None
            self._date_picker_allowed_widgets.clear()
            return
        # If nothing focused inside picker, close it
        if focus_widget is None:
            picker.destroy()
            self._date_picker_allowed_widgets.clear()
            return
        # Walk up parents to see if focus is within picker
        w = focus_widget
        while w is not None:
            if w == picker:
                return
            w = w.master
        picker.destroy()
        self._date_picker_allowed_widgets.clear()

    def _on_click_close_picker(self, event: tk.Event) -> None:
        """Close the date picker when clicking outside allowed widgets/picker."""
        picker = self._date_picker_window
        if picker is None or not picker.winfo_exists():
            self._date_picker_window = None
            self._date_picker_allowed_widgets.clear()
            return
        target = getattr(event, "widget", None)
        if target is None:
            return
        if self._is_descendant(target, picker):
            return
        if self._widget_is_allowed(target):
            return
        try:
            picker.destroy()
        finally:
            self._date_picker_window = None
            self._date_picker_allowed_widgets.clear()

    def _widget_is_allowed(self, widget: tk.Widget) -> bool:
        """Return True if widget is in the allowed list or a child of one."""
        for allowed in list(self._date_picker_allowed_widgets):
            if allowed is None:
                continue
            if widget == allowed or self._is_descendant(widget, allowed):
                return True
        return False

    @staticmethod
    def _is_descendant(widget: tk.Widget, ancestor: tk.Widget) -> bool:
        """Return True if widget is ancestor or descendant relationship matches."""
        w = widget
        while w is not None:
            if w == ancestor:
                return True
            w = getattr(w, "master", None)
        return False

    def on_account_filter_change(self, event: tk.Event) -> None:
        """Update summary and chart when account filter changes."""
        self.populate_table()
        self.update_summary_and_chart()

    def on_closed_filter_change(self) -> None:
        """Update table, summary and chart when the closed-only checkbox is toggled."""
        if self.closed_only_var.get():
            self.open_only_var.set(False)
        self.populate_table()
        self.update_summary_and_chart()

    def on_open_only_change(self) -> None:
        """Update filters when the open-only checkbox is toggled."""
        if self.open_only_var.get():
            self.closed_only_var.set(False)
        self.populate_table()
        self.update_summary_and_chart()

    def on_include_open_equity_change(self) -> None:
        """Refresh summary/chart when Include Open P&L toggles."""
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
        open_only = self.open_only_var.get() if hasattr(self, "open_only_var") else False
        entry_strategy_filter = self.entry_strategy_filter_var.get()
        exit_strategy_filter = self.exit_strategy_filter_var.get()
        exit_start_date = getattr(self, "exit_start_date", None)
        exit_end_date = getattr(self, "exit_end_date", None)
        symbol_filter_tokens = self._parsed_symbol_filter()
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
        has_symbol_filter = bool(symbol_filter_tokens)
        
        # Determine summary - always compute manually if strategy filters are active or top_set is present
        if top_set is None and not has_strategy_filter and not has_symbol_filter and not open_only:
            summary = self.model.compute_summary(account_filter, closed_only=closed_only,
                                                 start_date=self.start_date, end_date=self.end_date,
                                                 exit_start_date=exit_start_date, exit_end_date=exit_end_date)
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
                # Symbol filter
                if symbol_filter_tokens and trade.symbol.upper() not in symbol_filter_tokens:
                    continue
                # Closed/open filters
                if open_only and trade.is_closed:
                    continue
                if closed_only and not trade.is_closed:
                    continue
                # Account filter
                if account_filter and account_filter != "all" and trade.account_number != account_filter:
                    continue
                # Date range filter on entry_date (inclusive)
                if self.start_date and trade.entry_date.date() < self.start_date:
                    continue
                if self.end_date and trade.entry_date.date() > self.end_date:
                    continue
                # Date range filter on exit_date (inclusive)
                if exit_start_date or exit_end_date:
                    if not trade.exit_date:
                        continue
                    exit_date = trade.exit_date.date()
                    if exit_start_date and exit_date < exit_start_date:
                        continue
                    if exit_end_date and exit_date > exit_end_date:
                        continue
                # Closed-only filter now includes partial exits; only skip trades with no exit
                if closed_only and not trade.exit_date:
                    continue
                pnl = trade.pnl if trade.pnl is not None else 0.0
                pnl_pct = trade.pnl_pct if trade.pnl_pct is not None else 0.0
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
        open_pnl_current = 0.0
        missing_syms: List[str] = []
        if self.include_open_equity_var.get():
            try:
                open_pnl_current, missing_syms = self._compute_open_pnl_current(account_filter)
                if missing_syms:
                    summary_text += f"\nOpen P&L (current): N/A (missing prices for: {', '.join(sorted(missing_syms))})"
                else:
                    summary_text += f"\nOpen P&L (current): {open_pnl_current:.2f}"
            except Exception:
                summary_text += "\nOpen P&L (current): N/A"
        # Label depends on whether we're viewing open-only or closed trades
        if open_only:
            summary_text += f"\nRealized P&L (open trades): {summary['total_pnl']:.2f}"
        else:
            summary_text += f"\nClosed P&L (filtered): {summary['total_pnl']:.2f}"
        if self.include_open_equity_var.get():
            summary_text += f"\nClosed + Open (current): {summary['total_pnl'] + open_pnl_current:.2f}"
        self.summary_var.set(summary_text)
        # Compute equity curve DataFrame
        if top_set is None and not has_strategy_filter and not has_symbol_filter and not open_only:
            eq_df = self.model.equity_curve(account_filter, closed_only=closed_only,
                                             start_date=self.start_date, end_date=self.end_date,
                                             exit_start_date=exit_start_date, exit_end_date=exit_end_date)
        else:
            # Build equity curve from filtered trades
            data: Dict[dt.date, float] = {}
            for idx, trade in enumerate(self.model.trades):
                if top_set is not None and idx not in top_set:
                    continue
                # Strategy filters
                if not matches_strategy_filters(trade):
                    continue
                # Symbol filter
                if symbol_filter_tokens and trade.symbol.upper() not in symbol_filter_tokens:
                    continue
                # Open-only filter: skip closed trades (equity curve will be empty for open-only view)
                if open_only and trade.is_closed:
                    continue
                if not trade.is_closed or trade.exit_date is None or trade.pnl is None:
                    continue
                # Account filter
                if account_filter and account_filter != "all" and trade.account_number != account_filter:
                    continue
                # Date range filter on entry date (inclusive) - matches table filtering
                if self.start_date and trade.entry_date.date() < self.start_date:
                    continue
                if self.end_date and trade.entry_date.date() > self.end_date:
                    continue
                # Date range filter on exit date (inclusive)
                exit_date_dt = trade.exit_date.date()
                if exit_start_date and exit_date_dt < exit_start_date:
                    continue
                if exit_end_date and exit_date_dt > exit_end_date:
                    continue
                # Closed-only filter now includes partial exits; only skip trades with no exit
                if closed_only and not trade.exit_date:
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
        open_pnl_current_chart = 0.0
        missing_syms_chart: List[str] = []
        if self.include_open_equity_var.get():
            try:
                open_pnl_current_chart, missing_syms_chart = self._compute_open_pnl_current(account_filter)
            except Exception:
                open_pnl_current_chart = 0.0
                missing_syms_chart = []
        if not eq_df.empty:
            # Convert dates for matplotlib
            dates_dt = pd.to_datetime(eq_df["date"])
            y_values = eq_df["equity"].values
            
            # Plot line with gradient fill
            self.ax.plot(dates_dt, y_values, linewidth=2.5, color='#1f77b4', label='Cumulative P&L', zorder=3)
            self.ax.fill_between(dates_dt, y_values, alpha=0.25, color='#1f77b4', zorder=2)

            # Store values for cursor annotation (for orange marker)
            combined_val_for_cursor = None
            closed_val_for_cursor = None
            open_pnl_for_cursor = None
            scatter_artist = None

            if self.include_open_equity_var.get() and len(y_values) > 0:
                last_date = dates_dt.iloc[-1]
                closed_val = y_values[-1]
                combined = closed_val + open_pnl_current_chart
                scatter_artist = self.ax.scatter([last_date], [combined], color='#ff7f0e', s=60, zorder=4, label='Closed + Open (current)')
                self.ax.hlines(combined, dates_dt.min(), dates_dt.max(), colors='#ff7f0e', linestyles='--', linewidth=1.2, alpha=0.6)
                # Store for cursor
                combined_val_for_cursor = combined
                closed_val_for_cursor = closed_val
                open_pnl_for_cursor = open_pnl_current_chart

            # Interactive hover tooltip for date + equity value (requires mplcursors)
            try:
                import mplcursors
                import matplotlib.dates as mdates

                # Remove old cursors to avoid stacking listeners
                if getattr(self, '_eq_cursor', None):
                    try:
                        self._eq_cursor.remove()
                    except Exception:
                        pass
                if getattr(self, '_scatter_cursor', None):
                    try:
                        self._scatter_cursor.remove()
                    except Exception:
                        pass

                # Cursor for the equity line only (exclude the hlines)
                # Filter to only the main equity line (first line plotted)
                main_line = [self.ax.lines[0]] if self.ax.lines else []
                self._eq_cursor = mplcursors.cursor(main_line, hover=mplcursors.HoverMode.Transient)

                @self._eq_cursor.connect("add")
                def _show_equity_annotation(sel):
                    x_val, y_val = sel.target
                    date_str = mdates.num2date(x_val).date().isoformat()
                    sel.annotation.set_text(f"{date_str}\n$ {y_val:,.0f}")
                    sel.annotation.get_bbox_patch().set(fc="white", ec="#1f77b4", alpha=0.9)

                # Cursor for the orange scatter point (closed + open) - only on hover
                if scatter_artist is not None:
                    self._scatter_cursor = mplcursors.cursor(scatter_artist, hover=mplcursors.HoverMode.Transient)
                    
                    @self._scatter_cursor.connect("add")
                    def _show_combined_annotation(sel):
                        x_val, y_val = sel.target
                        date_str = mdates.num2date(x_val).date().isoformat()
                        # Show both closed P&L and the open P&L component
                        text_lines = [f"{date_str}", f"Closed + Open: $ {y_val:,.0f}"]
                        if closed_val_for_cursor is not None:
                            text_lines.append(f"Closed P&L: $ {closed_val_for_cursor:,.0f}")
                        if open_pnl_for_cursor is not None:
                            text_lines.append(f"Open P&L: $ {open_pnl_for_cursor:,.0f}")
                        sel.annotation.set_text("\n".join(text_lines))
                        sel.annotation.get_bbox_patch().set(fc="white", ec="#ff7f0e", alpha=0.9)
            except Exception:
                pass
            
            # Styling
            self.ax.set_xlabel('Date', fontsize=11, fontweight='bold', color='#333333')
            self.ax.set_ylabel('Cumulative P&L ($)', fontsize=11, fontweight='bold', color='#333333')
            self.ax.set_title('Equity Curve', fontsize=13, fontweight='bold', color='#333333', pad=15)

            if self.include_open_equity_var.get():
                self.ax.legend(loc='upper left')
            
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
            # No closed trades equity data - but still show open P&L if enabled
            if self.include_open_equity_var.get() and open_pnl_current_chart != 0:
                import datetime as datetime_module
                today = datetime_module.date.today()
                today_dt = pd.to_datetime(today)
                # Plot just the open P&L as a single point at today's date
                scatter_artist = self.ax.scatter([today_dt], [open_pnl_current_chart], color='#ff7f0e', s=80, zorder=4, label='Open P&L (current)')
                self.ax.hlines(open_pnl_current_chart, today_dt, today_dt, colors='#ff7f0e', linestyles='--', linewidth=1.2, alpha=0.6)
                
                # Add annotation for the open P&L point
                self.ax.annotate(f'${open_pnl_current_chart:,.0f}', 
                                xy=(today_dt, open_pnl_current_chart),
                                xytext=(10, 10), textcoords='offset points',
                                fontsize=10, color='#ff7f0e',
                                bbox=dict(boxstyle='round,pad=0.3', facecolor='white', edgecolor='#ff7f0e', alpha=0.9))
                
                self.ax.set_title('Open P&L (No Closed Trades)', fontsize=13, fontweight='bold', color='#333333', pad=15)
                self.ax.legend(loc='upper left')
                
                # Styling
                self.ax.set_xlabel('Date', fontsize=11, fontweight='bold', color='#333333')
                self.ax.set_ylabel('P&L ($)', fontsize=11, fontweight='bold', color='#333333')
                self.ax.grid(True, linestyle='-', linewidth=0.6, alpha=0.3, color='#cccccc', zorder=1)
                self.ax.set_axisbelow(True)
                for spine in self.ax.spines.values():
                    spine.set_edgecolor('#cccccc')
                    spine.set_linewidth(1)
                from matplotlib.ticker import FuncFormatter
                def dollar_formatter(x, pos):
                    return f'${x:,.0f}'
                self.ax.yaxis.set_major_formatter(FuncFormatter(dollar_formatter))
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

        # Refresh Analysis Two view to reflect current filters
        if hasattr(self, "analysis2_monthly_tree"):
            self.update_analysis_two_view()

    def _compute_open_pnl_current(self, account_filter: Optional[str], fetch_if_missing: bool = True) -> Tuple[float, List[str]]:
        """Compute unrealized P&L for open positions using latest available close.

        Returns (pnl_value, missing_symbols).
        """
        entry_strategy_filter = self.entry_strategy_filter_var.get()
        exit_strategy_filter = self.exit_strategy_filter_var.get()
        symbol_filter_tokens = self._parsed_symbol_filter()
        top_set = getattr(self, "top_filter_set", None)
        exit_start_date = getattr(self, "exit_start_date", None)
        exit_end_date = getattr(self, "exit_end_date", None)

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

        positions: Dict[str, List[Tuple[float, float, dt.date]]] = {}
        for idx, trade in enumerate(self.model.trades):
            if trade.is_closed:
                continue
            if top_set is not None and idx not in top_set:
                continue
            if not matches_strategy_filters(trade):
                continue
            if symbol_filter_tokens and trade.symbol.upper() not in symbol_filter_tokens:
                continue
            if account_filter and account_filter != "all" and trade.account_number != account_filter:
                continue
            if self.start_date and trade.entry_date.date() < self.start_date:
                continue
            if self.end_date and trade.entry_date.date() > self.end_date:
                continue
            if exit_start_date or exit_end_date:
                # Align with exit-date filtering behavior: skip opens when exit filters are set
                continue
            positions.setdefault(trade.symbol.upper(), []).append((trade.entry_price, trade.quantity, trade.entry_date.date()))

        if not positions:
            return 0.0, []

        total_pnl = 0.0
        missing_symbols: List[str] = []
        today_plus = dt.date.today() + dt.timedelta(days=1)
        for symbol, entries in positions.items():
            start_date = min(entry[2] for entry in entries)
            df = self.price_manager.get_price_data(symbol, start_date, today_plus)
            if (df is None or df.empty) and HAS_YFINANCE and fetch_if_missing:
                try:
                    fetched_df = self.price_manager.fetch_and_store(symbol, start_date, today_plus)
                    if fetched_df is not None and not fetched_df.empty:
                        df = self.price_manager.get_price_data(symbol, start_date, today_plus)
                    else:
                        df = None
                except Exception:
                    df = None
            if df is None or df.empty:
                missing_symbols.append(symbol)
                continue
            last_close = df['close'].dropna()
            if last_close.empty:
                missing_symbols.append(symbol)
                continue
            current_price = float(last_close.iloc[-1])
            for entry_price, qty, _ in entries:
                total_pnl += (current_price - entry_price) * qty

        return total_pnl, missing_symbols

    def _refresh_open_prices_core(self, account_filter: str) -> Tuple[str, List[str]]:
        """Fetch open prices and return status + missing symbols."""
        pnl_val, missing = self._compute_open_pnl_current(account_filter, fetch_if_missing=True)
        if missing:
            return (f"Open prices updated; missing: {', '.join(sorted(missing))}", missing)
        return ("Open prices updated", [])

    def refresh_open_prices(self) -> None:
        """Fetch price data for open positions (chart tab button)."""
        account_filter = self.account_var.get()
        self.chart_status_var.set("Refreshing open prices...")
        self.root.update_idletasks()
        try:
            status_msg, _ = self._refresh_open_prices_core(account_filter)
            self.chart_status_var.set(status_msg)
        except Exception as exc:
            self.chart_status_var.set(f"Error: {exc}")
        self.update_summary_and_chart()

    def refresh_open_prices_global(self) -> None:
        """Fetch price data for open positions from the main toolbar."""
        account_filter = self.account_var.get()
        try:
            status_msg, _ = self._refresh_open_prices_core(account_filter)
            messagebox.showinfo("Refresh Open Prices", status_msg)
        except Exception as exc:
            messagebox.showerror("Refresh Open Prices", f"Failed to refresh open prices: {exc}")
        self.update_summary_and_chart()

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