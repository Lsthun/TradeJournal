"""
Trade Journal Application - Streamlit + Plotly Edition
======================================================

Modern web-based trade journal with interactive Plotly charts.
Run with: streamlit run app_streamlit.py
"""

import csv
import datetime as dt
import os
import pickle
from dataclasses import dataclass
from typing import Dict, List, Tuple, Optional

import pandas as pd
import plotly.graph_objects as go
import streamlit as st


@dataclass
class Transaction:
    """Represents a single transaction from the CSV."""
    run_date: dt.datetime
    account: str
    account_number: str
    symbol: str
    action: str
    price: float
    quantity: float
    amount: float
    settlement_date: Optional[dt.datetime]

    @property
    def is_buy(self) -> bool:
        return self.quantity > 0

    @property
    def is_sell(self) -> bool:
        return self.quantity < 0


@dataclass
class TradeEntry:
    """Represents a matched trade (one or more buys matched to a sell)."""
    account: str
    account_number: str
    symbol: str
    entry_date: dt.datetime
    entry_price: float
    exit_date: Optional[dt.datetime]
    exit_price: Optional[float]
    quantity: float
    pnl: Optional[float]
    hold_period: Optional[int]
    note: str = ""
    buy_id: int = -1

    @property
    def is_closed(self) -> bool:
        return self.exit_date is not None

    @property
    def pnl_pct(self) -> Optional[float]:
        """Return the percentage return with 2 decimal places."""
        if self.exit_price is None or self.entry_price == 0:
            return None
        return round(((self.exit_price - self.entry_price) / self.entry_price) * 100, 2)


class TradeJournalModel:
    """Core logic for storing transactions, matching trades, and computing metrics."""

    def __init__(self):
        self.transactions: List[Transaction] = []
        self.trades: List[TradeEntry] = []
        self.open_positions: Dict[Tuple[str, str], List[Dict[str, object]]] = {}
        self.notes: Dict[tuple, str] = {}
        self.next_buy_id: int = 0
        self.screenshots: Dict[tuple, str] = {}
        self.seen_tx_keys: set = set()
        self.duplicate_transactions: List[Transaction] = []

    def clear(self) -> None:
        """Reset all stored data."""
        self.transactions.clear()
        self.trades.clear()
        self.open_positions.clear()
        self.notes.clear()
        self.screenshots.clear()
        self.seen_tx_keys.clear()
        self.next_buy_id = 0

    def reset_matching(self) -> None:
        """Reset trades and open positions while preserving transactions."""
        self.trades.clear()
        self.open_positions.clear()
        self.next_buy_id = 0

    def load_csv(self, filepath: str) -> None:
        """Load and parse transactions from a Fidelity CSV file."""
        self.reset_matching()
        self.duplicate_transactions.clear()
        self.duplicate_count = 0
        existing_keys = set(self.seen_tx_keys)
        new_keys: set = set()
        try:
            with open(filepath, newline="", encoding="utf-8-sig") as f:
                reader = csv.reader(f)
                header: List[str] = []
                for row in reader:
                    if not row or not any(cell.strip() for cell in row):
                        continue
                    header = row
                    break
                if not header:
                    raise RuntimeError("CSV file appears to be empty or missing header")
                header_map = {name.strip(): idx for idx, name in enumerate(header)}
                run_date_idx = header_map.get("Run Date")
                account_idx = header_map.get("Account")
                acct_num_idx = header_map.get("Account Number")
                action_idx = header_map.get("Action")
                symbol_idx = header_map.get("Symbol")
                price_idx = header_map.get("Price ($)")
                qty_idx = header_map.get("Quantity")
                amount_idx = header_map.get("Amount ($)")
                settlement_idx = header_map.get("Settlement Date")
                if run_date_idx is None or qty_idx is None or price_idx is None:
                    raise RuntimeError("CSV file is missing required columns")
                
                def to_float(s: str) -> float:
                    try:
                        return float(s.replace(',', '')) if s else 0.0
                    except ValueError:
                        return 0.0
                
                for row in reader:
                    if not row or len(row) <= run_date_idx:
                        continue
                    run_date_str = row[run_date_idx].strip()
                    if not run_date_str or not run_date_str[0].isdigit():
                        continue
                    run_date: Optional[dt.datetime] = None
                    for fmt in ("%m/%d/%Y %I:%M %p", "%m/%d/%Y"):
                        try:
                            run_date = dt.datetime.strptime(run_date_str, fmt)
                            break
                        except ValueError:
                            continue
                    if run_date is None:
                        continue
                    
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
                    key = (run_date, acct_num, symbol, qty, price, amount)
                    if key in existing_keys:
                        self.duplicate_transactions.append(tx)
                        self.duplicate_count += 1
                        continue
                    new_keys.add(key)
                    self.transactions.append(tx)
            self.seen_tx_keys.update(new_keys)
        except Exception as e:
            raise RuntimeError(f"Failed to load CSV: {e}")
        self._match_trades()

    def save_state(self, filepath: str) -> None:
        """Persist the current state to disk."""
        data = {
            'transactions': self.transactions,
            'notes': self.notes,
            'screenshots': self.screenshots,
            'seen_tx_keys': self.seen_tx_keys,
        }
        try:
            with open(filepath, 'wb') as f:
                pickle.dump(data, f)
        except Exception:
            pass

    def load_state(self, filepath: str) -> None:
        """Load persisted state from disk."""
        if not os.path.exists(filepath):
            return
        try:
            with open(filepath, 'rb') as f:
                data = pickle.load(f)
            self.transactions = data.get('transactions', [])
            self.notes = data.get('notes', {})
            self.screenshots = data.get('screenshots', {})
            self.seen_tx_keys = data.get('seen_tx_keys', set())
            self.next_buy_id = 0
            self._match_trades()
        except Exception:
            self.clear()

    def compute_key(self, trade: TradeEntry) -> tuple:
        """Compute a stable unique key for a trade entry."""
        entry_date_str = trade.entry_date.isoformat()
        exit_date_str = trade.exit_date.isoformat() if trade.exit_date else None
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
        self.trades = []
        self.open_positions = {}
        sorted_txs = sorted(self.transactions, key=lambda tx: tx.run_date)
        
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
                    )
                    self.trades.append(trade)
                    remaining -= matched_qty
                    buy["qty"] -= matched_qty
                    if buy["qty"] <= 1e-8:
                        self.open_positions[key].pop(0)
        
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
                )
                self.trades.append(trade)
        
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
        """Compute summary statistics for trades."""
        total_pnl = 0.0
        num_trades = 0
        num_wins = 0
        num_losses = 0
        total_hold = 0
        
        for trade in self.trades:
            if not trade.is_closed:
                continue
            if account_filter and account_filter != "all" and trade.account_number != account_filter:
                continue
            if start_date and trade.entry_date.date() < start_date:
                continue
            if end_date and trade.entry_date.date() > end_date:
                continue
            if closed_only:
                if trade.buy_id < 0:
                    continue
                if self.open_qty_by_buy_id.get(trade.buy_id, 0.0) > 1e-8:
                    continue
            
            pnl = trade.pnl or 0.0
            total_pnl += pnl
            num_trades += 1
            if pnl > 0:
                num_wins += 1
            elif pnl < 0:
                num_losses += 1
            total_hold += trade.hold_period or 0
        
        win_ratio = (num_wins / num_trades) if num_trades else 0.0
        avg_pnl = (total_pnl / num_trades) if num_trades else 0.0
        avg_hold = (total_hold / num_trades) if num_trades else 0.0
        
        return {
            "total_pnl": total_pnl,
            "num_trades": num_trades,
            "num_wins": num_wins,
            "num_losses": num_losses,
            "win_ratio": win_ratio,
            "avg_pnl": avg_pnl,
            "avg_hold": avg_hold,
        }

    def equity_curve(self, account_filter: Optional[str] = None, *, closed_only: bool = False,
                     start_date: Optional[dt.date] = None, end_date: Optional[dt.date] = None) -> pd.DataFrame:
        """Return a DataFrame representing the cumulative equity over time."""
        data: Dict[dt.date, float] = {}
        
        for trade in self.trades:
            if not trade.is_closed:
                continue
            if account_filter and account_filter != "all" and trade.account_number != account_filter:
                continue
            if start_date and trade.entry_date.date() < start_date:
                continue
            if end_date and trade.entry_date.date() > end_date:
                continue
            if closed_only:
                if trade.buy_id < 0 or self.open_qty_by_buy_id.get(trade.buy_id, 0.0) > 1e-8:
                    continue
            
            exit_date = trade.exit_date.date()
            data[exit_date] = data.get(exit_date, 0.0) + (trade.pnl or 0.0)
        
        dates = sorted(data.keys())
        equity_values = []
        cumulative = 0.0
        for d in dates:
            cumulative += data[d]
            equity_values.append(cumulative)
        
        return pd.DataFrame({"date": dates, "equity": equity_values})


# ============================================================================
# Initialize Session State
# ============================================================================

if 'model' not in st.session_state:
    st.session_state.model = TradeJournalModel()
    script_dir = os.path.dirname(os.path.abspath(__file__))
    persist_path = os.path.join(script_dir, 'journal_state.pkl')
    st.session_state.model.load_state(persist_path)
    st.session_state.persist_path = persist_path

model = st.session_state.model


# ============================================================================
# Page Configuration
# ============================================================================

st.set_page_config(
    page_title="Trade Journal",
    page_icon="📊",
    layout="wide",
)


# ============================================================================
# Main App
# ============================================================================

st.title("📊 Trade Journal")

# Sidebar controls
with st.sidebar:
    st.subheader("📁 Import Data")
    uploaded_file = st.file_uploader("Upload CSV", type="csv")
    if uploaded_file:
        temp_path = "temp_upload.csv"
        with open(temp_path, "wb") as f:
            f.write(uploaded_file.getbuffer())
        
        try:
            model.load_csv(temp_path)
            st.session_state.persist_path = st.session_state.persist_path
            try:
                model.save_state(st.session_state.persist_path)
            except:
                pass
            st.success("CSV loaded successfully!")
            if model.duplicate_transactions:
                st.warning(f"{len(model.duplicate_transactions)} duplicate transactions skipped.")
        except Exception as e:
            st.error(f"Error: {e}")
        finally:
            os.remove(temp_path)
    
    st.divider()
    st.subheader("🎛️ Filters")
    
    accounts = sorted({tx.account_number for tx in model.transactions})
    account_filter = st.selectbox("Account", ["all"] + accounts)
    
    col1, col2 = st.columns(2)
    with col1:
        start_date = st.date_input("Start Date", value=None)
    with col2:
        end_date = st.date_input("End Date", value=None)
    
    closed_only = st.checkbox("Closed Only", value=False)

# Main content
if not model.trades:
    st.info("👈 Upload a CSV file to get started")
else:
    # Summary metrics
    summary = model.compute_summary(
        account_filter if account_filter != "all" else None,
        closed_only=closed_only,
        start_date=start_date,
        end_date=end_date
    )
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total P&L", f"${summary['total_pnl']:.2f}")
    with col2:
        st.metric("# Trades", int(summary['num_trades']))
    with col3:
        st.metric("Win Ratio", f"{summary['win_ratio']*100:.1f}%")
    with col4:
        st.metric("Avg P&L", f"${summary['avg_pnl']:.2f}")
    
    st.divider()
    
    # Equity curve with Plotly
    eq_df = model.equity_curve(
        account_filter if account_filter != "all" else None,
        closed_only=closed_only,
        start_date=start_date,
        end_date=end_date
    )
    
    if not eq_df.empty:
        fig = go.Figure()
        fig.add_trace(go.Scatter(
            x=eq_df["date"],
            y=eq_df["equity"],
            mode="lines+markers",
            name="Cumulative P&L",
            line=dict(color="#1f77b4", width=2),
            marker=dict(size=6),
            fill="tozeroy",
            fillcolor="rgba(31, 119, 180, 0.2)",
            hovertemplate="<b>%{x|%Y-%m-%d}</b><br>P&L: $%{y:,.2f}<extra></extra>"
        ))
        
        fig.update_layout(
            title="Equity Curve",
            xaxis_title="Date",
            yaxis_title="Cumulative P&L ($)",
            template="plotly_white",
            height=400,
            hovermode="x unified",
        )
        
        st.plotly_chart(fig, width='stretch')
    else:
        st.info("No equity data to display")
    
    st.divider()
    
    # Trades table
    st.subheader("📋 Trades")
    
    display_trades = []
    for trade in model.trades:
        if account_filter != "all" and trade.account_number != account_filter:
            continue
        if start_date and trade.entry_date.date() < start_date:
            continue
        if end_date and trade.entry_date.date() > end_date:
            continue
        if closed_only and not trade.is_closed:
            continue
        display_trades.append(trade)
    
    trade_data = []
    for trade in display_trades:
        trade_data.append({
            "Account": trade.account_number,
            "Symbol": trade.symbol,
            "Entry Date": trade.entry_date.strftime("%Y-%m-%d"),
            "Entry Price": f"${trade.entry_price:.2f}",
            "Exit Date": trade.exit_date.strftime("%Y-%m-%d") if trade.exit_date else "-",
            "Exit Price": f"${trade.exit_price:.2f}" if trade.exit_price else "-",
            "Qty": f"{trade.quantity:.2f}",
            "P&L": f"${trade.pnl:.2f}" if trade.pnl is not None else "-",
            "P&L %": f"{trade.pnl_pct:.2f}%" if trade.pnl_pct is not None else "-",
            "Days": int(trade.hold_period) if trade.hold_period is not None else "-",
        })
    
    df_display = pd.DataFrame(trade_data)
    st.dataframe(df_display, width='stretch')
    
    # Export
    st.divider()
    st.subheader("💾 Export")
    
    col1, col2 = st.columns(2)
    with col1:
        csv_data = df_display.to_csv(index=False)
        st.download_button(
            label="Download CSV",
            data=csv_data,
            file_name="trade_journal.csv",
            mime="text/csv"
        )
    
    with col2:
        try:
            buffer = pd.ExcelWriter("temp.xlsx", engine="openpyxl")
            df_display.to_excel(buffer, index=False)
            buffer.close()
            with open("temp.xlsx", "rb") as f:
                excel_data = f.read()
            st.download_button(
                label="Download Excel",
                data=excel_data,
                file_name="trade_journal.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )
            os.remove("temp.xlsx")
        except ImportError:
            st.info("Install openpyxl for Excel export")
