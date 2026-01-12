#!/usr/bin/env python3
"""
Trading Journal - A simple command-line application to track trading activities.
"""

import json
import os
from datetime import datetime
from typing import List, Dict, Optional


class Trade:
    """Represents a single trade."""
    
    def __init__(self, symbol: str, action: str, quantity: int, price: float, 
                 date: str = None, notes: str = ""):
        """
        Initialize a Trade.
        
        Args:
            symbol: Stock/asset symbol (e.g., 'AAPL', 'BTC')
            action: 'BUY' or 'SELL'
            quantity: Number of shares/units
            price: Price per unit
            date: Trade date (ISO format), defaults to current date
            notes: Optional notes about the trade
        """
        self.symbol = symbol.upper()
        self.action = action.upper()
        self.quantity = quantity
        self.price = price
        self.date = date or datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        self.notes = notes
        self.total = quantity * price
    
    def to_dict(self) -> Dict:
        """Convert trade to dictionary."""
        return {
            'symbol': self.symbol,
            'action': self.action,
            'quantity': self.quantity,
            'price': self.price,
            'date': self.date,
            'notes': self.notes,
            'total': self.total
        }
    
    @classmethod
    def from_dict(cls, data: Dict) -> 'Trade':
        """Create Trade from dictionary."""
        return cls(
            symbol=data['symbol'],
            action=data['action'],
            quantity=data['quantity'],
            price=data['price'],
            date=data.get('date'),
            notes=data.get('notes', '')
        )
    
    def __str__(self) -> str:
        """String representation of trade."""
        return (f"{self.date} | {self.symbol:8} | {self.action:4} | "
                f"Qty: {self.quantity:6} | Price: ${self.price:8.2f} | "
                f"Total: ${self.total:10.2f} | Notes: {self.notes}")


class Journal:
    """Manages a collection of trades."""
    
    def __init__(self, filename: str = 'trades.json'):
        """
        Initialize Journal.
        
        Args:
            filename: Path to JSON file for storing trades
        """
        self.filename = filename
        self.trades: List[Trade] = []
        self.load()
    
    def add_trade(self, trade: Trade) -> None:
        """Add a trade to the journal."""
        self.trades.append(trade)
        self.save()
    
    def get_trades(self, symbol: Optional[str] = None) -> List[Trade]:
        """
        Get trades, optionally filtered by symbol.
        
        Args:
            symbol: Optional symbol to filter by
            
        Returns:
            List of trades
        """
        if symbol:
            return [t for t in self.trades if t.symbol == symbol.upper()]
        return self.trades
    
    def delete_trade(self, index: int) -> bool:
        """
        Delete a trade by index.
        
        Args:
            index: Index of trade to delete (0-based)
            
        Returns:
            True if deleted, False if index invalid
        """
        if 0 <= index < len(self.trades):
            self.trades.pop(index)
            self.save()
            return True
        return False
    
    def get_summary(self) -> Dict:
        """Get summary statistics."""
        if not self.trades:
            return {'total_trades': 0, 'total_value': 0, 'symbols': []}
        
        symbols = set(t.symbol for t in self.trades)
        total_value = sum(abs(t.total) for t in self.trades)
        
        return {
            'total_trades': len(self.trades),
            'total_value': total_value,
            'symbols': sorted(list(symbols))
        }
    
    def save(self) -> None:
        """Save trades to JSON file."""
        with open(self.filename, 'w') as f:
            json.dump([t.to_dict() for t in self.trades], f, indent=2)
    
    def load(self) -> None:
        """Load trades from JSON file."""
        if os.path.exists(self.filename):
            try:
                with open(self.filename, 'r') as f:
                    data = json.load(f)
                    self.trades = [Trade.from_dict(t) for t in data]
            except (json.JSONDecodeError, KeyError):
                self.trades = []


class TradingJournalCLI:
    """Command-line interface for Trading Journal."""
    
    def __init__(self):
        """Initialize CLI."""
        self.journal = Journal()
    
    def display_menu(self) -> None:
        """Display main menu."""
        print("\n" + "="*60)
        print("           TRADING JOURNAL")
        print("="*60)
        print("1. Add Trade")
        print("2. View All Trades")
        print("3. View Trades by Symbol")
        print("4. Delete Trade")
        print("5. View Summary")
        print("6. Exit")
        print("="*60)
    
    def add_trade(self) -> None:
        """Add a new trade through CLI."""
        print("\n--- Add New Trade ---")
        try:
            symbol = input("Symbol (e.g., AAPL, BTC): ").strip()
            if not symbol:
                print("Error: Symbol cannot be empty.")
                return
            
            action = input("Action (BUY/SELL): ").strip().upper()
            if action not in ['BUY', 'SELL']:
                print("Error: Action must be BUY or SELL.")
                return
            
            quantity = int(input("Quantity: ").strip())
            if quantity <= 0:
                print("Error: Quantity must be positive.")
                return
            
            price = float(input("Price per unit: ").strip())
            if price <= 0:
                print("Error: Price must be positive.")
                return
            
            notes = input("Notes (optional): ").strip()
            
            trade = Trade(symbol, action, quantity, price, notes=notes)
            self.journal.add_trade(trade)
            print(f"\n✓ Trade added successfully!")
            print(f"  Total value: ${trade.total:.2f}")
            
        except ValueError as e:
            print(f"Error: Invalid input - {e}")
        except Exception as e:
            print(f"Error: {e}")
    
    def view_all_trades(self) -> None:
        """Display all trades."""
        trades = self.journal.get_trades()
        if not trades:
            print("\nNo trades found.")
            return
        
        print(f"\n--- All Trades ({len(trades)} total) ---")
        for i, trade in enumerate(trades):
            print(f"{i}. {trade}")
    
    def view_trades_by_symbol(self) -> None:
        """Display trades filtered by symbol."""
        symbol = input("\nEnter symbol: ").strip()
        if not symbol:
            print("Error: Symbol cannot be empty.")
            return
        
        trades = self.journal.get_trades(symbol)
        if not trades:
            print(f"\nNo trades found for {symbol.upper()}.")
            return
        
        print(f"\n--- Trades for {symbol.upper()} ({len(trades)} total) ---")
        for i, trade in enumerate(trades):
            print(f"{i}. {trade}")
    
    def delete_trade(self) -> None:
        """Delete a trade."""
        self.view_all_trades()
        if not self.journal.trades:
            return
        
        try:
            index = int(input("\nEnter trade number to delete: ").strip())
            if self.journal.delete_trade(index):
                print("✓ Trade deleted successfully!")
            else:
                print("Error: Invalid trade number.")
        except ValueError:
            print("Error: Please enter a valid number.")
    
    def view_summary(self) -> None:
        """Display journal summary."""
        summary = self.journal.get_summary()
        print("\n--- Trading Journal Summary ---")
        print(f"Total Trades: {summary['total_trades']}")
        print(f"Total Trade Value: ${summary['total_value']:.2f}")
        if summary['symbols']:
            print(f"Symbols Traded: {', '.join(summary['symbols'])}")
    
    def run(self) -> None:
        """Run the CLI application."""
        print("\nWelcome to Trading Journal!")
        
        while True:
            self.display_menu()
            choice = input("\nEnter your choice (1-6): ").strip()
            
            if choice == '1':
                self.add_trade()
            elif choice == '2':
                self.view_all_trades()
            elif choice == '3':
                self.view_trades_by_symbol()
            elif choice == '4':
                self.delete_trade()
            elif choice == '5':
                self.view_summary()
            elif choice == '6':
                print("\nThank you for using Trading Journal. Goodbye!")
                break
            else:
                print("\nError: Invalid choice. Please select 1-6.")


def main():
    """Main entry point."""
    cli = TradingJournalCLI()
    cli.run()


if __name__ == '__main__':
    main()
