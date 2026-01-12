# Trading Journal

A simple command-line Python application to track and manage your trading activities.

## Features

- **Add Trades**: Record buy/sell transactions with symbol, quantity, price, and notes
- **View Trades**: List all trades or filter by specific symbols
- **Delete Trades**: Remove trades from your journal
- **Summary Statistics**: View total trades, total value, and symbols traded
- **Persistent Storage**: All trades are automatically saved to JSON file

## Requirements

- Python 3.6 or higher
- No external dependencies required

## Installation

1. Clone the repository:
```bash
git clone https://github.com/Lsthun/TradeJournal.git
cd TradeJournal
```

2. (Optional) Create a virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies (if any):
```bash
pip install -r requirements.txt
```

## Usage

Run the trading journal application:

```bash
python3 trade_journal.py
```

### Main Menu Options

1. **Add Trade**: Enter details for a new trade
   - Symbol (e.g., AAPL, TSLA, BTC)
   - Action (BUY or SELL)
   - Quantity (number of shares/units)
   - Price per unit
   - Optional notes

2. **View All Trades**: Display all recorded trades with details

3. **View Trades by Symbol**: Filter and view trades for a specific symbol

4. **Delete Trade**: Remove a trade by its index number

5. **View Summary**: Display statistics including:
   - Total number of trades
   - Total trade value
   - List of symbols traded

6. **Exit**: Save and exit the application

### Example Usage

```
Welcome to Trading Journal!

============================================================
           TRADING JOURNAL
============================================================
1. Add Trade
2. View All Trades
3. View Trades by Symbol
4. Delete Trade
5. View Summary
6. Exit
============================================================

Enter your choice (1-6): 1

--- Add New Trade ---
Symbol (e.g., AAPL, BTC): AAPL
Action (BUY/SELL): BUY
Quantity: 10
Price per unit: 150.50
Notes (optional): Strong earnings report

✓ Trade added successfully!
  Total value: $1505.00
```

## Data Storage

All trades are automatically saved to `trades.json` in the current directory. The data persists between sessions.

## Project Structure

```
TradeJournal/
├── trade_journal.py    # Main application
├── requirements.txt    # Python dependencies
├── .gitignore         # Git ignore file
└── README.md          # This file
```

## Classes

### Trade
Represents a single trade with properties:
- `symbol`: Stock/asset symbol
- `action`: BUY or SELL
- `quantity`: Number of units
- `price`: Price per unit
- `date`: Trade timestamp
- `notes`: Optional notes
- `total`: Calculated total value

### Journal
Manages the collection of trades with methods:
- `add_trade()`: Add a new trade
- `get_trades()`: Retrieve trades (optionally filtered)
- `delete_trade()`: Remove a trade
- `get_summary()`: Get statistics
- `save()`: Persist to file
- `load()`: Load from file

### TradingJournalCLI
Provides the command-line interface for user interaction.

## License

MIT License

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.