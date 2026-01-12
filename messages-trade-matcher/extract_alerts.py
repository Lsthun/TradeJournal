#!/usr/bin/env python3
"""
Extract stock screener alert messages from macOS Messages SQLite database.

This script reads from chat_backup.db and exports messages to alerts.csv.
"""

import sqlite3
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
import os
import sys


def convert_apple_timestamp(apple_time):
    """
    Convert Apple epoch time (nanoseconds since 2001-01-01) to local datetime.
    
    Args:
        apple_time: Time in Apple epoch (nanoseconds)
        
    Returns:
        ISO 8601 formatted string in America/Chicago timezone, or None if invalid
    """
    if apple_time is None or apple_time == 0:
        return None
    
    try:
        # Convert from nanoseconds to seconds and apply Apple epoch offset
        unix_seconds = (apple_time / 1e9) + 978307200
        
        # Create UTC datetime and convert to Chicago timezone
        utc_dt = datetime.fromtimestamp(unix_seconds, tz=ZoneInfo("UTC"))
        chicago_tz = ZoneInfo("America/Chicago")
        chicago_dt = utc_dt.astimezone(chicago_tz)
        
        # Return ISO 8601 format
        return chicago_dt.isoformat()
    except (ValueError, OSError) as e:
        print(f"Warning: Could not convert timestamp {apple_time}: {e}", file=sys.stderr)
        return None


def parse_strategy_and_symbols(text):
    """
    Extract strategy name and symbols from message text.
    
    Format: "Alert: New symbol(s): [SYMBOLS] was/were added to [STRATEGY]."
    
    Symbols: Listed after "New symbol:" or "New symbols:" and before "was/were added to"
    Strategy name: After "was/were added to" and ended by a period.
    
    Args:
        text: Message text to parse
        
    Returns:
        Tuple of (strategy_name, list_of_symbols) or (None, []) if parsing fails
    """
    if not text:
        return None, []
    
    strategy = None
    symbols = []
    
    # Check if this is an alert message
    if "Alert:" not in text or ("was added to" not in text and "were added to" not in text):
        return None, []
    
    # Extract symbols - look for "New symbol:" or "New symbols:"
    for symbol_marker in ["New symbols:", "New symbol:"]:
        if symbol_marker in text:
            try:
                start = text.find(symbol_marker) + len(symbol_marker)
                # Find where symbols end (at "was added to" or "were added to")
                rest = text[start:]
                
                # Look for the end of symbols (before was/were added to)
                end_marker = None
                end_pos = -1
                for marker in ["was added to", "were added to"]:
                    pos = rest.find(marker)
                    if pos != -1 and (end_pos == -1 or pos < end_pos):
                        end_pos = pos
                        end_marker = marker
                
                if end_pos != -1:
                    symbol_text = rest[:end_pos].strip()
                    raw_symbols = symbol_text.split(",")
                    symbols = [s.strip() for s in raw_symbols if s.strip()]
            except Exception as e:
                print(f"Warning: Could not parse symbols from text: {e}", file=sys.stderr)
            break
    
    # Extract strategy name - after "was/were added to" and before period
    for added_marker in ["was added to", "were added to"]:
        if added_marker in text:
            try:
                start = text.find(added_marker) + len(added_marker)
                rest = text[start:].strip()
                # Strategy ends at the first period
                end = rest.find(".")
                if end != -1:
                    strategy = rest[:end].strip()
            except Exception as e:
                print(f"Warning: Could not parse strategy from text: {e}", file=sys.stderr)
            break
    
    return strategy, symbols


def main():
    """Main function to extract alerts from Messages database."""
    
    # Look for database in multiple locations
    db_path = None
    for path in ["chat_backup.db", 
                 os.path.expanduser("~/Desktop/chat_backup.db"),
                 os.path.expanduser("~/Library/Messages/chat.db")]:
        if os.path.exists(path):
            db_path = path
            break
    
    if not db_path:
        print("Error: chat_backup.db not found", file=sys.stderr)
        sys.exit(1)
    
    try:
        # Connect to database with timeout
        conn = sqlite3.connect(db_path, timeout=30.0)
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        # Execute exact query as specified
        print("Querying database... this may take a moment", file=sys.stderr)
        query = """
        SELECT
          message.ROWID AS message_id,
          message.date,
          message.text,
          handle.id AS sender
        FROM message
        LEFT JOIN handle ON message.handle_id = handle.ROWID
        WHERE message.text IS NOT NULL
        ORDER BY message.date;
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        conn.close()
        print(f"Retrieved {len(rows)} messages from database", file=sys.stderr)
        
        if not rows:
            print("No messages found in database.", file=sys.stderr)
            return
        
        # Process rows and extract strategy alerts from sender 81861
        data = []
        for message_id, apple_time, text, sender in rows:
            # Filter for messages from sender 81861
            if sender != "81861":
                continue
            
            timestamp_local = convert_apple_timestamp(apple_time)
            strategy, symbols = parse_strategy_and_symbols(text)
            
            # Create one row per symbol
            if symbols and strategy:
                for symbol in symbols:
                    data.append({
                        "date": timestamp_local,
                        "strategy": strategy,
                        "symbol": symbol
                    })
        
        # Write to CSV using pandas
        df = pd.DataFrame(data)
        output_file = "alerts.csv"
        df.to_csv(output_file, index=False)
        
        print(f"Successfully exported {len(df)} alerts to {output_file}")
        
    except sqlite3.DatabaseError as e:
        print(f"Error reading database: {e}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"Unexpected error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
