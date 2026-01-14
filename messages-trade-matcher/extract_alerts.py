#!/usr/bin/env python3
"""
Extract stock screener alert messages from macOS Messages SQLite database.

This script reads from chat_backup.db and exports messages to alerts.csv.

TO REFRESH YOUR DATABASE WITH NEW MESSAGES:
  1. Open Finder and press Cmd+Shift+G (Go to Folder)
  2. Paste this path: ~/Library/Messages
  3. Find the file named "chat.db"
  4. Copy it to your Desktop and rename to "chat_backup.db"
     (Replace your old chat_backup.db file)
  5. Run this script again to extract the latest alerts
"""

import sqlite3
import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
import os
import sys
import re

# ============================================================================
# Configuration: Set the start date for pulling messages
# Format: "YYYY-MM-DD" (e.g., "2025-05-01" for May 1, 2025)
# Set to None to include all messages
# ============================================================================
START_DATE = "2025-05-01"  # Change this to your desired start date
# ============================================================================


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
    
    # Extract symbols - primary path for "New symbol:" / "New symbols:"
    for symbol_marker in ["New symbols:", "New symbol:"]:
        if symbol_marker in text:
            try:
                start = text.find(symbol_marker) + len(symbol_marker)
                rest = text[start:]
                end_pos = min([pos for pos in [rest.find("was added to"), rest.find("were added to")] if pos != -1] or [-1])
                if end_pos != -1:
                    symbol_text = rest[:end_pos].strip()
                else:
                    symbol_text = rest.strip()
                raw_symbols = re.split(r"[,\s]+", symbol_text)
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
                end = rest.find(".")
                if end != -1:
                    strategy = rest[:end].strip()
                else:
                    strategy = rest
            except Exception as e:
                print(f"Warning: Could not parse strategy from text: {e}", file=sys.stderr)
            break

    # Fallback: handle "Following list of symbols were added to ..." or similar variants
    if (not symbols or not strategy) and "added to" in text:
        try:
            pattern = r"Alert:\s*(?:Following\s+list\s+of\s+symbols\s+|list\s+of\s+symbols\s+|symbols\s+)?(?P<symbols>.+?)\s+were\s+added\s+to\s+(?P<strategy>[^.]+)"
            match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
            if match:
                symbol_text = match.group("symbols").strip()
                raw_symbols = re.split(r"[,\s]+", symbol_text)
                parsed_symbols = [s.strip() for s in raw_symbols if s.strip()]
                if parsed_symbols:
                    symbols = parsed_symbols
                strategy_candidate = match.group("strategy").strip()
                if strategy_candidate:
                    strategy = strategy_candidate
        except Exception as e:
            print(f"Warning: Fallback parse failed: {e}", file=sys.stderr)

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
        
        # Parse start date if provided
        start_date_dt = None
        if START_DATE:
            try:
                start_date_dt = datetime.strptime(START_DATE, "%Y-%m-%d")
                print(f"Filtering messages from {START_DATE} onwards", file=sys.stderr)
            except ValueError:
                print(f"Error: Invalid START_DATE format '{START_DATE}'. Use 'YYYY-MM-DD'", file=sys.stderr)
                sys.exit(1)
        
        for message_id, apple_time, text, sender in rows:
            # Filter for messages from sender 81861
            if sender != "81861":
                continue
            
            timestamp_local = convert_apple_timestamp(apple_time)
            
            # Apply date filter if START_DATE is set
            if start_date_dt and timestamp_local:
                try:
                    message_dt = datetime.fromisoformat(timestamp_local)
                    # Compare dates only (ignore time)
                    if message_dt.date() < start_date_dt.date():
                        continue
                except ValueError:
                    pass
            
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
