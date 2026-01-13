#!/usr/bin/env python3
"""
Screenshot Migration Tool
=========================

This script helps migrate screenshots from absolute paths (especially macOS paths)
to relative paths or to a portable screenshots folder.

Usage:
1. Run this script in the same directory as your journal_state.pkl
2. Choose to either:
   - Convert paths to relative (works if images are in accessible locations)
   - Copy images to a portable 'screenshots' folder within the journal directory
"""

import os
import sys
import shutil
import pickle
from pathlib import Path

def load_state(filepath: str) -> dict:
    """Load the journal state from pickle file."""
    if not os.path.exists(filepath):
        print(f"Error: {filepath} not found")
        return {}
    
    try:
        with open(filepath, 'rb') as f:
            data = pickle.load(f)
        return data
    except Exception as e:
        print(f"Error loading state: {e}")
        return {}

def save_state(filepath: str, data: dict) -> None:
    """Save the journal state to pickle file."""
    try:
        with open(filepath, 'wb') as f:
            pickle.dump(data, f, protocol=pickle.HIGHEST_PROTOCOL)
        print(f"✓ Saved state to {filepath}")
    except Exception as e:
        print(f"Error saving state: {e}")

def migrate_to_relative(data: dict, journal_dir: str) -> dict:
    """Convert absolute screenshot paths to relative paths."""
    if 'screenshots' not in data:
        print("No screenshots found in journal state")
        return data
    
    screenshots = data['screenshots']
    migrated_count = 0
    
    for key, ss_list in screenshots.items():
        for ss_entry in ss_list:
            old_path = ss_entry.get('filepath', '')
            if not old_path or old_path.startswith('..'):
                continue
            
            # Try to make relative
            try:
                if os.path.isabs(old_path):
                    rel_path = os.path.relpath(old_path, journal_dir)
                    if not rel_path.startswith('..'):
                        ss_entry['filepath'] = rel_path
                        migrated_count += 1
                        print(f"  ✓ {old_path} → {rel_path}")
                    else:
                        print(f"  ⚠ {old_path} (outside journal dir, keeping absolute)")
            except Exception as e:
                print(f"  ✗ Error processing {old_path}: {e}")
    
    return data

def migrate_to_screenshots_folder(data: dict, journal_dir: str) -> dict:
    """Copy screenshot images to a portable 'screenshots' folder and update paths."""
    if 'screenshots' not in data:
        print("No screenshots found in journal state")
        return data
    
    # Create screenshots folder
    ss_folder = os.path.join(journal_dir, 'screenshots')
    os.makedirs(ss_folder, exist_ok=True)
    print(f"Created/verified screenshots folder: {ss_folder}")
    
    screenshots = data['screenshots']
    migrated_count = 0
    failed_count = 0
    
    for key, ss_list in screenshots.items():
        for ss_entry in ss_list:
            old_path = ss_entry.get('filepath', '')
            if not old_path:
                continue
            
            # Resolve the path if it's relative
            if not os.path.isabs(old_path):
                old_path = os.path.join(journal_dir, old_path)
            
            # Check if file exists
            if not os.path.exists(old_path):
                print(f"  ✗ File not found: {old_path}")
                failed_count += 1
                continue
            
            # Copy file to screenshots folder
            try:
                filename = os.path.basename(old_path)
                # Avoid name collisions by adding index if needed
                dest_path = os.path.join(ss_folder, filename)
                counter = 1
                base_name, ext = os.path.splitext(filename)
                while os.path.exists(dest_path):
                    dest_path = os.path.join(ss_folder, f"{base_name}_{counter}{ext}")
                    counter += 1
                
                shutil.copy2(old_path, dest_path)
                
                # Update the path to be relative to journal dir
                rel_path = os.path.relpath(dest_path, journal_dir)
                ss_entry['filepath'] = rel_path
                migrated_count += 1
                print(f"  ✓ Copied {filename}")
            except Exception as e:
                print(f"  ✗ Error copying {old_path}: {e}")
                failed_count += 1
    
    print(f"\nMigration complete: {migrated_count} copied, {failed_count} failed")
    return data

def main():
    """Main migration function."""
    print("=" * 60)
    print("Trade Journal Screenshot Migration Tool")
    print("=" * 60)
    
    # Find journal directory
    script_dir = os.path.dirname(os.path.abspath(__file__))
    state_file = os.path.join(script_dir, 'journal_state.pkl')
    
    print(f"\nJournal directory: {script_dir}")
    print(f"State file: {state_file}")
    
    if not os.path.exists(state_file):
        print(f"\n✗ Error: {state_file} not found")
        print("Make sure you run this in the same directory as your journal app")
        sys.exit(1)
    
    # Load state
    print("\nLoading journal state...")
    data = load_state(state_file)
    
    if not data:
        print("✗ Failed to load journal state")
        sys.exit(1)
    
    if 'screenshots' not in data or not data['screenshots']:
        print("✓ No screenshots found in journal")
        sys.exit(0)
    
    # Count screenshots
    total_ss = sum(len(ss_list) for ss_list in data['screenshots'].values())
    print(f"✓ Found {total_ss} screenshot(s) in {len(data['screenshots'])} trade(s)")
    
    # Ask user for migration method
    print("\n" + "=" * 60)
    print("Migration Options:")
    print("  1) Convert to RELATIVE PATHS (recommended if images are accessible)")
    print("  2) Copy to SCREENSHOTS FOLDER (best for portability across machines)")
    print("  3) Show current paths (no changes)")
    print("=" * 60)
    
    choice = input("\nSelect option (1-3): ").strip()
    
    if choice == '1':
        print("\n📁 Converting to relative paths...")
        data = migrate_to_relative(data, script_dir)
        save_state(state_file, data)
        
    elif choice == '2':
        print("\n📁 Copying images to screenshots folder...")
        data = migrate_to_screenshots_folder(data, script_dir)
        save_state(state_file, data)
        
    elif choice == '3':
        print("\n📋 Current screenshot paths:")
        for key, ss_list in data.get('screenshots', {}).items():
            print(f"\n  Trade {key}:")
            for ss_entry in ss_list:
                path = ss_entry.get('filepath', '(missing)')
                label = ss_entry.get('label', '(no label)')
                exists = "✓" if os.path.exists(path) or os.path.exists(os.path.join(script_dir, path)) else "✗"
                print(f"    {exists} {path}")
                print(f"       Label: {label}")
    else:
        print("Invalid option")
        sys.exit(1)
    
    print("\n✓ Done!")

if __name__ == '__main__':
    main()
