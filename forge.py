#!/usr/bin/env python3
"""
Offline Data Forge (ODF)
------------------------
A zero-dependency infrastructure tool for offline-first applications.
Dynamically ingests flat-file data (CSV) and forges optimized, indexed 
SQLite databases for use in zero-connectivity environments (e.g., remote classrooms).

Author: Maintainer
License: MIT
"""

import sqlite3
import csv
import os
import sys
import argparse
import logging
from typing import List, Dict, Any, Tuple

# Configure logging for CI/CD and terminal output
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - [%(levelname)s] - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger("OfflineForge")

def infer_sql_type(value: str) -> str:
    """Infers the SQLite data type from a string value."""
    if not value or value.strip() == "":
        return "TEXT"
    
    # Try Integer
    try:
        int(value)
        return "INTEGER"
    except ValueError:
        pass

    # Try Float
    try:
        float(value)
        return "REAL"
    except ValueError:
        pass

    return "TEXT"

def analyze_headers_and_types(csv_file: str) -> Tuple[List[str], List[str]]:
    """Reads the CSV to extract headers and infer the best data types."""
    with open(csv_file, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        try:
            headers = next(reader)
        except StopIteration:
            logger.error(f"File {csv_file} is empty.")
            sys.exit(1)
        
        # Sanitize headers (remove spaces, special chars)
        clean_headers = [h.strip().replace(" ", "_").replace("-", "_").lower() for h in headers]
        
        # Read the first data row to infer types
        try:
            first_row = next(reader)
            types = [infer_sql_type(val) for val in first_row]
        except StopIteration:
            types = ["TEXT" for _ in clean_headers]
            
        return clean_headers, types

def forge_database(csv_path: str, db_path: str, table_name: str, chunk_size: int = 1000):
    """Core engine: Creates the database schema and batches data insertion."""
    if not os.path.exists(csv_path):
        logger.error(f"Input file not found: {csv_path}")
        sys.exit(1)

    logger.info(f"Analyzing schema for {csv_path}...")
    headers, types = analyze_headers_and_types(csv_path)
    
    # Construct CREATE TABLE statement
    columns_def = ", ".join([f"{h} {t}" for h, t in zip(headers, types)])
    create_stmt = f"CREATE TABLE IF NOT EXISTS {table_name} (id INTEGER PRIMARY KEY AUTOINCREMENT, {columns_def});"
    
    logger.info(f"Forging offline database at {db_path}...")
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Build schema
    cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
    cursor.execute(create_stmt)
    
    # Prepare INSERT statement
    placeholders = ", ".join(["?" for _ in headers])
    insert_stmt = f"INSERT INTO {table_name} ({', '.join(headers)}) VALUES ({placeholders})"
    
    # Process data in chunks to handle massive datasets without memory overflow
    logger.info("Ingesting data payload...")
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        
        batch = []
        total_inserted = 0
        
        for row in reader:
            # Pad row if it's missing columns
            if len(row) < len(headers):
                row.extend([""] * (len(headers) - len(row)))
            # Truncate row if it has too many columns
            row = row[:len(headers)]
            
            batch.append(row)
            
            if len(batch) >= chunk_size:
                cursor.executemany(insert_stmt, batch)
                conn.commit()
                total_inserted += len(batch)
                batch = []
                
        # Insert remaining
        if batch:
            cursor.executemany(insert_stmt, batch)
            conn.commit()
            total_inserted += len(batch)
            
    # Create indexes on the first two columns for faster offline querying
    if len(headers) > 0:
        cursor.execute(f"CREATE INDEX IF NOT EXISTS idx_{headers[0]} ON {table_name}({headers[0]})")
    
    conn.close()
    logger.info(f"Success! {total_inserted} records secured in offline table '{table_name}'.")

def main():
    parser = argparse.ArgumentParser(description="Offline Data Forge - CSV to SQLite compiler for zero-connectivity apps.")
    parser.add_argument("input_csv", help="Path to the source CSV file")
    parser.add_argument("--db", default="offline_storage.db", help="Output SQLite database name")
    parser.add_argument("--table", default="dataset", help="Target table name")
    
    args = parser.parse_args()
    
    print("\n========================================")
    print("   OFFLINE DATA FORGE - Engine v1.0")
    print("========================================\n")
    
    forge_database(args.input_csv, args.db, args.table)

if __name__ == "__main__":
    main()
