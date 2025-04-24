import sqlite3
import logging
from typing import Optional, Any, List, Dict, Tuple

def get_db_connection() -> sqlite3.Connection:
    """Establishes a connection to the SQLite database."""
    conn = sqlite3.connect(settings.database_path)
    conn.row_factory = sqlite3.Row # Return rows as dictionary-like objects
    return conn

def initialize_database():
    """Creates the necessary tables if they don't exist."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # --- Transactions Table ---
        # Stores individual financial transactions from all sources
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transactions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                transaction_time TEXT NOT NULL,     -- ISO 8601 format timestamp (e.g., from Wise)
                description TEXT,                 -- Description of the transaction
                amount REAL NOT NULL,             -- Transaction amount (positive for income, negative for expense)
                currency TEXT NOT NULL,           -- 3-letter currency code (e.g., USD, NZD, KRW)
                source TEXT NOT NULL,             -- Where the data came from ('wise', 'anz', 'ocr')
                source_id TEXT UNIQUE,            -- Unique ID from the source system (e.g., Wise transaction ID) - helps prevent duplicates
                category TEXT,                    -- Assigned spending category (e.g., 'Groceries', 'Rent')
                raw_data TEXT,                    -- Store the original JSON/CSV row data for reference/debugging
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                updated_at TEXT DEFAULT CURRENT_TIMESTAMP
            );
        """)
        logger.info("Checked/Created 'transactions' table.")

        # --- Indexing ---
        # Index on source_id for faster lookups and duplicate checks
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_transactions_source_id ON transactions (source_id);")
        # Index on time and category for faster querying by the RAG retriever
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_time ON transactions (transaction_time);")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_transactions_category ON transactions (category);")
        logger.info("Checked/Created indexes.")

        # --- Trigger for updated_at ---
        # Automatically update updated_at timestamp on row changes
        cursor.execute("""
            CREATE TRIGGER IF NOT EXISTS update_transactions_updated_at
            AFTER UPDATE ON transactions
            FOR EACH ROW
            BEGIN
                UPDATE transactions SET updated_at = CURRENT_TIMESTAMP WHERE id = OLD.id;
            END;
        """)
        logger.info("Checked/Created updated_at trigger.")

        # Add other tables as needed (e.g., accounts, categories)

        conn.commit()
        logger.info("Database initialization complete.")
    except sqlite3.Error as e:
        logger.error(f"Database initialization error: {e}")
        conn.rollback()
    finally:
        conn.close()

def execute_query(query: str, params: Optional[Tuple] = None) -> List[Dict[str, Any]]:
    """Executes a SELECT query and returns results."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        results = [dict(row) for row in cursor.fetchall()]
        return results
    except sqlite3.Error as e:
        logger.error(f"Error executing query: {e}\nQuery: {query}\nParams: {params}")
        return []
    finally:
        conn.close()

def execute_update(query: str, params: Optional[Tuple] = None) -> bool:
    """Executes an INSERT, UPDATE, or DELETE query."""
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        if params:
            cursor.execute(query, params)
        else:
            cursor.execute(query)
        conn.commit()
        return True
    except sqlite3.Error as e:
        logger.error(f"Error executing update: {e}\nQuery: {query}\nParams: {params}")
        conn.rollback()
        return False
    finally:
        conn.close()
