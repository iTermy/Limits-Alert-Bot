"""
Database schema creation and initialization
"""
from utils.logger import get_logger

logger = get_logger("database.schema")


async def initialize_database(db_manager):
    """
    Initialize database and create enhanced tables

    Args:
        db_manager: DatabaseManager instance
    """
    async with db_manager.get_connection() as conn:
        # Enable foreign key constraints
        await conn.execute("PRAGMA foreign_keys = ON")

        # Create enhanced signals table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                message_id TEXT UNIQUE NOT NULL,
                channel_id TEXT NOT NULL,
                instrument TEXT NOT NULL,
                direction TEXT NOT NULL,
                stop_loss REAL NOT NULL,
                expiry_type TEXT,
                expiry_time TIMESTAMP,
                status TEXT DEFAULT 'active',

                -- New fields for enhanced tracking
                first_limit_hit_time TIMESTAMP,     -- When first limit was hit
                closed_at TIMESTAMP,                 -- When signal reached final status
                closed_reason TEXT,                  -- 'automatic' or 'manual'
                result_pips REAL,                    -- For future P&L tracking

                -- Tracking fields
                total_limits INTEGER DEFAULT 0,      -- Total number of limits
                limits_hit INTEGER DEFAULT 0,        -- Number of limits hit

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                -- Constraints
                CHECK (status IN ('active', 'hit', 'profit', 'breakeven', 'stop_loss', 'cancelled')),
                CHECK (direction IN ('long', 'short'))
            )
        """)

        # Create enhanced limits table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS limits (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER NOT NULL,
                price_level REAL NOT NULL,
                sequence_number INTEGER NOT NULL,    -- Order of limits (1st, 2nd, 3rd)

                -- Status tracking
                status TEXT DEFAULT 'pending',
                hit_time TIMESTAMP,
                hit_price REAL,                      -- Actual price when hit (for spread tracking)

                -- Alert tracking
                approaching_alert_sent BOOLEAN DEFAULT FALSE,
                hit_alert_sent BOOLEAN DEFAULT FALSE,

                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (signal_id) REFERENCES signals(id) ON DELETE CASCADE,
                CHECK (status IN ('pending', 'hit', 'cancelled')),
                UNIQUE(signal_id, sequence_number)
            )
        """)

        # Create status_changes table for audit trail
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS status_changes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                signal_id INTEGER NOT NULL,
                old_status TEXT,
                new_status TEXT NOT NULL,
                change_type TEXT NOT NULL,           -- 'automatic' or 'manual'
                reason TEXT,                          -- Optional reason for manual changes
                changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

                FOREIGN KEY (signal_id) REFERENCES signals(id) ON DELETE CASCADE
            )
        """)

        # Create performance metrics table for analytics
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS performance_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date DATE NOT NULL,
                instrument TEXT,
                total_signals INTEGER DEFAULT 0,
                profitable INTEGER DEFAULT 0,
                breakeven INTEGER DEFAULT 0,
                stop_loss INTEGER DEFAULT 0,
                cancelled INTEGER DEFAULT 0,
                win_rate REAL,

                UNIQUE(date, instrument)
            )
        """)

        # Create comprehensive indexes
        await _create_indexes(conn)

        await conn.commit()
        logger.info("Enhanced database schema initialized successfully")


async def _create_indexes(conn):
    """
    Create database indexes for performance

    Args:
        conn: Database connection
    """
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_signals_status ON signals(status)",
        "CREATE INDEX IF NOT EXISTS idx_signals_channel ON signals(channel_id)",
        "CREATE INDEX IF NOT EXISTS idx_signals_message ON signals(message_id)",
        "CREATE INDEX IF NOT EXISTS idx_signals_instrument ON signals(instrument)",
        "CREATE INDEX IF NOT EXISTS idx_signals_closed_at ON signals(closed_at)",
        "CREATE INDEX IF NOT EXISTS idx_limits_signal ON limits(signal_id)",
        "CREATE INDEX IF NOT EXISTS idx_limits_status ON limits(status)",
        "CREATE INDEX IF NOT EXISTS idx_status_changes_signal ON status_changes(signal_id)",
        "CREATE INDEX IF NOT EXISTS idx_performance_date ON performance_metrics(date)"
    ]

    for index_query in indexes:
        await conn.execute(index_query)

    logger.debug(f"Created {len(indexes)} database indexes")