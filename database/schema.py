"""
Database schema creation and initialization (PostgreSQL / Supabase)
"""
from utils.logger import get_logger

logger = get_logger("database.schema")


async def initialize_database(db_manager):
    """
    Initialize database and create tables if they don't exist.

    Args:
        db_manager: DatabaseManager instance
    """
    async with db_manager.get_connection() as conn:

        # Create signals table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS signals (
                id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                message_id  TEXT UNIQUE NOT NULL,
                channel_id  TEXT NOT NULL,
                instrument  TEXT NOT NULL,
                direction   TEXT NOT NULL,
                stop_loss   DOUBLE PRECISION NOT NULL,
                expiry_type TEXT,
                expiry_time TIMESTAMPTZ,
                status      TEXT DEFAULT 'active',

                first_limit_hit_time TIMESTAMPTZ,
                closed_at            TIMESTAMPTZ,
                closed_reason        TEXT,
                result_pips          DOUBLE PRECISION,

                total_limits INTEGER DEFAULT 0,
                limits_hit   INTEGER DEFAULT 0,

                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),

                CONSTRAINT signals_status_check
                    CHECK (status IN ('active', 'hit', 'profit', 'breakeven', 'stop_loss', 'cancelled')),
                CONSTRAINT signals_direction_check
                    CHECK (direction IN ('long', 'short'))
            )
        """)

        # Create limits table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS limits (
                id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                signal_id       BIGINT NOT NULL REFERENCES signals(id) ON DELETE CASCADE,
                price_level     DOUBLE PRECISION NOT NULL,
                sequence_number INTEGER NOT NULL,

                status    TEXT DEFAULT 'pending',
                hit_time  TIMESTAMPTZ,
                hit_price DOUBLE PRECISION,

                approaching_alert_sent BOOLEAN DEFAULT FALSE,
                hit_alert_sent         BOOLEAN DEFAULT FALSE,

                created_at TIMESTAMPTZ DEFAULT NOW(),

                CONSTRAINT limits_status_check
                    CHECK (status IN ('pending', 'hit', 'cancelled')),
                CONSTRAINT limits_signal_seq_unique
                    UNIQUE (signal_id, sequence_number)
            )
        """)

        # Create status_changes audit table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS status_changes (
                id          BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                signal_id   BIGINT NOT NULL REFERENCES signals(id) ON DELETE CASCADE,
                old_status  TEXT,
                new_status  TEXT NOT NULL,
                change_type TEXT NOT NULL,
                reason      TEXT,
                changed_at  TIMESTAMPTZ DEFAULT NOW()
            )
        """)

        # Create performance metrics table
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS performance_metrics (
                id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                date         DATE NOT NULL,
                instrument   TEXT,
                total_signals INTEGER DEFAULT 0,
                profitable    INTEGER DEFAULT 0,
                breakeven     INTEGER DEFAULT 0,
                stop_loss     INTEGER DEFAULT 0,
                cancelled     INTEGER DEFAULT 0,
                win_rate      DOUBLE PRECISION,

                CONSTRAINT perf_date_instrument_unique UNIQUE (date, instrument)
            )
        """)

        await _create_indexes(conn)

    logger.info("Database schema initialized successfully")


async def _create_indexes(conn):
    """
    Create database indexes for performance.

    Args:
        conn: asyncpg connection
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
        "CREATE INDEX IF NOT EXISTS idx_performance_date ON performance_metrics(date)",
    ]

    for index_query in indexes:
        await conn.execute(index_query)

    logger.debug(f"Created {len(indexes)} database indexes")