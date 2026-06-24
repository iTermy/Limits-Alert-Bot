"""
Database schema creation and initialization (PostgreSQL / Supabase)
"""

import json
from pathlib import Path

from utils.logger import get_logger

logger = get_logger("database.schema")

# Inline channel-name → signal-type map for the one-time backfill. Mirrors
# core.parser.pattern_parsers.CHANNEL_TYPE_MAP — kept here to avoid importing
# the parser (and its heavy deps) into the DB layer.
_CHANNEL_NAME_TO_TYPE = {
    "scalps": "scalp",
    "swing-trades": "swing",
    "gold-swings": "swing",
    "gold-tolls-map": "toll",
    "general-tolls": "toll",
    "oil-tolls": "toll",
    "gold-pa-signals": "pa",
    "price-action-trades": "pa",
    "gold-1-1-rr": "1-1",
}


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
                tp_price             DOUBLE PRECISION,

                total_limits INTEGER DEFAULT 0,
                limits_hit   INTEGER DEFAULT 0,
                type         TEXT DEFAULT 'standard',

                created_at TIMESTAMPTZ DEFAULT NOW(),
                updated_at TIMESTAMPTZ DEFAULT NOW(),

                CONSTRAINT signals_status_check
                    CHECK (status IN ('active', 'hit', 'profit', 'breakeven', 'stop_loss', 'cancelled')),
                CONSTRAINT signals_direction_check
                    CHECK (direction IN ('long', 'short')),
                CONSTRAINT signals_type_check
                    CHECK (type IN ('standard', 'scalp', 'swing', 'toll', 'pa', '1-1'))
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

        # Create live_prices table (written by LivePriceWriter, read by execution bot)
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS live_prices (
                symbol     TEXT PRIMARY KEY,
                bid        DOUBLE PRECISION NOT NULL,
                ask        DOUBLE PRECISION NOT NULL,
                feed       TEXT NOT NULL,
                updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
            )
        """)

        # Create trailing_simulations table — shadow what-if trailing-stop
        # tracking, written by TrailingStopMonitor. Never read by alerting code.
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS trailing_simulations (
                id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                signal_id       BIGINT NOT NULL REFERENCES signals(id) ON DELETE CASCADE,
                level           TEXT NOT NULL,
                distance_value  DOUBLE PRECISION NOT NULL,
                distance_type   TEXT NOT NULL,
                anchor_price    DOUBLE PRECISION NOT NULL,
                high_water_mark DOUBLE PRECISION NOT NULL,
                hwm_updated_at  TIMESTAMPTZ,
                stopped_out     BOOLEAN DEFAULT FALSE,
                stop_price      DOUBLE PRECISION,
                stop_time       TIMESTAMPTZ,
                stop_reason     TEXT,
                pnl_at_stop     DOUBLE PRECISION,
                created_at      TIMESTAMPTZ DEFAULT NOW(),

                CONSTRAINT trailing_level_check
                    CHECK (level IN ('tight', 'medium', 'loose')),
                CONSTRAINT trailing_distance_type_check
                    CHECK (distance_type IN ('pips', 'dollars')),
                CONSTRAINT trailing_signal_level_unique
                    UNIQUE (signal_id, level)
            )
        """)

        await _create_indexes(conn)

        # Run migrations for any new columns added to existing tables
        await _run_migrations(conn)

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
        "CREATE INDEX IF NOT EXISTS idx_live_prices_updated ON live_prices(updated_at)",
        "CREATE INDEX IF NOT EXISTS idx_trailing_signal ON trailing_simulations(signal_id)",
        "CREATE INDEX IF NOT EXISTS idx_trailing_incomplete ON trailing_simulations(stopped_out) WHERE stopped_out = FALSE",
    ]

    for index_query in indexes:
        await conn.execute(index_query)

    logger.debug(f"Created {len(indexes)} database indexes")


async def _run_migrations(conn):
    """
    Run migrations to add new columns to existing tables.
    Uses IF NOT EXISTS pattern to be idempotent.
    """
    migrations = [
        # Add type column to signals table — replaces the legacy scalp boolean.
        # The scalp column is backfilled into type below (in Python) and then dropped.
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'signals' AND column_name = 'type'
            ) THEN
                ALTER TABLE signals ADD COLUMN type TEXT DEFAULT 'standard';
            END IF;
        END $$;
        """,
        # License system — per-user key allowance table
        """
        CREATE TABLE IF NOT EXISTS license_allowances (
            discord_id  TEXT PRIMARY KEY,
            max_keys    INTEGER NOT NULL DEFAULT 1
        );
        """,
        # License system — one row per issued license key
        # mt5_account is globally unique: one license per MT5 account
        """
        CREATE TABLE IF NOT EXISTS licenses (
            id           BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
            discord_id   TEXT NOT NULL,
            mt5_account  TEXT NOT NULL,
            license_key  TEXT NOT NULL,
            status       TEXT DEFAULT 'active',
            created_at   TIMESTAMPTZ DEFAULT NOW(),
            revoked_at   TIMESTAMPTZ,
            CONSTRAINT licenses_mt5_unique    UNIQUE (mt5_account),
            CONSTRAINT licenses_key_unique    UNIQUE (license_key),
            CONSTRAINT licenses_status_check  CHECK (status IN ('active', 'revoked'))
        );
        """,
        # Bot mode status — single-row table tracking active modes. news_mode is a
        # TEXT list of categories currently under news (e.g. 'EUR, GOLD' or 'ALL'),
        # NULL when no news. spread_hour stays a boolean. Updated in real-time.
        """
        CREATE TABLE IF NOT EXISTS bot_mode_status (
            id           INT PRIMARY KEY DEFAULT 1,
            news_mode    TEXT,
            spread_hour  BOOLEAN NOT NULL DEFAULT FALSE,
            updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            CONSTRAINT bot_mode_status_singleton CHECK (id = 1)
        );
        """,
        # Seed the single status row if it does not exist yet
        """
        INSERT INTO bot_mode_status (id, spread_hour)
        VALUES (1, FALSE)
        ON CONFLICT (id) DO NOTHING;
        """,
        # Migrate existing installs: news_mode BOOLEAN → TEXT. Consumers (incl. the
        # EX bot) read truthiness, so a non-empty string = active and NULL = inactive.
        """
        DO $$
        BEGIN
            IF (SELECT data_type FROM information_schema.columns
                WHERE table_name = 'bot_mode_status' AND column_name = 'news_mode') = 'boolean' THEN
                ALTER TABLE bot_mode_status ALTER COLUMN news_mode DROP DEFAULT;
                -- Drop NOT NULL before the type change: the USING clause turns a
                -- FALSE boolean into NULL, which the still-active constraint rejects.
                ALTER TABLE bot_mode_status ALTER COLUMN news_mode DROP NOT NULL;
                ALTER TABLE bot_mode_status ALTER COLUMN news_mode TYPE TEXT
                    USING (CASE WHEN news_mode THEN 'ALL' ELSE NULL END);
            END IF;
        END $$;
        """,
        # Add revoked_reason to licenses table (stage18 — auto-revoke tracking)
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'licenses' AND column_name = 'revoked_reason'
            ) THEN
                ALTER TABLE licenses ADD COLUMN revoked_reason TEXT;
            END IF;
        END $$;
        """,
        # Index for fast lookups (optional — single row, mostly a hint)
        """
        CREATE INDEX IF NOT EXISTS idx_bot_mode_status_updated ON bot_mode_status(updated_at);
        """,
        # ic_bid/ic_ask are obsolete — the execution bot now derives its broker
        # offset from its own MT5 feed against the stored price. Drop the columns.
        """
        ALTER TABLE live_prices DROP COLUMN IF EXISTS ic_bid;
        """,
        """
        ALTER TABLE live_prices DROP COLUMN IF EXISTS ic_ask;
        """,
        # M5: feed health table — written by FeedHealthMonitor each health check.
        # EX reads this in Phase 4.4 to skip placement on stale feeds.
        """
        CREATE TABLE IF NOT EXISTS feed_health (
            feed          TEXT PRIMARY KEY,
            status        TEXT NOT NULL,
            stale_seconds INTEGER,
            last_seen     TIMESTAMPTZ,
            updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """,
        # Replace result_pips (P&L value) with tp_price (market price at auto-TP trigger).
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'signals' AND column_name = 'tp_price'
            ) THEN
                ALTER TABLE signals ADD COLUMN tp_price DOUBLE PRECISION;
            END IF;
        END $$;
        """,
        """
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'signals' AND column_name = 'result_pips'
            ) THEN
                ALTER TABLE signals DROP COLUMN result_pips;
            END IF;
        END $$;
        """,
        # Persist Discord alert embed + ping references so restarts can reuse
        # the existing messages instead of orphaning them.
        """
        ALTER TABLE signals ADD COLUMN IF NOT EXISTS alert_message_id BIGINT;
        """,
        """
        ALTER TABLE signals ADD COLUMN IF NOT EXISTS alert_channel_id BIGINT;
        """,
        """
        ALTER TABLE signals ADD COLUMN IF NOT EXISTS ping_message_id BIGINT;
        """,
        # Persist the archived embed location so reply commands (e.g. reactivate
        # from finished-signals) survive a bot restart.
        """
        ALTER TABLE signals ADD COLUMN IF NOT EXISTS finished_message_id BIGINT;
        """,
        """
        ALTER TABLE signals ADD COLUMN IF NOT EXISTS finished_channel_id BIGINT;
        """,
    ]

    for migration in migrations:
        await conn.execute(migration)

    logger.debug(f"Ran {len(migrations)} database migrations")

    await _migrate_scalp_to_type(conn)


async def _migrate_scalp_to_type(conn):
    """
    One-shot backfill: derive signals.type from channel_id (via channels.json)
    and the legacy scalp boolean, then drop the scalp column.

    Idempotent — if the scalp column is already gone, this is a no-op.
    """
    scalp_exists = await conn.fetchval(
        """
        SELECT EXISTS(
            SELECT 1 FROM information_schema.columns
            WHERE table_name = 'signals' AND column_name = 'scalp'
        )
        """
    )
    if not scalp_exists:
        return

    try:
        channels_path = Path(__file__).resolve().parent.parent / "config" / "channels.json"
        channels = json.loads(channels_path.read_text(encoding="utf-8")).get(
            "monitored_channels", {}
        )
    except Exception as e:
        logger.warning(f"Could not load channels.json for type backfill: {e}")
        channels = {}

    # channel_id (str) → type
    id_to_type = {}
    for name, cid in channels.items():
        signal_type = _CHANNEL_NAME_TO_TYPE.get(name.lower())
        if signal_type:
            id_to_type[str(cid)] = signal_type

    for cid, signal_type in id_to_type.items():
        await conn.execute(
            """
            UPDATE signals
            SET type = $1
            WHERE channel_id = $2
              AND (type IS NULL OR type = 'standard')
            """,
            signal_type,
            cid,
        )

    # Fallback: any remaining rows that were marked scalp=true (e.g. text-detected
    # scalps in channels we don't classify) become type='scalp'.
    await conn.execute(
        """
        UPDATE signals
        SET type = 'scalp'
        WHERE scalp = TRUE AND (type IS NULL OR type = 'standard')
        """
    )

    # Add CHECK constraint once data is clean.
    await conn.execute(
        """
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.table_constraints
                WHERE constraint_name = 'signals_type_check'
                  AND table_name = 'signals'
            ) THEN
                ALTER TABLE signals
                ADD CONSTRAINT signals_type_check
                CHECK (type IN ('standard', 'scalp', 'swing', 'toll', 'pa', '1-1'));
            END IF;
        END $$;
        """
    )

    await conn.execute("ALTER TABLE signals DROP COLUMN scalp")
    logger.info("Migrated signals.scalp → signals.type and dropped scalp column")
