"""
Trading Bot Core - Main bot class
"""

import traceback
from datetime import datetime
from typing import Optional, Set

import discord
from discord.ext import commands, tasks

from core.services import ServiceRegistry
from database import db, initialize_signal_db
from utils.config_loader import config, load_settings
from utils.logger import get_logger

logger = get_logger("bot")


class TradingBot(commands.Bot):
    """Main Discord bot class with modular architecture"""

    def __init__(self):
        # Set up intents
        intents = discord.Intents.default()
        intents.message_content = True
        intents.messages = True
        intents.guilds = True
        intents.reactions = True
        intents.members = (
            True  # Required for on_member_update / on_member_remove (license auto-management)
        )

        super().__init__(
            command_prefix="!",
            intents=intents,
            help_command=None,  # We have a custom help command
        )

        # Initialize attributes
        self.logger = get_logger("bot")
        self.start_time = datetime.utcnow()
        self.channels_config = None
        self.monitored_channels: Set[int] = set()
        self.allowed_channel_ids: Set[int] = set()
        self.alert_channel_id: Optional[int] = None
        self.command_channel_id: Optional[int] = None
        self.signal_db = None
        self.message_handler = None
        self.expiry_manager = None
        self.monitor = None
        self.channel_cleaner = None
        self.news_fetcher = None

        # Flat service registry — populated during setup_hook, injected into cogs/handlers
        self.services = ServiceRegistry()

        # News mode manager — tracks active news windows
        from core.news_manager import NewsManager

        self.news_manager = NewsManager()
        self.news_manager.load_from_file()
        self.news_manager.start_cleanup_task()

        # Admin user IDs from settings.json
        bot_settings = load_settings()
        self.admin_ids = bot_settings.admin_ids

    async def setup_hook(self):
        """Called when bot is getting ready"""
        self.logger.info("Starting bot setup...")

        # Initialize database FIRST
        await db.initialize()
        self.signal_db = initialize_signal_db(db)

        # Populate service registry with DB layer
        self.services.db = db
        self.services.signal_db = self.signal_db

        # Give NewsManager a reference to the DB so it can persist mode status,
        # then sync the news_mode column to the events loaded from disk (corrects
        # any stale value a previous crash may have left behind).
        self.news_manager.set_db(db)
        await self.news_manager.reconcile_news_mode()

        # Load channel configuration SECOND
        await self.load_config()

        from discord_handlers.message_handler import MessageHandler

        self.message_handler = MessageHandler(self)
        self.logger.info("Message handler initialized")

        await self.initialize_price_monitor()

        # Populate service registry with monitor subsystems
        if self.monitor:
            self.services.monitor = self.monitor
            self.services.alert_system = self.monitor.alert_system
            self.services.stream_manager = self.monitor.stream_manager
            self.services.tp_config = self.monitor.tp_config
            self.services.tp_monitor = self.monitor.tp_monitor
            self.services.nm_config = self.monitor.nm_config
            self.services.nm_monitor = self.monitor.nm_monitor
            self.services.alert_config = self.monitor.alert_config
            self.services.trailing_monitor = self.monitor.trailing_monitor

        # Connect alert system to message handler
        if self.monitor and self.message_handler:
            self.message_handler.alert_system = self.monitor.alert_system
            self.logger.info("Connected alert system to message handler")
        else:
            self.logger.error("Failed to connect alert system - monitor or message_handler is None")

        # Start expiry manager
        from core.expiry_manager import ExpiryManager

        self.expiry_manager = ExpiryManager(self)

        from core.channel_cleaner import ChannelCleaner

        self.channel_cleaner = ChannelCleaner(self)

        await self.load_extensions()

        # Start background tasks
        self.heartbeat.start()

        self.logger.info("Bot setup completed with modular architecture")

    async def load_config(self):
        """Load configuration from files"""
        self.channels_config = config.load("channels.json")

        # Set up monitored channels
        self.monitored_channels.clear()
        for channel_name, channel_id in self.channels_config.get("monitored_channels", {}).items():
            if channel_id:
                self.monitored_channels.add(int(channel_id))
                self.logger.info(f"Monitoring channel: {channel_name} ({channel_id})")

        # Set alert channel
        alert_id = self.channels_config.get("alert_channel")
        if alert_id:
            self.alert_channel_id = int(alert_id)
            self.logger.info(f"Alert channel set: {alert_id}")

        # Set command channel
        command_id = self.channels_config.get("command_channel")
        if command_id:
            self.command_channel_id = int(command_id)
            self.logger.info(f"Command channel set: {command_id}")

        # Build allowed_channel_ids once — read by on_message and MessageHandler
        self.allowed_channel_ids = set(self.monitored_channels)
        for key in (
            "alert_channel",
            "command_channel",
            "pa-alert-channel",
            "toll-alert-channel",
            "general-tolls-alert",
            "legends-trade-alert",
            "finished_signals",
            "profit_channel",
        ):
            val = self.channels_config.get(key)
            if val:
                self.allowed_channel_ids.add(int(val))

    async def load_extensions(self):
        """Load all command cogs"""
        extensions = [
            "commands.admin",
            "commands.signals",
            "commands.config",
        ]

        for extension in extensions:
            try:
                await self.load_extension(extension)
                self.logger.info(f"Loaded extension: {extension}")
            except Exception as e:
                self.logger.error(f"Failed to load extension {extension}: {e}")

    async def on_ready(self):
        """Called when bot is fully ready"""
        self.logger.info(f"Bot logged in as {self.user.name} ({self.user.id})")
        self.logger.info(f"Connected to {len(self.guilds)} guild(s)")

        # Ensure alert system connection is valid (guard against race conditions)
        if self.monitor and self.message_handler and not self.message_handler.alert_system:
            self.message_handler.alert_system = self.monitor.alert_system
            self.logger.info("Re-connected alert system in on_ready")

        # Start live embed price update loop
        if self.monitor and self.monitor.alert_system:
            self.monitor.alert_system.bot = self
            self.monitor.alert_system.start_live_updates()
            self.logger.info("Live embed update loop started")

        # Set bot status
        await self.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.watching, name="for trading signals"
            )
        )

    async def on_message(self, message: discord.Message):
        """Handle new messages"""
        # Only process messages in allowed channels
        if message.channel.id not in self.allowed_channel_ids:
            return

        # Let message handler process it first
        try:
            await self.message_handler.handle_new_message(message)
        except discord.NotFound:
            self.logger.debug(f"Message {message.id} was deleted during processing")
        except Exception as e:
            self.logger.error(
                f"Error in message handler for message {message.id}: {str(e)!r}",
                exc_info=True,
            )

        # Then process commands (only in allowed channels)
        try:
            await self.process_commands(message)
        except Exception as e:
            self.logger.error(
                f"Error processing commands for message {message.id}: {str(e)!r}", exc_info=True
            )

    async def on_raw_message_edit(self, payload: discord.RawMessageUpdateEvent):
        """Handle message edits, including messages edited after a restart.

        The cached ``on_message_edit`` only fires for messages received since the
        process started; an edit to an older signal would be silently missed. The
        raw event fires for every edit, so we filter to monitored channels (cheap,
        avoids fetching for the many edits the bot makes in alert channels), fetch
        the current message, and hand it to the same handler.
        """
        if not self.message_handler:
            return

        if payload.channel_id not in self.monitored_channels:
            return

        channel = self.get_channel(payload.channel_id)
        if channel is None:
            try:
                channel = await self.fetch_channel(payload.channel_id)
            except (discord.NotFound, discord.Forbidden):
                return

        try:
            message = await channel.fetch_message(payload.message_id)
        except (discord.NotFound, discord.Forbidden):
            return

        await self.message_handler.handle_message_edit(message)

    async def on_raw_message_delete(self, payload: discord.RawMessageDeleteEvent):
        """Handle message deletions"""
        if self.message_handler:
            await self.message_handler.handle_message_delete(payload)

    async def on_command_error(self, ctx: commands.Context, error: commands.CommandError):
        """Handle command errors"""
        if isinstance(error, commands.CommandNotFound):
            if self.command_channel_id and ctx.channel.id == self.command_channel_id:
                await ctx.send("❌ Unknown command. Use `!help` to see available commands.")
            return

        if isinstance(error, commands.MissingPermissions):
            await ctx.send("❌ You don't have permission to use this command.")
            return

        if isinstance(error, commands.CheckFailure):
            # This is handled by individual cogs
            return

        self.logger.error(f"Command error: {error}")

        # Send generic error message
        embed = discord.Embed(
            title="❌ Command Error", description=str(error), color=discord.Color.red()
        )
        await ctx.send(embed=embed)

    async def initialize_price_monitor(self):
        """Initialize the price monitoring system — creates all subsystems and injects them."""
        self.logger.info("Starting price monitor initialization...")
        try:
            from price_feeds.alert_config import AlertDistanceConfig
            from price_feeds.alert_system import AlertSystem
            from price_feeds.feed_health_monitor import FeedHealthMonitor
            from price_feeds.live_price_writer import LivePriceWriter
            from price_feeds.nm_config import NMConfig
            from price_feeds.nm_monitor import NearMissMonitor
            from price_feeds.price_stream_manager import PriceStreamManager
            from price_feeds.streaming_monitor import StreamingPriceMonitor
            from price_feeds.tp_config import TPConfig
            from price_feeds.tp_monitor import AutoTPMonitor
            from price_feeds.trailing_config import TrailingStopConfig
            from price_feeds.trailing_monitor import TrailingStopMonitor

            from database.trailing_ops import TrailingDatabase

            # Create subsystems
            alert_config = AlertDistanceConfig()
            stream_manager = PriceStreamManager()
            alert_system = AlertSystem(
                bot=self, stream_manager=stream_manager, alert_config=alert_config
            )
            tp_config = TPConfig()
            tp_monitor = AutoTPMonitor(
                tp_config=tp_config,
                signal_db=self.signal_db,
                db=db,
                alert_system=alert_system,
            )
            nm_config = NMConfig()
            nm_monitor = NearMissMonitor(
                nm_config=nm_config,
                signal_db=self.signal_db,
                db=db,
                alert_system=alert_system,
            )
            trailing_config = TrailingStopConfig(tp_config=tp_config)
            trailing_monitor = TrailingStopMonitor(
                trailing_config=trailing_config,
                trailing_db=TrailingDatabase(db),
            )
            live_price_writer = LivePriceWriter(
                db_manager=db,
                stream_manager=stream_manager,
            )

            bot_settings = load_settings()
            health_monitor = FeedHealthMonitor(
                stream_manager=stream_manager,
                bot=self,
                admin_user_id=bot_settings.health_alert_admin_id,
                us_market_holidays=bot_settings.us_market_holidays,
                db=db,
            )

            # Wire alert channels before creating the monitor
            await self._setup_alert_channels(alert_system)

            # Create monitor with all dependencies injected
            self.monitor = StreamingPriceMonitor(
                bot=self,
                signal_db=self.signal_db,
                db=db,
                alert_system=alert_system,
                stream_manager=stream_manager,
                alert_config=alert_config,
                tp_config=tp_config,
                tp_monitor=tp_monitor,
                nm_config=nm_config,
                nm_monitor=nm_monitor,
                trailing_monitor=trailing_monitor,
                live_price_writer=live_price_writer,
                health_monitor=health_monitor,
            )

            # Initialize and start monitoring
            await self.monitor.initialize()
            await self.monitor.start()

            # Re-start the news manager cleanup task now that we have an alert system
            self.news_manager.start_cleanup_task(alert_system=alert_system)

            # Start the ForexFactory news fetcher (auto-populates news windows).
            # Started here so newly-added windows get activation alerts via the
            # cleanup loop above.
            from core.news_fetcher import NewsFetcher

            self.news_fetcher = NewsFetcher(self.news_manager, bot_settings.news_autofetch)
            self.services.news_fetcher = self.news_fetcher
            if bot_settings.news_autofetch.enabled:
                self.news_fetcher.start()
                self.logger.info("ForexFactory news fetcher started")

            self.logger.info("Price monitoring system initialized and started")
            self.logger.info(
                f"Alert system created with {len(alert_system.alert_messages)} tracked messages"
            )

        except Exception as e:
            self.logger.error(f"Failed to initialize price monitor: {e}", exc_info=True)
            # Don't fail bot startup if monitor fails
            self.monitor = None

    async def _setup_alert_channels(self, alert_system):
        """Fetch Discord channels and wire them into the alert system."""
        channel_setters = {
            "alert_channel": alert_system.set_channel,
            "pa-alert-channel": alert_system.set_pa_channel,
            "toll-alert-channel": alert_system.set_toll_channel,
            "general-tolls-alert": alert_system.set_general_toll_channel,
            "legends-trade-alert": alert_system.set_legends_channel,
        }
        for key, setter in channel_setters.items():
            channel_id = self.channels_config.get(key)
            if channel_id:
                try:
                    channel = await self.fetch_channel(int(channel_id))
                    setter(channel)
                    self.logger.info(f"{key} channel set: #{channel.name}")
                except Exception as e:
                    self.logger.error(f"Failed to set {key} channel: {e}")
            else:
                self.logger.warning(f"No {key} configured in channels.json")

    @tasks.loop(seconds=30)
    async def heartbeat(self):
        """Periodic heartbeat for monitoring"""
        self.logger.debug("Heartbeat - Bot is running")

    @heartbeat.before_loop
    async def before_heartbeat(self):
        """Wait for bot to be ready before starting heartbeat"""
        await self.wait_until_ready()

    async def close(self):
        """Cleanup when bot shuts down"""
        # Log the call stack so we can see what triggered the shutdown — the
        # trigger (e.g. a fatal gateway close inside discord.py) is otherwise
        # invisible, leaving only feed teardown errors in the log.
        self.logger.warning(
            "Shutting down bot... close() called from:\n%s",
            "".join(traceback.format_stack()),
        )

        # Cancel background tasks
        self.heartbeat.cancel()

        if self.expiry_manager:
            self.expiry_manager.stop()

        if self.channel_cleaner:
            self.channel_cleaner.stop()

        if self.news_manager:
            self.news_manager.stop_cleanup_task()

        if self.news_fetcher:
            self.news_fetcher.stop()

        if self.monitor:
            await self.monitor.stop()

        # Close database connection
        if db:
            await db.close()

        # Call parent close
        await super().close()
