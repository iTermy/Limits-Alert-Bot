"""
Trading Bot Core - Main bot class with modular architecture
Fixed to properly connect alert system to message handler
"""
import discord
from discord.ext import commands, tasks
from typing import Optional, Set
from utils.logger import get_logger
from utils.config_loader import config
from database import db
from database import initialize_signal_db
from utils.logger import get_logger

logger = get_logger('bot')


class TradingBot(commands.Bot):
    """Main Discord bot class with modular architecture"""

    def __init__(self):
        # Bot configuration
        settings = config.load("settings.json")

        # Set up intents
        intents = discord.Intents.default()
        intents.message_content = True
        intents.messages = True
        intents.guilds = True
        intents.reactions = True

        super().__init__(
            command_prefix=settings.get("bot_prefix", "!"),
            intents=intents,
            help_command=None  # We have a custom help command
        )

        # Initialize attributes
        self.logger = get_logger("bot")
        self.settings = settings
        self.channels_config = None
        self.monitored_channels: Set[int] = set()
        self.alert_channel_id: Optional[int] = None
        self.command_channel_id: Optional[int] = None
        self.signal_db = None
        self.message_handler = None
        self.expiry_manager = None
        self.monitor = None  # Changed from price_monitor to monitor for consistency

        # Admin user IDs
        self.admin_ids = [582358569542877184]  # Replace with actual admin IDs

    async def setup_hook(self):
        """Called when bot is getting ready"""
        self.logger.info("Starting bot setup...")

        # Initialize database FIRST
        await db.initialize()
        self.signal_db = initialize_signal_db(db)

        # Load channel configuration SECOND
        await self.load_config()

        # Initialize message handler THIRD (before monitor)
        from discord_handlers.message_handler import MessageHandler
        self.message_handler = MessageHandler(self)
        self.logger.info("Message handler initialized")

        # Initialize price monitoring FOURTH (after message handler)
        await self.initialize_price_monitor()

        # CRITICAL: Connect alert system to message handler
        if self.monitor and self.message_handler:
            self.message_handler.alert_system = self.monitor.alert_system
            self.logger.info(f"Connected alert system to message handler")
            self.logger.info(f"Alert system is tracking {len(self.monitor.alert_system.alert_messages)} messages")
        else:
            self.logger.error("Failed to connect alert system - monitor or message_handler is None")

        # Start expiry manager
        from core.expiry_manager import ExpiryManager
        self.expiry_manager = ExpiryManager(self)

        # Load command extensions
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

    async def load_extensions(self):
        """Load all command cogs"""
        extensions = [
            'commands.general_commands',
            'commands.signal_commands',
            'commands.admin_commands',
            'commands.feed_commands'
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

        # Double-check the connection after everything is ready
        if self.monitor and self.message_handler:
            # Ensure connection is still valid
            if not self.message_handler.alert_system:
                self.message_handler.alert_system = self.monitor.alert_system
                self.logger.info("Re-connected alert system in on_ready")

            # Log status
            tracked_count = len(self.monitor.alert_system.alert_messages) if self.monitor.alert_system else 0
            self.logger.info(f"Alert system status: {tracked_count} tracked messages")

        # Set bot status
        await self.change_presence(
            activity=discord.Activity(
                type=discord.ActivityType.watching,
                name="for trading signals"
            )
        )

    async def on_message(self, message: discord.Message):
        """Handle new messages"""
        # Let message handler process it first
        if self.message_handler:
            await self.message_handler.handle_new_message(message)

        # Then process commands
        await self.process_commands(message)

    async def on_message_edit(self, before: discord.Message, after: discord.Message):
        """Handle message edits"""
        if self.message_handler:
            await self.message_handler.handle_message_edit(before, after)

    async def on_raw_message_delete(self, payload: discord.RawMessageDeleteEvent):
        """Handle message deletions"""
        if self.message_handler:
            await self.message_handler.handle_message_delete(payload)

    async def on_command_error(self, ctx: commands.Context, error: commands.CommandError):
        """Handle command errors"""
        if isinstance(error, commands.CommandNotFound):
            return  # Ignore unknown commands

        if isinstance(error, commands.MissingPermissions):
            await ctx.send("❌ You don't have permission to use this command.")
            return

        if isinstance(error, commands.CheckFailure):
            # This is handled by individual cogs
            return

        self.logger.error(f"Command error: {error}")

        # Send generic error message
        embed = discord.Embed(
            title="❌ Command Error",
            description=str(error),
            color=discord.Color.red()
        )
        await ctx.send(embed=embed)

    async def initialize_price_monitor(self):
        """Initialize the price monitoring system"""
        self.logger.info("Starting price monitor initialization...")
        try:
            from price_feeds.monitor import PriceMonitor

            # Create monitor instance - use self.monitor not self.price_monitor
            self.monitor = PriceMonitor(
                bot=self,
                signal_db=self.signal_db,
                db=db
            )

            # Initialize and start monitoring
            await self.monitor.initialize()
            await self.monitor.start()

            self.logger.info("Price monitoring system initialized and started")
            self.logger.info(f"Alert system created with {len(self.monitor.alert_system.alert_messages)} tracked messages")

        except Exception as e:
            self.logger.error(f"Failed to initialize price monitor: {e}", exc_info=True)
            # Don't fail bot startup if monitor fails
            self.monitor = None

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
        self.logger.info("Shutting down bot...")

        # Cancel background tasks
        self.heartbeat.cancel()

        if self.monitor:
            await self.monitor.stop()

        # Close database connection
        if db:
            await db.close()

        # Call parent close
        await super().close()