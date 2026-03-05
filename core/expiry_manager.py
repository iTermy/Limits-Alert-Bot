import asyncio
from discord.ext import tasks
from utils.logger import get_logger


class ExpiryManager:
    """Manages automatic expiry of trading signals."""

    def __init__(self, bot):
        self.bot = bot
        self.logger = get_logger("expiry_manager")

        # Start background loop
        self.check_expiry.start()

    @tasks.loop(minutes=5)
    async def check_expiry(self):
        """Check and expire signals past expiry_time."""
        try:
            # Get the list of signal IDs that are about to be expired so we can update embeds
            alert_system = (
                self.bot.monitor.alert_system
                if hasattr(self.bot, 'monitor') and self.bot.monitor else None
            )

            # Collect IDs with persistent embeds before expiring
            ids_to_update = []
            if alert_system:
                ids_to_update = [
                    sid for sid in alert_system.signal_messages.keys()
                ]

            count = await self.bot.signal_db.expire_old_signals()
            if count > 0:
                self.logger.info(f"Expired {count} signals")

                # Update embeds for any expired signals that had persistent messages
                if alert_system and ids_to_update:
                    monitor = self.bot.monitor if hasattr(self.bot, 'monitor') and self.bot.monitor else None
                    for sig_id in ids_to_update:
                        try:
                            signal = await self.bot.signal_db.get_signal_with_limits(sig_id)
                            if signal and signal.get('status') == 'cancelled' and signal.get('closed_reason') == 'automatic':
                                await alert_system.update_embed_for_signal_id(
                                    sig_id,
                                    'expired',
                                    ping_text="Signal expired."
                                )
                                # React to the original signal message with ❌
                                if monitor:
                                    try:
                                        await monitor._react_to_original_signal(signal, "❌")
                                    except Exception as _re:
                                        self.logger.warning(f"Could not react to original message for expired signal {sig_id}: {_re}")
                        except Exception as _ue:
                            self.logger.warning(f"Could not update embed after expiry for signal {sig_id}: {_ue}")

        except Exception as e:
            self.logger.error(f"Error in expiry loop: {e}")

    @check_expiry.before_loop
    async def before_check_expiry(self):
        """Wait until bot is ready before starting."""
        await self.bot.wait_until_ready()

    def stop(self):
        """Stop the expiry loop (optional for cleanup)."""
        self.check_expiry.cancel()