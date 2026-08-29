import discord
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
            monitor = self.bot.services.monitor
            alert_system = self.bot.services.alert_system

            # Only the cancelled ids come back — a HIT signal rolls its expiry
            # forward and stays open, so none of the cleanup below applies to it.
            cancelled_ids = await self.bot.services.signal_db.expire_old_signals()

            for sig_id in cancelled_ids:
                try:
                    await self._handle_expired_signal(sig_id, alert_system, monitor)
                except Exception as _e:
                    self.logger.warning(f"Post-expiry cleanup failed for signal {sig_id}: {_e}")

        except Exception as e:
            self.logger.error(f"Error in expiry loop: {e}", exc_info=True)

    async def _handle_expired_signal(self, sig_id, alert_system, monitor):
        """
        Perform all post-expiry actions for a single signal:
          • Finalize the trailing / excursion analytics trackers
          • Update / cancel the approaching alert embed (which also schedules the
            15-minute move to finished-signals, since "expired" is an end state)
          • Add ❌ reaction to the original signal message
          • Delete the original message for gold-toll signals with no embed
        """
        # Fetch the fresh signal data (status is now 'cancelled', closed_reason='expiry')
        signal = await self.bot.services.signal_db.get_signal_with_limits(sig_id)
        if not signal:
            self.logger.warning(f"Could not fetch signal {sig_id} after expiry")
            return

        # ── a. Close out the analytics trackers at the expiry price ─────────
        if monitor:
            try:
                await monitor.finalize_trailing_on_manual_close(sig_id)
            except Exception as _fin:
                self.logger.warning(
                    f"Could not finalize trackers for expired signal {sig_id}: {_fin}"
                )

        # ── b. Update the persistent embed (approaching alert or hit embed) ──
        if alert_system and sig_id in alert_system.signal_messages:
            try:
                # Stop live price refresh for this signal
                alert_system._unregister_live_embed(sig_id)

                # Update the embed to show the expired/cancelled state
                await alert_system.update_signal_message(
                    signal=signal,
                    event="expired",
                    ping_text="⏰ Signal expired.",
                )
                self.logger.debug(f"Updated embed to expired for signal {sig_id}")

            except Exception as _embed_err:
                self.logger.warning(
                    f"Could not update embed for expired signal {sig_id}: {_embed_err}"
                )
        elif alert_system:
            # No embed exists (signal expired before approaching alert was sent).
            # Delete the original message in auto-purge channels.
            src_channel_id = str(signal.channel_id or "")
            src_message_id = str(signal.message_id or "")
            if (
                alert_system.is_auto_purge_channel(src_channel_id)
                and src_message_id
                and not src_message_id.startswith("manual_")
            ):
                await self._delete_original_message(src_channel_id, src_message_id, sig_id)

        # ── c. Add ❌ reaction to the original signal message ────────────────
        if monitor:
            try:
                from price_feeds.monitors.streaming_monitor import react_to_original_signal

                await react_to_original_signal(self.bot, signal, "❌")
            except Exception as _re:
                self.logger.warning(
                    f"Could not react to original message for expired signal {sig_id}: {_re}"
                )

    async def _delete_original_message(self, channel_id: str, message_id: str, sig_id: int):
        """Delete the original signal message (auto-purge channels with no embed)."""
        try:
            channel = self.bot.get_channel(int(channel_id))
            if channel:
                try:
                    msg = await channel.fetch_message(int(message_id))
                    await msg.delete()
                    self.logger.debug(
                        f"Deleted gold-tolls original message {message_id} "
                        f"for expired signal {sig_id}"
                    )
                except discord.NotFound:
                    pass
                except discord.Forbidden:
                    self.logger.warning(
                        f"No permission to delete gold-tolls message {message_id} "
                        f"for signal {sig_id}"
                    )
        except Exception as e:
            self.logger.warning(
                f"Could not delete gold-tolls original message for signal {sig_id}: {e}"
            )

    @check_expiry.before_loop
    async def before_check_expiry(self):
        """Wait until bot is ready before starting."""
        await self.bot.wait_until_ready()

    def stop(self):
        """Stop the expiry loop (optional for cleanup)."""
        self.check_expiry.cancel()
