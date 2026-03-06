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
            alert_system = (
                self.bot.monitor.alert_system
                if hasattr(self.bot, 'monitor') and self.bot.monitor else None
            )
            monitor = (
                self.bot.monitor
                if hasattr(self.bot, 'monitor') and self.bot.monitor else None
            )

            # ── 1. Query which signals are about to be expired BEFORE expiring ──
            # We need their IDs so we can do per-signal work afterward.
            about_to_expire = []
            if alert_system:
                try:
                    from database.models import SignalStatus
                    pre_expire_rows = await self.bot.signal_db.db.fetch_all(
                        """
                        SELECT id, message_id, channel_id
                        FROM signals
                        WHERE status IN ($1, $2)
                          AND expiry_time IS NOT NULL
                          AND expiry_time < CURRENT_TIMESTAMP
                        """,
                        (SignalStatus.ACTIVE, SignalStatus.HIT),
                    )
                    about_to_expire = list(pre_expire_rows) if pre_expire_rows else []
                except Exception as _pre:
                    self.logger.warning(f"Could not pre-fetch expiring signals: {_pre}")

            # ── 2. Run the DB expiry (sets status=cancelled, closed_reason='expiry') ──
            count = await self.bot.signal_db.expire_old_signals()
            if count > 0:
                self.logger.info(f"Expired {count} signals")

            # ── 3. Per-signal post-expiry cleanup ────────────────────────────
            for row in about_to_expire:
                sig_id = row['id']
                try:
                    await self._handle_expired_signal(sig_id, row, alert_system, monitor)
                except Exception as _e:
                    self.logger.warning(
                        f"Post-expiry cleanup failed for signal {sig_id}: {_e}"
                    )

        except Exception as e:
            self.logger.error(f"Error in expiry loop: {e}", exc_info=True)

    async def _handle_expired_signal(self, sig_id, row, alert_system, monitor):
        """
        Perform all post-expiry actions for a single signal:
          • Update / cancel the approaching alert embed
          • Schedule the 15-minute move to finished-signals channel
          • Add ❌ reaction to the original signal message
          • Delete the original message for gold-toll signals with no embed
        """
        # Fetch the fresh signal data (status is now 'cancelled', closed_reason='expiry')
        signal = await self.bot.signal_db.get_signal_with_limits(sig_id)
        if not signal:
            self.logger.warning(f"Could not fetch signal {sig_id} after expiry")
            return

        # Normalise key so alert_system methods work
        if "signal_id" not in signal:
            signal = dict(signal)
            signal["signal_id"] = signal.get("id", sig_id)

        # ── 3a. Update the persistent embed (approaching alert or hit embed) ──
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

                # Schedule move to finished-signals channel after 15 minutes
                alert_system._schedule_end_state_deletion(sig_id, event="expired")
                self.logger.debug(f"Scheduled archive move for expired signal {sig_id}")

            except Exception as _embed_err:
                self.logger.warning(
                    f"Could not update embed for expired signal {sig_id}: {_embed_err}"
                )
        elif alert_system:
            # No embed exists (signal expired before approaching alert was sent).
            # Still handle gold-toll original message deletion directly.
            src_channel_id = str(signal.get("channel_id", ""))
            src_message_id = str(signal.get("message_id", ""))
            if (
                src_channel_id in alert_system.toll_channel_ids
                and src_message_id
                and not src_message_id.startswith("manual_")
            ):
                await self._delete_original_message(src_channel_id, src_message_id, sig_id)

        # ── 3b. Add ❌ reaction to the original signal message ────────────────
        if monitor:
            try:
                await monitor._react_to_original_signal(signal, "❌")
            except Exception as _re:
                self.logger.warning(
                    f"Could not react to original message for expired signal {sig_id}: {_re}"
                )

    async def _delete_original_message(self, channel_id: str, message_id: str, sig_id: int):
        """Delete the original signal message (used for gold-toll signals with no embed)."""
        try:
            import discord
            channel = self.bot.get_channel(int(channel_id))
            if channel:
                try:
                    msg = await channel.fetch_message(int(message_id))
                    await msg.delete()
                    self.logger.info(
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