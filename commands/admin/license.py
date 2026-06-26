"""
License Commands - activate, setkeys, grantkey, revoke, licenses + member listeners
"""

import asyncio

import discord
from discord.ext import commands

from .._base import BaseCog


class LicenseCog(BaseCog):
    """License management commands and auto-revoke/reactivate listeners"""

    LICENSE_ROLE_NAME = "Signal Subscriber"
    LICENSE_DM_TIMEOUT = 120

    @staticmethod
    def _generate_license_key() -> str:
        import secrets

        return secrets.token_hex(16)

    @commands.command(name="activate")
    async def activate(self, ctx: commands.Context):
        """Get an Auto-Limits-Adder license key (requires Signal Subscriber role)"""
        try:
            await ctx.message.delete()
        except (discord.HTTPException, discord.Forbidden):
            pass

        member = ctx.author

        role_names = [r.name for r in getattr(member, "roles", [])]
        if not any(self.LICENSE_ROLE_NAME in r for r in role_names):
            try:
                await member.send(
                    f"❌ You need the **{self.LICENSE_ROLE_NAME}** role to activate a license.\n"
                    "If you are a subscriber, please contact an admin."
                )
            except discord.Forbidden:
                pass
            return

        try:
            await member.send(
                "👋 **Auto-Limits-Adder License Activation**\n\n"
                "To activate your license, please reply here with your **MT5 account number** "
                "(the login number shown in MetaTrader 5).\n\n"
                "_Type `cancel` at any time to abort._"
            )
        except discord.Forbidden:
            try:
                notice = await ctx.send(
                    f"❌ {member.mention} I can't DM you. "
                    "Please **enable DMs from server members** in your Privacy Settings, "
                    "then run `!activate` again."
                )
                await asyncio.sleep(20)
                await notice.delete()
            except discord.HTTPException:
                pass
            return

        def dm_check(m: discord.Message) -> bool:
            return m.author.id == member.id and isinstance(m.channel, discord.DMChannel)

        try:
            reply = await self.bot.wait_for(
                "message", check=dm_check, timeout=self.LICENSE_DM_TIMEOUT
            )
        except asyncio.TimeoutError:
            await member.send(
                f"⏰ Activation timed out after {self.LICENSE_DM_TIMEOUT}s. "
                "Run `!activate` in the server again when you're ready."
            )
            return

        if reply.content.strip().lower() == "cancel":
            await member.send("❌ Activation cancelled.")
            return

        mt5_account = reply.content.strip()
        if not mt5_account:
            await member.send("❌ No account number received. Please run `!activate` again.")
            return

        async with self.bot.signal_db.db.get_connection() as conn:
            existing_keys = await conn.fetch(
                "SELECT mt5_account, license_key FROM licenses WHERE discord_id = $1 AND status = 'active'",
                str(member.id),
            )
            max_keys = (
                await conn.fetchval(
                    "SELECT max_keys FROM license_allowances WHERE discord_id = $1",
                    str(member.id),
                )
                or 1
            )

            # Already registered this exact account → don't issue a duplicate key.
            if any(r["mt5_account"] == mt5_account for r in existing_keys):
                await member.send(
                    f"ℹ️ You already have an active license for MT5 account `{mt5_account}`.\n"
                    "If you've **lost your key**, ask an admin to re-issue it with `!grantkey`."
                )
                return

            # Block only when the user has used up their key allowance.
            if len(existing_keys) >= max_keys:
                accounts = ", ".join(f"`{r['mt5_account']}`" for r in existing_keys)
                await member.send(
                    f"ℹ️ You've reached your license limit ({len(existing_keys)}/{max_keys}) "
                    f"for MT5 account(s): {accounts}.\n\n"
                    "To register an **additional** MT5 account, ask an admin to raise your "
                    "limit with `!setkeys`.\n"
                    "If you've **lost a key**, ask an admin to re-issue it with `!grantkey`."
                )
                return

            existing_owner = await conn.fetchrow(
                "SELECT discord_id FROM licenses WHERE mt5_account = $1 AND status = 'active'",
                mt5_account,
            )
            if existing_owner:
                await member.send(
                    f"❌ MT5 account `{mt5_account}` is already registered to another user.\n"
                    "If you believe this is an error, please contact an admin."
                )
                return

            license_key = self._generate_license_key()
            await conn.execute(
                """
                INSERT INTO licenses (discord_id, mt5_account, license_key, status, created_at)
                VALUES ($1, $2, $3, 'active', NOW())
                """,
                str(member.id),
                mt5_account,
                license_key,
            )

        self.logger.info(f"License issued — user={member} ({member.id}), mt5={mt5_account}")
        await member.send(
            f"✅ **License activated!**\n\n"
            f"**Your license key:**\n```\n{license_key}\n```\n\n"
            f"Paste this key into the settings page of the Auto-Limits-Adder execution bot.\n\n"
            f"⚠️ Keep this key private. It is locked to MT5 account `{mt5_account}`."
        )

    @commands.command(name="setkeys")
    async def setkeys(self, ctx: commands.Context, member: discord.Member, max_keys: int):
        """Admin: set how many license keys a user may hold. Usage: !setkeys @user <n>"""
        if not self.is_admin(ctx.author):
            await ctx.send("❌ Admin only.")
            return
        if max_keys < 1:
            await ctx.send("❌ max_keys must be at least 1.")
            return

        async with self.bot.signal_db.db.get_connection() as conn:
            await conn.execute(
                """
                INSERT INTO license_allowances (discord_id, max_keys)
                VALUES ($1, $2)
                ON CONFLICT (discord_id) DO UPDATE SET max_keys = EXCLUDED.max_keys
                """,
                str(member.id),
                max_keys,
            )

        self.logger.info(f"Allowance updated — user={member} ({member.id}), max_keys={max_keys}")
        await ctx.send(
            f"✅ **{member.display_name}** can now hold up to **{max_keys}** license key(s)."
        )

    @commands.command(name="grantkey")
    async def grantkey(self, ctx: commands.Context, member: discord.Member, mt5_account: str):
        """Admin: issue a license key for a specific MT5 account. Usage: !grantkey @user <mt5_account>"""
        if not self.is_admin(ctx.author):
            await ctx.send("❌ Admin only.")
            return

        async with self.bot.signal_db.db.get_connection() as conn:
            existing = await conn.fetchrow(
                "SELECT discord_id FROM licenses WHERE mt5_account = $1 AND status = 'active'",
                mt5_account,
            )
            if existing:
                await ctx.send(
                    f"❌ MT5 account `{mt5_account}` is already registered "
                    f"to Discord user ID `{existing['discord_id']}`."
                )
                return

            active_count = await conn.fetchval(
                "SELECT COUNT(*) FROM licenses WHERE discord_id = $1 AND status = 'active'",
                str(member.id),
            )
            max_keys = (
                await conn.fetchval(
                    "SELECT max_keys FROM license_allowances WHERE discord_id = $1",
                    str(member.id),
                )
                or 1
            )

            if active_count >= max_keys:
                await ctx.send(
                    f"❌ {member.display_name} already has {active_count}/{max_keys} active license(s). "
                    f"Use `!setkeys @{member.display_name} {active_count + 1}` to raise their limit first."
                )
                return

            license_key = self._generate_license_key()
            await conn.execute(
                """
                INSERT INTO licenses (discord_id, mt5_account, license_key, status, created_at)
                VALUES ($1, $2, $3, 'active', NOW())
                """,
                str(member.id),
                mt5_account,
                license_key,
            )

        self.logger.info(
            f"License granted by admin — user={member} ({member.id}), mt5={mt5_account}, by={ctx.author}"
        )
        await ctx.send(
            f"✅ License issued for **{member.display_name}** (MT5: `{mt5_account}`). Key sent to their DMs."
        )

        try:
            await member.send(
                f"🔑 An admin has issued you an **Auto-Limits-Adder** license key.\n\n"
                f"**Your license key:**\n```\n{license_key}\n```\n\n"
                f"Paste this key into the settings page of the Auto-Limits-Adder execution bot.\n\n"
                f"This key is locked to MT5 account `{mt5_account}`. Keep it private."
            )
        except discord.Forbidden:
            await ctx.send(
                f"⚠️ Could not DM {member.display_name} — share the key manually:\n```\n{license_key}\n```"
            )

    @commands.command(name="revoke")
    async def revoke(self, ctx: commands.Context, member: discord.Member, mt5_account: str = None):
        """Admin: revoke a license. Usage: !revoke @user [mt5_account]"""
        if not self.is_admin(ctx.author):
            await ctx.send("❌ Admin only.")
            return

        async with self.bot.signal_db.db.get_connection() as conn:
            if mt5_account:
                row = await conn.fetchrow(
                    "SELECT id FROM licenses WHERE discord_id = $1 AND mt5_account = $2 AND status = 'active'",
                    str(member.id),
                    mt5_account,
                )
                if not row:
                    await ctx.send(
                        f"❌ No active license found for {member.display_name} with MT5 account `{mt5_account}`."
                    )
                    return
                await conn.execute(
                    "UPDATE licenses SET status = 'revoked', revoked_at = NOW() WHERE id = $1",
                    row["id"],
                )
                await ctx.send(
                    f"✅ License revoked for **{member.display_name}** (MT5: `{mt5_account}`)."
                )
            else:
                rows = await conn.fetch(
                    "SELECT id, mt5_account FROM licenses WHERE discord_id = $1 AND status = 'active'",
                    str(member.id),
                )
                if len(rows) == 0:
                    await ctx.send(f"❌ {member.display_name} has no active licenses.")
                    return
                if len(rows) > 1:
                    accounts = ", ".join(f"`{r['mt5_account']}`" for r in rows)
                    await ctx.send(
                        f"❌ {member.display_name} has {len(rows)} active licenses ({accounts}). "
                        f"Specify the MT5 account: `!revoke @user <mt5_account>`"
                    )
                    return
                row = rows[0]
                await conn.execute(
                    "UPDATE licenses SET status = 'revoked', revoked_at = NOW() WHERE id = $1",
                    row["id"],
                )
                await ctx.send(
                    f"✅ License revoked for **{member.display_name}** (MT5: `{row['mt5_account']}`)."
                )

        self.logger.info(
            f"License revoked — user={member} ({member.id}), mt5={mt5_account or 'auto'}, by={ctx.author}"
        )

    @commands.command(name="licenses")
    async def licenses(self, ctx: commands.Context, member: discord.Member = None):
        """Admin: list licenses. Usage: !licenses  OR  !licenses @user"""
        if not self.is_admin(ctx.author):
            await ctx.send("❌ Admin only.")
            return

        async with self.bot.signal_db.db.get_connection() as conn:
            if member:
                rows = await conn.fetch(
                    "SELECT mt5_account, license_key, status, created_at FROM licenses "
                    "WHERE discord_id = $1 ORDER BY created_at DESC",
                    str(member.id),
                )
                max_keys = (
                    await conn.fetchval(
                        "SELECT max_keys FROM license_allowances WHERE discord_id = $1",
                        str(member.id),
                    )
                    or 1
                )
                active_count = sum(1 for r in rows if r["status"] == "active")

                embed = discord.Embed(
                    title=f"🔑 Licenses — {member.display_name}",
                    description=f"Allowance: **{active_count}/{max_keys}** active keys",
                    color=discord.Color.blue(),
                )
                if not rows:
                    embed.add_field(name="No licenses found", value="\u200b", inline=False)
                for row in rows:
                    status_emoji = "✅" if row["status"] == "active" else "❌"
                    created = row["created_at"].strftime("%Y-%m-%d") if row["created_at"] else "?"
                    embed.add_field(
                        name=f"{status_emoji} MT5: `{row['mt5_account']}`",
                        value=f"Key: `{row['license_key'][:8]}...` | Created: {created} | {row['status']}",
                        inline=False,
                    )
                await ctx.send(embed=embed)

            else:
                rows = await conn.fetch(
                    """
                    SELECT l.discord_id, l.mt5_account, l.license_key, l.created_at,
                           COALESCE(la.max_keys, 1) AS max_keys
                    FROM licenses l
                    LEFT JOIN license_allowances la ON la.discord_id = l.discord_id
                    WHERE l.status = 'active'
                    ORDER BY l.discord_id, l.created_at
                    """
                )
                if not rows:
                    await ctx.send("ℹ️ No active licenses.")
                    return

                from collections import defaultdict

                grouped: dict = defaultdict(list)
                for row in rows:
                    grouped[row["discord_id"]].append(row)

                embed = discord.Embed(
                    title="🔑 All Active Licenses",
                    description=f"{len(rows)} license(s) across {len(grouped)} user(s)",
                    color=discord.Color.green(),
                )
                for discord_id, user_rows in grouped.items():
                    guild_member = ctx.guild.get_member(int(discord_id)) if ctx.guild else None
                    display_name = guild_member.display_name if guild_member else f"ID:{discord_id}"
                    max_k = user_rows[0]["max_keys"]
                    lines = [
                        f"• MT5 `{r['mt5_account']}` — key `{r['license_key'][:8]}...` — "
                        f"{r['created_at'].strftime('%Y-%m-%d') if r['created_at'] else '?'}"
                        for r in user_rows
                    ]
                    embed.add_field(
                        name=f"{display_name} ({len(user_rows)}/{max_k})",
                        value="\n".join(lines),
                        inline=False,
                    )
                    if len(embed.fields) >= 25:
                        embed.set_footer(
                            text="Showing first 25 users — use !licenses @user for details."
                        )
                        break

                await ctx.send(embed=embed)

    # ==================== LICENSE AUTO-MANAGEMENT ====================

    @commands.Cog.listener()
    async def on_member_update(self, before: discord.Member, after: discord.Member):
        """Auto-revoke licenses when Signal Subscriber role is removed."""
        before_roles = {r.name for r in before.roles}
        after_roles = {r.name for r in after.roles}

        had_role = self.LICENSE_ROLE_NAME in before_roles
        has_role = self.LICENSE_ROLE_NAME in after_roles

        if had_role == has_role:
            return

        if had_role and not has_role:
            await self._auto_revoke_licenses(after, reason="role_removed")
        elif not had_role and has_role:
            await self._auto_reactivate_licenses(after)

    @commands.Cog.listener()
    async def on_member_remove(self, member: discord.Member):
        """Auto-revoke licenses when a member leaves the server."""
        await self._auto_revoke_licenses(member, reason="left_server")

    async def _auto_revoke_licenses(self, member: discord.Member, reason: str) -> None:
        """Revoke all active licenses for *member* and notify them by DM."""
        try:
            async with self.bot.signal_db.db.get_connection() as conn:
                rows = await conn.fetch(
                    "SELECT id, mt5_account FROM licenses WHERE discord_id = $1 AND status = 'active'",
                    str(member.id),
                )
                if not rows:
                    return

                for row in rows:
                    await conn.execute(
                        """
                        UPDATE licenses
                        SET status = 'revoked', revoked_at = NOW(),
                            revoked_reason = $1
                        WHERE id = $2
                        """,
                        reason,
                        row["id"],
                    )

            self.logger.info(
                f"Auto-revoked {len(rows)} license(s) for {member} ({member.id}) — reason={reason}"
            )

            if reason == "role_removed":
                msg = (
                    "⚠️ **Your Auto-Limits-Adder license has been revoked.**\n\n"
                    f"Your **{self.LICENSE_ROLE_NAME}** role was removed, so your license key "
                    "has been automatically deactivated.\n\n"
                    "If you regain subscriber access your license will be reactivated automatically. "
                    "Contact an admin if you believe this is a mistake."
                )
            else:
                msg = (
                    "⚠️ **Your Auto-Limits-Adder license has been revoked.**\n\n"
                    "Your license was deactivated because you left the server. "
                    "If you rejoin and regain subscriber access, contact an admin to restore your license."
                )

            try:
                await member.send(msg)
            except discord.Forbidden:
                pass

        except Exception as e:
            self.logger.error(
                f"Failed to auto-revoke licenses for {member} ({member.id}): {e}", exc_info=True
            )

    async def _auto_reactivate_licenses(self, member: discord.Member) -> None:
        """Reactivate licenses previously revoked by the auto-revoke system."""
        try:
            async with self.bot.signal_db.db.get_connection() as conn:
                rows = await conn.fetch(
                    """
                    SELECT id, mt5_account, license_key
                    FROM licenses
                    WHERE discord_id = $1
                      AND status = 'revoked'
                      AND revoked_reason = 'role_removed'
                    ORDER BY revoked_at DESC
                    """,
                    str(member.id),
                )
                if not rows:
                    return

                for row in rows:
                    await conn.execute(
                        """
                        UPDATE licenses
                        SET status = 'active', revoked_at = NULL, revoked_reason = NULL
                        WHERE id = $1
                        """,
                        row["id"],
                    )

            self.logger.info(f"Auto-reactivated {len(rows)} license(s) for {member} ({member.id})")

            keys_info = "\n".join(
                f"• MT5 `{r['mt5_account']}` → key `{r['license_key'][:8]}…`" for r in rows
            )
            try:
                await member.send(
                    f"✅ **Your Auto-Limits-Adder license has been reactivated!**\n\n"
                    f"Your **{self.LICENSE_ROLE_NAME}** role was restored, so your license key(s) "
                    f"are active again:\n{keys_info}\n\n"
                    "No changes needed in your config — just (re)start the Auto-Limits-Adder bot."
                )
            except discord.Forbidden:
                pass

        except Exception as e:
            self.logger.error(
                f"Failed to auto-reactivate licenses for {member} ({member.id}): {e}", exc_info=True
            )
