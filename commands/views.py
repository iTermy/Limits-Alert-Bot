"""
Discord UI views for command interactions.
"""

from typing import Dict, List, Optional

import discord

from utils.formatting import format_price, get_status_emoji


class ActiveSignalsView(discord.ui.View):
    """Pagination view for active signals"""

    def __init__(
        self,
        signals: List[Dict],
        guild_id: int,
        instrument: Optional[str],
        page_size: int = 10,
        timeout: int = 180,
    ):
        super().__init__(timeout=timeout)
        self.signals = signals
        self.guild_id = guild_id
        self.instrument = instrument
        self.page_size = page_size
        self.current_page = 0
        self.max_page = (len(signals) - 1) // page_size if signals else 0
        self.update_buttons()

    def update_buttons(self):
        self.previous_button.disabled = self.current_page <= 0
        self.next_button.disabled = self.current_page >= self.max_page
        self.page_label.label = f"Page {self.current_page + 1}/{self.max_page + 1}"

    def get_page_embed(self) -> discord.Embed:
        start_idx = self.current_page * self.page_size
        end_idx = min(start_idx + self.page_size, len(self.signals))
        page_signals = self.signals[start_idx:end_idx]

        return self.create_active_signals_embed(
            page_signals,
            self.guild_id,
            self.instrument,
            page_info=(self.current_page + 1, self.max_page + 1, len(self.signals)),
        )

    def create_active_signals_embed(
        self, signals: List[Dict], guild_id: int, instrument: Optional[str], page_info: tuple
    ) -> discord.Embed:
        current_page, total_pages, total_signals = page_info

        if not signals and current_page == 1:
            return discord.Embed(
                title="📊 Active Signals",
                description="No active signals found"
                + (f" for {instrument}" if instrument else ""),
                color=0xFFA500,
            )

        embed = discord.Embed(
            title="Active Signals",
            description=f"Showing page {current_page}/{total_pages} ({total_signals} total signals)"
            + (f" for {instrument}" if instrument else ""),
            color=0x00BFFF,
        )

        # Rows are presentation dicts from get_active_signals_detailed_sorted,
        # not SignalData models — pending_limits here is a list of floats.
        for row in signals:
            status = row.get("status", "active")
            status_emoji = get_status_emoji(status)

            pending_limits = row.get("pending_limits", [])
            hit_limits = row.get("hit_limits", [])

            if pending_limits:
                limits_str = ", ".join(
                    [format_price(p, row["instrument"]) for p in pending_limits]
                )
            else:
                limits_str = "None pending"

            if hit_limits:
                limits_str += f" | {len(hit_limits)} hit"

            if str(row["message_id"]).startswith("manual_"):
                link_label = "Manual Entry"
            else:
                message_url = f"https://discord.com/channels/{guild_id}/{row['channel_id']}/{row['message_id']}"
                link_label = f"{message_url}"

            field_value = f"**Limits:** {limits_str}"

            if row.get("distance_info") and status.lower() in ["active", "hit"]:
                distance_info = row["distance_info"]

                if row.get("is_crypto") or row.get("is_index"):
                    distance_dollars = abs(distance_info.get("distance", 0))
                    if distance_dollars > 0 and status.upper() != "HIT":
                        field_value += f"\n**Distance:** ${distance_dollars:.2f} away"
                else:
                    formatted_distance = distance_info.get("formatted", "")
                    if formatted_distance and status.upper() != "HIT":
                        field_value += f"\n**Distance:** {formatted_distance}"

            if row.get("time_remaining"):
                field_value += f"\n**Expiry:** {row['time_remaining']}"

            field_value += f"\n**Source:** {link_label}"

            embed.add_field(
                name=f"{status_emoji} #{row['id']} - {row['instrument']} - {row['direction'].upper()}",
                value=field_value,
                inline=False,
            )

        embed.set_footer(text=f"Total: {total_signals} signals | Use buttons to navigate")
        return embed

    @discord.ui.button(label="◀ Previous", style=discord.ButtonStyle.primary, custom_id="previous")
    async def previous_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page > 0:
            self.current_page -= 1
            self.update_buttons()
            await interaction.response.edit_message(embed=self.get_page_embed(), view=self)

    @discord.ui.button(
        label="Page 1/1", style=discord.ButtonStyle.secondary, custom_id="page_label", disabled=True
    )
    async def page_label(self, interaction: discord.Interaction, button: discord.ui.Button):
        pass

    @discord.ui.button(label="Next ▶", style=discord.ButtonStyle.primary, custom_id="next")
    async def next_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        if self.current_page < self.max_page:
            self.current_page += 1
            self.update_buttons()
            await interaction.response.edit_message(embed=self.get_page_embed(), view=self)
