"""Authorization helpers for signal management.

A single source of truth for "who is allowed to manage signals" — used by both
the prefix-command cogs and the reply-based message handler so the rule can't
drift between the two entry points.
"""


def is_admin(bot, user) -> bool:
    """True if the user is a configured bot admin or a Discord server
    administrator. Works for both guild members and DM users (who have no
    guild_permissions)."""
    if user.id in bot.admin_ids:
        return True
    perms = getattr(user, "guild_permissions", None)
    return perms is not None and perms.administrator


def is_signal_manager(bot, user) -> bool:
    """True if the user may manage signals (cancel / profit / news / etc.).

    Authorized when the user is a configured bot admin, a Discord server
    administrator, a configured manager user ID, or a member holding any of the
    configured manager role IDs. Everyone else is read-only.
    """
    if is_admin(bot, user):
        return True
    if user.id in getattr(bot, "signal_manager_user_ids", set()):
        return True

    role_ids = getattr(bot, "signal_manager_role_ids", set())
    if role_ids:
        user_roles = getattr(user, "roles", None)
        if user_roles and any(role.id in role_ids for role in user_roles):
            return True

    return False
