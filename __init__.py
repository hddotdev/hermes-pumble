"""Pumble platform plugin for Hermes Agent."""

from .adapter import PumbleAdapter, check_requirements, validate_config, env_enablement


def register(ctx):
    ctx.register_platform(
        name="pumble",
        label="Pumble",
        adapter_factory=lambda cfg: PumbleAdapter(cfg),
        check_fn=check_requirements,
        validate_config=validate_config,
        env_enablement_fn=env_enablement,
        cron_deliver_env_var="PUMBLE_HOME_CHANNEL",
        allowed_users_env="PUMBLE_ALLOWED_USERS",
        allow_all_env="PUMBLE_ALLOW_ALL_USERS",
        max_message_length=10000,
        platform_hint=(
            "You are chatting via Pumble. Use concise markdown; the adapter "
            "polls configured channels and posts replies back to Pumble."
        ),
        emoji="💬",
    )
