"""Polling Pumble gateway adapter.

Pumble's API Key addon exposes REST reads/writes but no inbound event stream.
This adapter therefore polls configured channels and deduplicates message IDs.
"""

from __future__ import annotations

import asyncio
import datetime as dt
import logging
import os
import time
from typing import Any

import httpx

from gateway.config import Platform
from gateway.platforms.base import BasePlatformAdapter, MessageEvent, MessageType, SendResult

logger = logging.getLogger(__name__)
DEFAULT_API_URL = "https://pumble-api-keys.addons.marketplace.cake.com"


def _truthy(name: str, default: bool = False) -> bool:
    value = os.getenv(name)
    return default if value is None else value.strip().lower() in {"1", "true", "yes", "on"}


def _csv(name: str) -> list[str]:
    return [item.strip() for item in os.getenv(name, "").split(",") if item.strip()]


def _get_secret(name: str) -> str:
    # Keep compatibility with Hermes profile-scoped secrets while allowing
    # this standalone plugin to run in a minimal test environment.
    try:
        from agent.secret_scope import get_secret
        value = get_secret(name, None)
        if value:
            return value
    except Exception:
        pass
    return os.getenv(name, "")


def _items(payload: Any, *keys: str) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        values = payload
    elif isinstance(payload, dict):
        values = []
        for key in keys:
            value = payload.get(key)
            if isinstance(value, list):
                values = value
                break
    else:
        values = []

    # Pumble's listChannels endpoint returns wrappers such as
    # {"channel": {...}, "users": [...]}; other endpoints return flat
    # objects. Normalize both forms at the API boundary.
    result: list[dict[str, Any]] = []
    for item in values:
        if not isinstance(item, dict):
            continue
        nested = item.get("channel") or item.get("message")
        result.append(nested if isinstance(nested, dict) else item)
    return result


def _first(data: dict[str, Any], *keys: str, default: Any = "") -> Any:
    for key in keys:
        value = data.get(key)
        if value is not None and value != "":
            return value
    return default


def _message_text(message: dict[str, Any]) -> str:
    return str(_first(message, "text", "message", "content", "body", default=""))


def _message_id(message: dict[str, Any]) -> str:
    value = _first(message, "id", "messageId", "message_id", default="")
    return str(value) if value else ""


def _author_id(message: dict[str, Any]) -> str:
    author = message.get("user") or message.get("author") or message.get("sender")
    if isinstance(author, dict):
        return str(_first(author, "id", "userId", "user_id", default=""))
    return str(_first(message, "userId", "user_id", "authorId", "senderId", default=""))


def _author_name(message: dict[str, Any]) -> str:
    author = message.get("user") or message.get("author") or message.get("sender")
    if isinstance(author, dict):
        return str(_first(author, "name", "displayName", "username", "email", default=""))
    return str(_first(message, "userName", "authorName", "senderName", default=""))


def _created_at(message: dict[str, Any]) -> dt.datetime:
    value = _first(message, "createdAt", "created_at", "timestamp", "sentAt", default="")
    if isinstance(value, (int, float)):
        return dt.datetime.fromtimestamp(value / 1000 if value > 10_000_000_000 else value, tz=dt.timezone.utc)
    if isinstance(value, str):
        try:
            return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            pass
    return dt.datetime.now(dt.timezone.utc)


class PumbleClient:
    def __init__(self, api_key: str, base_url: str = DEFAULT_API_URL, client: httpx.AsyncClient | None = None):
        self.base_url = base_url.rstrip("/")
        self.client = client or httpx.AsyncClient(timeout=30.0)
        self._owns_client = client is None
        self.headers = {"ApiKey": api_key, "Accept": "application/json"}

    async def close(self) -> None:
        if self._owns_client:
            await self.client.aclose()

    async def request(self, method: str, path: str, **kwargs: Any) -> Any:
        response = await self.client.request(method, f"{self.base_url}/{path.lstrip('/')}", headers=self.headers, **kwargs)
        response.raise_for_status()
        if not response.content:
            return {}
        return response.json()

    async def list_channels(self) -> list[dict[str, Any]]:
        return _items(await self.request("GET", "/listChannels"), "channels", "data")

    async def list_messages(self, channel_id: str, limit: int = 100) -> list[dict[str, Any]]:
        payload = await self.request("GET", "/listMessages", params={"channelId": channel_id, "limit": limit})
        return _items(payload, "messages", "data")

    async def send_message(self, channel_id: str, text: str) -> str:
        payload = await self.request("POST", "/sendMessage", json={"channelId": channel_id, "text": text, "asBot": False})
        if isinstance(payload, dict):
            return str(_first(payload, "id", "messageId", default=""))
        return ""

    async def my_info(self) -> dict[str, Any]:
        payload = await self.request("GET", "/myInfo")
        return payload if isinstance(payload, dict) else {}


class PumbleAdapter(BasePlatformAdapter):
    """Hermes adapter for Pumble's API Key addon."""

    def __init__(self, config):
        super().__init__(config=config, platform=Platform("pumble"))
        extra = getattr(config, "extra", {}) or {}
        self.api_key = _get_secret("PUMBLE_API_KEY") or str(extra.get("api_key", ""))
        self.base_url = os.getenv("PUMBLE_API_URL") or str(extra.get("api_url", DEFAULT_API_URL))
        self.channel_filter = set(_csv("PUMBLE_CHANNELS"))
        self.include_dms = _truthy("PUMBLE_INCLUDE_DMS", False)
        self.require_mention = _truthy("PUMBLE_REQUIRE_MENTION", False)
        self.mention = os.getenv("PUMBLE_MENTION", "@hermes").lower()
        self.poll_seconds = max(2.0, float(os.getenv("PUMBLE_POLL_SECONDS", "5")))
        self.user_id = os.getenv("PUMBLE_USER_ID", "")
        self.client: PumbleClient | None = None
        self._poll_task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._seen: set[str] = set()
        self._channels: dict[str, dict[str, Any]] = {}
        self._bootstrapped = False

    async def connect(self, *, is_reconnect: bool = False) -> bool:
        if not self.api_key:
            logger.error("Pumble: PUMBLE_API_KEY is required")
            return False
        self.client = PumbleClient(self.api_key, self.base_url)
        try:
            channels = await self.client.list_channels()
            self._channels = {self._channel_id(c): c for c in channels if self._channel_id(c)}
            if not self.user_id:
                try:
                    info = await self.client.my_info()
                    self.user_id = str(_first(info, "id", "userId", "user_id", default=""))
                except Exception:
                    logger.debug("Pumble: /myInfo unavailable; set PUMBLE_USER_ID to suppress own messages", exc_info=True)
            await self._bootstrap_seen()
            self._stop.clear()
            self._poll_task = asyncio.create_task(self._poll_loop())
            self._mark_connected()
            logger.info("Pumble: connected; monitoring %d channel(s)", len(self._channels_to_poll()))
            return True
        except Exception as exc:
            logger.error("Pumble: connection failed: %s", exc)
            await self.disconnect()
            return False

    async def disconnect(self) -> None:
        self._stop.set()
        if self._poll_task:
            self._poll_task.cancel()
            try:
                await self._poll_task
            except asyncio.CancelledError:
                pass
            self._poll_task = None
        self._mark_disconnected()
        if self.client:
            await self.client.close()
            self.client = None

    def _channel_id(self, channel: dict[str, Any]) -> str:
        return str(_first(channel, "id", "channelId", "channel_id", default=""))

    def _channel_name(self, channel: dict[str, Any]) -> str:
        return str(_first(channel, "name", "channelName", "channel_name", default=self._channel_id(channel)))

    def _channel_type(self, channel: dict[str, Any]) -> str:
        return str(_first(channel, "channelType", "type", default="")).upper()

    def _channels_to_poll(self) -> list[dict[str, Any]]:
        result = []
        for channel in self._channels.values():
            cid = self._channel_id(channel)
            name = self._channel_name(channel)
            if self.channel_filter and cid not in self.channel_filter and name not in self.channel_filter:
                continue
            if not self.include_dms and self._channel_type(channel) in {"DIRECT", "DM", "GROUP_DM"}:
                continue
            result.append(channel)
        return result

    async def _bootstrap_seen(self) -> None:
        if not self.client:
            return
        for channel in self._channels_to_poll():
            try:
                for message in await self.client.list_messages(self._channel_id(channel)):
                    mid = _message_id(message)
                    if mid:
                        self._seen.add(mid)
            except Exception as exc:
                logger.warning("Pumble: cannot read channel %s during bootstrap: %s", self._channel_name(channel), exc)
        self._bootstrapped = True

    async def _poll_loop(self) -> None:
        while not self._stop.is_set():
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Pumble: polling cycle failed")
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=self.poll_seconds)
            except asyncio.TimeoutError:
                pass

    async def _poll_once(self) -> None:
        if not self.client:
            return
        # Refresh the directory so newly-created channels can be selected.
        channels = await self.client.list_channels()
        self._channels = {self._channel_id(c): c for c in channels if self._channel_id(c)}
        for channel in self._channels_to_poll():
            channel_id = self._channel_id(channel)
            messages = await self.client.list_messages(channel_id)
            messages.sort(key=lambda item: _created_at(item))
            for message in messages:
                mid = _message_id(message)
                text = _message_text(message).strip()
                if not mid or mid in self._seen:
                    continue
                self._seen.add(mid)
                if not text or (self.user_id and _author_id(message) == self.user_id):
                    continue
                if self.require_mention:
                    mention_tokens = {
                        self.mention,
                        f"<<@{self.user_id}>>" if self.user_id else "",
                        f"<@{self.user_id}>" if self.user_id else "",
                        f"@{self.user_id}" if self.user_id else "",
                    }
                    if not any(token and token in text.lower() for token in mention_tokens):
                        continue
                await self._dispatch_message(channel, message, text)

    async def _dispatch_message(self, channel: dict[str, Any], message: dict[str, Any], text: str) -> None:
        if not self._message_handler:
            return
        channel_id = self._channel_id(channel)
        author_id = _author_id(message)
        author_name = _author_name(message) or author_id
        source = self.build_source(
            chat_id=channel_id,
            chat_name=self._channel_name(channel),
            chat_type="dm" if self._channel_type(channel) in {"DIRECT", "DM"} else "group",
            user_id=author_id,
            user_name=author_name,
            message_id=_message_id(message),
        )
        event = MessageEvent(
            text=text,
            message_type=MessageType.TEXT,
            user_id=author_id,
            user_name=author_name,
            source=source,
            raw_message=message,
            message_id=_message_id(message),
            timestamp=_created_at(message),
        )
        await self.handle_message(event)

    def _resolve_channel_id(self, chat_id: str) -> str:
        """Accept either a Pumble channel ID or configured channel name."""
        raw = str(chat_id)
        if raw in self._channels:
            return raw
        for channel in self._channels.values():
            if self._channel_name(channel) == raw:
                return self._channel_id(channel)
        return raw

    async def send(self, chat_id, content, reply_to=None, metadata=None):
        if not self.client:
            return SendResult(success=False, error="Pumble adapter is not connected", retryable=True)
        try:
            message_id = await self.client.send_message(self._resolve_channel_id(str(chat_id)), str(content))
            if message_id:
                self._seen.add(message_id)
            return SendResult(success=True, message_id=message_id or None)
        except httpx.HTTPStatusError as exc:
            return SendResult(success=False, error=f"Pumble API returned HTTP {exc.response.status_code}", retryable=exc.response.status_code >= 500)
        except Exception as exc:
            return SendResult(success=False, error=str(exc), retryable=True)

    async def get_chat_info(self, chat_id):
        channel = self._channels.get(str(chat_id), {})
        return {"name": self._channel_name(channel) if channel else str(chat_id), "type": "group"}


def check_requirements() -> bool:
    return bool(_get_secret("PUMBLE_API_KEY"))


def validate_config(config) -> bool:
    extra = getattr(config, "extra", {}) or {}
    return bool(_get_secret("PUMBLE_API_KEY") or extra.get("api_key"))


def env_enablement() -> dict[str, Any] | None:
    key = _get_secret("PUMBLE_API_KEY")
    if not key:
        return None
    seed: dict[str, Any] = {"api_key": key, "api_url": os.getenv("PUMBLE_API_URL", DEFAULT_API_URL)}
    home = os.getenv("PUMBLE_HOME_CHANNEL", "").strip()
    if home:
        seed["home_channel"] = {"chat_id": home, "name": home}
    return seed
