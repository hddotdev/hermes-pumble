"""Unit tests for the Pumble adapter's provider boundary."""

import asyncio
import os
import unittest

from adapter import PumbleAdapter, PumbleClient, _author_id, _created_at, _message_text


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload
        self.content = b"{}"

    def raise_for_status(self):
        return None

    def json(self):
        return self.payload


class FakeHttpClient:
    def __init__(self):
        self.calls = []

    async def request(self, method, url, headers=None, **kwargs):
        self.calls.append((method, url, headers, kwargs))
        if url.endswith("/listChannels"):
            return FakeResponse({"channels": [{"id": "c1", "name": "general", "channelType": "PUBLIC"}]})
        if url.endswith("/listMessages"):
            return FakeResponse({"messages": [{"id": "m1", "text": "hello", "userId": "u1", "createdAt": "2026-01-01T00:00:00Z"}]})
        if url.endswith("/sendMessage"):
            return FakeResponse({"messageId": "m2"})
        return FakeResponse({})


class PumbleClientTests(unittest.TestCase):
    def test_api_shapes_and_header(self):
        async def run():
            http = FakeHttpClient()
            client = PumbleClient("secret", client=http)
            channels = await client.list_channels()
            messages = await client.list_messages("c1")
            sent = await client.send_message("c1", "reply")
            self.assertEqual(channels[0]["id"], "c1")
            self.assertEqual(_message_text(messages[0]), "hello")
            self.assertEqual(_author_id(messages[0]), "u1")
            self.assertEqual(sent, "m2")
            self.assertEqual(http.calls[0][2]["ApiKey"], "secret")
            self.assertEqual(http.calls[-1][3]["json"], {"channelId": "c1", "text": "reply", "asBot": False})

        asyncio.run(run())

    def test_adapter_filters_own_messages_and_bootstraps(self):
        async def run():
            old = dict(os.environ)
            try:
                os.environ.update({
                    "PUMBLE_API_KEY": "secret",
                    "PUMBLE_USER_ID": "bot",
                    "PUMBLE_INCLUDE_DMS": "false",
                    "PUMBLE_CHANNELS": "c1",
                })
                # A real Hermes plugin registers its platform before the
                # adapter is constructed; mirror that registry contract here.
                from gateway.platform_registry import platform_registry
                platform_registry.register_deferred("pumble", lambda: None)
                adapter = PumbleAdapter(type("Config", (), {"extra": {}})())
                adapter.client = PumbleClient("secret", client=FakeHttpClient())
                adapter._channels = {"c1": {"id": "c1", "name": "general", "channelType": "PUBLIC"}}
                await adapter._bootstrap_seen()
                self.assertIn("m1", adapter._seen)
                self.assertEqual(_created_at({"createdAt": "2026-01-01T00:00:00Z"}).year, 2026)
            finally:
                os.environ.clear()
                os.environ.update(old)

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
