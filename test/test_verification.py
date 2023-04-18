import unittest
import json
import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from . import BaseTestsWithStorage

from nostr_relay.config import Config

try:
    from nostr_relay import verification
except ImportError:
    verification = None

PK1 = "f6d7c79924aa815d0d408bc28c1a23af208209476c1b7691df96f7d7b72a2753"
PK2 = "8f50290eaa19f3cefc831270f3c2b5ddd3f26d11b0b72bc957067d6811bc618d"


class MockSessionGet:
    def __init__(self, response):
        self.response = response

    async def __aenter__(self):
        return self.response

    async def __aexit__(self, exc_type, exc, tb):
        pass


@unittest.skipIf(verification is None, "nostr_bot not available")
class VerificationTests(BaseTestsWithStorage):
    async def get_events(self, query):
        async for event in self.storage.run_single_query([query]):
            yield event

    async def add_event(self, event, check_response=False):
        return await self.storage.add_event(event.to_json_object())

    def make_profile(self, privkey, identifier=None):
        meta = {}
        if identifier:
            meta["nip05"] = identifier

        event = self.make_event(
            privkey, content=json.dumps(meta), kind=0, as_dict=False
        )
        return event

    async def test_disabled(self):
        assert verification.is_nip05_verified(self.make_profile(PK1), Config)

    # async def test_enabled_unverified(self):
    #     verifier = verification.NIP05CheckerBot({"nip05_verification": "enabled", "blacklist": "baddomain.biz"})

    #     with self.assertRaises(VerificationError) as e:
    #         profile = self.make_profile(PK1, identifier="test@localhost")
    #         profile.content = "abcd"
    #         await verifier.verify(profile)
    #     assert e.exception.args[0] == "rejected: metadata must have nip05 tag"

    #     with self.assertRaises(VerificationError) as e:
    #         await verifier.verify(self.make_profile(PK1))
    #     assert e.exception.args[0] == "rejected: metadata must have nip05 tag"

    #     # bad domains
    #     with self.assertRaises(VerificationError) as e:
    #         event = self.make_profile(PK1, identifier="test@baddomain.biz")
    #         await verifier.verify(event)
    #     assert e.exception.args[0] == "rejected: metadata must have nip05 tag"

    #     with self.assertRaises(VerificationError) as e:
    #         event = self.make_profile(PK1, identifier="test@localhost/foo")
    #         await verifier.verify(event)
    #     assert e.exception.args[0] == "rejected: metadata must have nip05 tag"

    #     with self.assertRaises(VerificationError) as e:
    #         await verifier.verify(self.make_event(PK1, kind=1, as_dict=False))
    #     assert (
    #         e.exception.args[0]
    #         == "rejected: pubkey 5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d must be verified"
    #     )

    # async def test_enabled_candidate(self):
    #     verifier = Verifier(
    #         self.storage,
    #         {"nip05_verification": "enabled", "blacklist": "baddomain.biz"},
    #     )

    #     assert await verifier.verify(
    #         self.make_profile(PK1, identifier="test@localhost")
    #     )

    #     verifier = Verifier(
    #         self.storage,
    #         {"nip05_verification": "passive", "blacklist": "baddomain.biz"},
    #     )
    #     assert await verifier.verify(
    #         self.make_profile(PK1, identifier="test@localhost")
    #     )

    #     assert await verifier.verify(self.make_profile(PK1))

    def mock_session(self, mock):
        response = AsyncMock()

        session = MagicMock()
        session.get.return_value.__aenter__.return_value = SimpleNamespace(
            json=response
        )

        mock.return_value.__aenter__.return_value = session
        return response

    @patch("nostr_relay.verification.get_aiohttp_session")
    async def test_process_verifications(self, mock):
        verifier = verification.NIP05CheckerBot(
            {
                "nip05_verification": "enabled",
                "blacklist": "baddomain.biz",
                "last_run": 9999999999,
            }
        )
        verifier.manager = self

        response = self.mock_session(mock)

        # returns bad json
        for resp in (".", {}, {"names": "foo"}, {"names": {"test": "1234"}}):
            response.return_value = resp
            with self.assertLogs("NIP05CheckerBot", level="INFO") as cm:
                assert await verifier.handle_event(
                    self.make_profile(PK1, identifier="test@localhost")
                )
            assert 2 <= len(cm.output)

        # good json

        response.return_value = {
            "names": {
                "test": "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d"
            }
        }
        with self.assertLogs("NIP05CheckerBot", level="INFO") as cm:
            assert await verifier.handle_event(
                self.make_profile(PK1, identifier="test@localhost")
            )
        assert (
            "INFO:NIP05CheckerBot:Saved verification for test@localhost=5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d success=True"
            == cm.output[-1]
        )

        # bad domain
        with self.assertLogs("NIP05CheckerBot", level="INFO") as cm:
            assert not await verifier.handle_event(
                self.make_profile(PK1, identifier="test@baddomain.biz")
            )
        assert (
            "ERROR:NIP05CheckerBot:Illegal identifier test@baddomain.biz"
            == cm.output[-1]
        )

    # @patch("nostr_relay.verification.Verifier.get_aiohttp_session")
    # async def test_verify_verified(self, mock):
    #     """
    #     Test that a verified identity can post
    #     """
    #     response = self.mock_session(mock)
    #     response.return_value = {
    #         "names": {
    #             "test": "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d"
    #         }
    #     }

    #     verifier = Verifier(self.storage, {"nip05_verification": "enabled"})

    #     profile_event = self.make_profile(PK1, identifier="test@localhost")
    #     await self.storage.add_event(profile_event.to_json_object())
    #     await self.storage.add_event(
    #         self.make_profile(PK2, identifier="foo@localhost").to_json_object()
    #     )
    #     assert await verifier.verify(profile_event)
    #     task = asyncio.create_task(verifier.start())
    #     await asyncio.sleep(1)
    #     await verifier.stop()

    #     assert await verifier.verify(
    #         self.make_event(PK1, as_dict=False, kind=1, content="yes")
    #     )

    @patch("nostr_relay.verification.get_aiohttp_session")
    async def test_batch_query_expiration(self, mock):
        """
        Test that the batch query reverifies accounts
        """
        response = self.mock_session(mock)
        response.return_value = {
            "names": {
                "test": "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d"
            }
        }

        verifier = verification.NIP05CheckerBot(
            {"nip05_verification": "enabled", "expiration": 2, "update_frequency": 1},
        )
        verifier.manager = self

        profile_event = self.make_profile(PK1, identifier="test@localhost")
        await self.storage.add_event(profile_event.to_json_object())
        await self.storage.add_event(
            self.make_profile(PK2, identifier="foo@localhost").to_json_object()
        )
        assert await verifier.handle_event(profile_event)
        asyncio.create_task(verifier.start())
        await asyncio.sleep(1)

        await verifier.handle_event(
            self.make_event(PK1, as_dict=False, kind=1, content="yes")
        )
        await asyncio.sleep(1.2)
        await verifier.handle_event(
            self.make_event(PK1, as_dict=False, kind=1, content="yes")
        )
