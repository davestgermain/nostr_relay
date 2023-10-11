from . import BaseTestsWithStorage

from nostr_relay.config import Config
from nostr_relay import dynamic_lists
from nostr_relay.errors import StorageError

PK1 = "f6d7c79924aa815d0d408bc28c1a23af208209476c1b7691df96f7d7b72a2753"
PK2 = "8f50290eaa19f3cefc831270f3c2b5ddd3f26d11b0b72bc957067d6811bc618d"


class DynamicListTests(BaseTestsWithStorage):
    def setUp(self):
        super().setUp()
        Config.pubkey_whitelist = [
            "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d"
        ]
        Config.dynamic_lists = {
            "allow_list_queries": [{"kinds": [3]}],
            "deny_list_queries": [{"kinds": [1984]}],
        }

    async def test_allow_list(self):
        good_private_key = (
            "3a252ede173a41de27efa445138f33410de90cf22766670176161a3ec762e502"
        )
        good_public_key = (
            "6985b7fa48a5d9fde780a12d4748d90730be2675e0a474c0df46303e8b63fe5e"
        )

        event = self.make_event(PK1, kind=3, tags=[["p", good_public_key]])
        await self.storage.add_event(event)
        builder = dynamic_lists.ListBuilder()

        await builder.run_once()

        with self.assertRaises(StorageError):
            bad_event = self.make_event(PK2, as_dict=False)
            dynamic_lists.is_pubkey_allowed(bad_event, Config)

        good_event = self.make_event(good_private_key, kind=1, as_dict=False)
        dynamic_lists.is_pubkey_allowed(good_event, Config)

    async def test_deny_list(self):
        Config.pubkey_whitelist = []
        Config.service_privatekey = None
        bad_private_key = (
            "ae0a023e5c7675ac06f2ba9a6ff058283927f87d8f80e64b28bca0ac68a0a979"
        )
        bad_public_key = (
            "3e3607011e001e93e72511ebedd5c78a365a320163ee2118db6584e97824cb1f"
        )

        # create a report
        event = self.make_event(
            PK1,
            kind=1984,
            tags=[["p", bad_public_key], ["p", "garbage"]],
            content="spam",
        )
        await self.storage.add_event(event)

        builder = dynamic_lists.ListBuilder()

        await builder.run_once()

        # test with blocklist
        with self.assertRaises(StorageError):
            bad_event = self.make_event(bad_private_key, kind=1, as_dict=False)
            dynamic_lists.is_pubkey_allowed(bad_event, Config)

        assert 1 == len(dynamic_lists.DENIED_PUBKEYS)

    async def test_start(self):
        builder = dynamic_lists.ListBuilder()
        await builder.start()
        assert builder.running
        await builder.stop()

        Config.dynamic_lists = {}
        builder = dynamic_lists.ListBuilder()
        await builder.start()
        assert not builder.running
