import unittest
import time
from . import BaseTestsWithStorage, PK1, PK2
from nostr_relay import validators
from nostr_relay.errors import StorageError
from nostr_relay.config import ConfigClass
from aionostr.event import Event


class ValidatorTests(BaseTestsWithStorage):
    async def asyncSetUp(self):
        self.config = ConfigClass()

    async def asyncTearDown(self):
        pass

    async def test_is_signed(self):
        bad_event = self.make_event(PK1, pubkey=PK2, as_dict=False)
        with self.assertRaises(StorageError):
            validators.is_signed(bad_event, self.config)

        good_event = self.make_event(PK1, as_dict=False)
        validators.is_signed(good_event, self.config)

    async def test_is_not_too_large(self):
        self.config.max_event_size = 255

        bad_event = self.make_event(PK1, content="x" * 256, as_dict=False)
        with self.assertRaises(StorageError) as e:
            validators.is_not_too_large(bad_event, self.config)
        assert (
            e.exception.args[0]
            == f"invalid: 280 characters should be enough for anybody"
        )

        good_event = self.make_event(PK1, content="x" * 255, as_dict=False)
        validators.is_not_too_large(good_event, self.config)

    async def test_is_recent(self):
        self.config.oldest_event = 10
        bad_event = self.make_event(PK1, created_at=time.time() - 11, as_dict=False)
        with self.assertRaises(StorageError) as e:
            validators.is_recent(bad_event, self.config)
        assert e.exception.args[0] == f"invalid: {bad_event.created_at} is too old"

        bad_event = self.make_event(PK1, created_at=time.time() + 7200, as_dict=False)
        with self.assertRaises(StorageError) as e:
            validators.is_recent(bad_event, self.config)
        assert (
            e.exception.args[0] == f"invalid: {bad_event.created_at} is in the future"
        )

        good_event = self.make_event(PK1, as_dict=False)
        validators.is_recent(good_event, self.config)

    async def test_is_kind(self):
        self.config.valid_kinds = [0, 1000]
        bad_event = self.make_event(PK1, kind=1, as_dict=False)
        with self.assertRaises(StorageError) as e:
            validators.is_certain_kind(bad_event, self.config)
        assert e.exception.args[0] == f"invalid: kind=1 not allowed"

        good_event = self.make_event(PK1, kind=1000, as_dict=False)
        validators.is_certain_kind(good_event, self.config)

    async def test_is_author_whitelisted(self):
        self.config.pubkey_whitelist = [
            "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d"
        ]
        bad_event = self.make_event(PK2, as_dict=False)
        with self.assertRaises(StorageError) as e:
            validators.is_author_whitelisted(bad_event, self.config)

        good_event = self.make_event(PK1, as_dict=False)
        validators.is_author_whitelisted(good_event, self.config)

    async def test_is_author_blacklisted(self):
        self.config.pubkey_blacklist = [
            "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d"
        ]
        bad_event = self.make_event(PK1, as_dict=False)
        with self.assertRaises(StorageError) as e:
            validators.is_author_blacklisted(bad_event, self.config)

        good_event = self.make_event(PK2, as_dict=False)
        validators.is_author_blacklisted(good_event, self.config)

    async def test_is_pow(self):
        self.config.require_pow = 20
        bad_event = self.make_event(PK1, created_at=1675529750, as_dict=False)

        with self.assertRaises(StorageError) as e:
            validators.is_pow(bad_event, self.config)
        assert e.exception.args[0] == f"rejected: 20 PoW required. Found: 0"

        bad_event = self.make_event(
            PK1,
            created_at=1675529856,
            content="test",
            kind=1,
            tags=[["nonce", "197574", "19"]],
            as_dict=False,
        )

        with self.assertRaises(StorageError) as e:
            validators.is_pow(bad_event, self.config)
        assert e.exception.args[0] == f"rejected: 20 PoW required. Found: 19"

        good_event = self.make_event(
            PK1,
            content="test",
            created_at=1675529339,
            kind=1,
            tags=[["nonce", "179241", "20"]],
            as_dict=False,
        )
        validators.is_pow(good_event, self.config)
