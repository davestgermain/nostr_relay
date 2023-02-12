import logging, logging.config
import unittest
import os.path
import time
import sqlalchemy

from nostr_relay.config import Config
from nostr_relay.util import Periodic

Config.load(os.path.join(os.path.dirname(__file__), "./test_config.yaml"), reload=True)

from aionostr.event import Event, PrivateKey

PK1 = "f6d7c79924aa815d0d408bc28c1a23af208209476c1b7691df96f7d7b72a2753"
PK2 = "8f50290eaa19f3cefc831270f3c2b5ddd3f26d11b0b72bc957067d6811bc618d"


class BaseTestsWithStorage(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        Config.load(
            os.path.join(os.path.dirname(__file__), "./test_config.yaml"), reload=True
        )
        logging.config.dictConfig(Config.logging)

    def setUp(self):
        pass
        # logging.disable(logging.CRITICAL)

    async def asyncSetUp(self):
        from nostr_relay.storage import get_storage, get_metadata

        self.storage = get_storage()
        await self.storage.setup()
        async with self.storage.db.begin() as conn:
            await conn.run_sync(get_metadata().create_all)

    async def asyncTearDown(self):
        async with self.storage.db.begin() as conn:
            try:
                await conn.execute(self.storage.EventTable.delete())
            except sqlalchemy.exc.OperationalError:
                pass

    @classmethod
    def tearDownClass(cls):
        Periodic.cancel_running()

    def make_event(
        self,
        privkey,
        pubkey=None,
        kind=0,
        created_at=None,
        tags=None,
        as_dict=True,
        **kwargs
    ):
        if pubkey is None:
            private_key = PrivateKey(bytes.fromhex(privkey))
            pubkey = private_key.pubkey.serialize()[1:].hex()

        evt = Event(
            kind=kind,
            pubkey=pubkey,
            created_at=int(created_at or time.time()),
            tags=tags or [],
            **kwargs
        )
        evt.sign(privkey)
        if as_dict:
            return evt.to_json_object()
        else:
            return evt
