import asyncio
import unittest
import time
import shutil, os
import secrets

from . import BaseTestsWithStorage, PK1, PK2
from nostr_relay.errors import StorageError
from nostr_relay.storage import kv
from nostr_relay.config import Config


class KVStorageTests(BaseTestsWithStorage):
    envdir = "/tmp/nostrtests/"

    def setUp(self):
        pass
        # logging.disable(logging.CRITICAL)

    async def asyncSetUp(self):
        self.envdir = f"/tmp/nostrtests/{secrets.token_hex(6)}"
        Config.storage = {
            "class": "nostr_relay.storage.kv.LMDBStorage",
            "path": self.envdir,
        }
        self.storage = kv.LMDBStorage(Config.storage)
        await self.storage.setup()

    async def asyncTearDown(self):
        await self.storage.close()

    def tearDown(self):
        if False and os.path.isdir(self.envdir):
            shutil.rmtree(self.envdir)

    async def test_add_event(self):
        event = self.make_event(PK1, kind=1, content="hello world")
        result = await self.storage.add_event(event)
        assert result[0].id == event["id"]

        event = self.make_event(
            PK1,
            kind=0,
            content="hello world",
            created_at=int(time.time() - 10),
            tags=[["p", event["id"]], ["expiration", "12345"]],
        )
        result = await self.storage.add_event(event)
        assert result[0].id == event["id"]

        event = self.make_event(PK1, kind=0, content="hello world 2")
        result = await self.storage.add_event(event)
        assert result[0].id == event["id"]

    async def test_add_bad_events(self):
        with self.assertRaises(StorageError):
            result = await self.storage.add_event(["foo"])

    async def test_get_event(self):
        event = self.make_event(PK1, kind=1, content="test_get_event")
        result = await self.storage.add_event(event)

        # adding event returns before queue completes
        await asyncio.sleep(0.2)
        retrieved = await self.storage.get_event(event["id"])
        assert retrieved.id == event["id"]

    async def test_good_queries(self):
        now = int(time.time())
        for i in range(10):
            event = self.make_event(
                PK1, kind=1, content=f"test_good_queries {now} {i}", created_at=now - i
            )
            await self.storage.add_event(event)
        for i in range(10):
            event = self.make_event(
                PK1,
                kind=22222,
                content=f"test_good_queries {now} {i}",
                created_at=now - i,
            )
            await self.storage.add_event(event)

        await asyncio.sleep(0.4)
        query = {"kinds": [1]}
        results = []
        async for event in self.storage.run_single_query(query):
            assert event.kind == 1
            results.append(event)

        assert len(results) == 10

        query = [{"kinds": [1], "since": now - 3}, {"kinds": [22222], "until": now - 4}]
        results = []
        async for event in self.storage.run_single_query(query):
            assert event.kind in (1, 22222)
            results.append(event)

        assert len(results) == 6

    async def test_subscription(self):
        now = int(time.time())
        for i in range(10):
            event = self.make_event(
                PK1, kind=1, content=f"test_good_queries {now} {i}", created_at=now - i
            )
            print(event["pubkey"])
            await self.storage.add_event(event)
        for i in range(10):
            event = self.make_event(
                PK1,
                kind=22222,
                content=f"test_good_queries {now} {i}",
                created_at=now - i,
            )
            await self.storage.add_event(event)

        event = self.make_event(
            PK2,
            kind=1,
            content=f"test_good_queries tag",
            tags=[
                [
                    "p",
                    "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d",
                ]
            ],
        )
        await self.storage.add_event(event)

        await asyncio.sleep(0.5)
        query = [
            {
                "kinds": [1, 2, 3, 4, 5],
                "authors": [
                    "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d"
                ],
                "limit": 10,
            },
            {"kinds": [2]},
            {"ids": ["abcdef"]},
        ]
        queue = asyncio.Queue()
        sub = await self.storage.subscribe(
            "test",
            "test_subscriptions",
            query,
            queue,
        )

        count = 0
        while True:
            sub_id, event = await queue.get()
            if event is None:
                break
            assert sub_id == "test_subscriptions"
            assert event.kind == 1
            count += 1
        assert count == 10
        await self.storage.unsubscribe("test", "test_subscriptions")

    async def test_tag_query(self):
        event = self.make_event(
            PK2,
            kind=1,
            content=f"test_good_queries tag",
            tags=[
                [
                    "p",
                    "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d",
                ]
            ],
        )
        await self.storage.add_event(event)

        await asyncio.sleep(0.5)

        query = [
            {"#p": ["5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d"]}
        ]
        plan = kv.planner(query)[0]

        results = []
        kv.execute_one_plan(
            self.storage.env, plan, results.append, log=self.storage.log
        )

        assert results[0].id == event["id"]

    async def test_date_scan(self):
        now = int(time.time())
        for i in range(10):
            event = self.make_event(
                PK1, kind=1, content=f"test_good_queries {now} {i}", created_at=now - i
            )
            # print(event['pubkey'])
            await self.storage.add_event(event)
        await asyncio.sleep(0.8)

        results = []

        query = {
            # "since": now - 100,
            "until": now
            + 100,
        }
        async for event in self.storage.run_single_query(query):
            assert event.kind == 1
            results.append(event)
            print(str(event))
        assert len(results) == 10

        results.clear()
        query = {
            "since": now - 5,
            "until": now - 4,
        }
        async for event in self.storage.run_single_query(query):
            assert event.kind == 1
            results.append(event)
        assert len(results) == 1

        results.clear()
        query = {
            "since": now - 100,
            # "until": now + 100,
        }
        async for event in self.storage.run_single_query(query):
            assert event.kind == 1
            results.append(event)
        assert len(results) == 10

    async def test_delete_event(self):
        event = self.make_event(PK1, kind=1, content="test_delete_event")
        result = await self.storage.add_event(event)

        # adding event returns before queue completes
        await asyncio.sleep(0.2)
        retrieved = await self.storage.get_event(event["id"])
        assert retrieved.id == event["id"]

        self.storage.delete_event(event["id"])
        await asyncio.sleep(0.2)

        retrieved = await self.storage.get_event(event["id"])
        assert retrieved is None

        again = self.make_event(
            PK1, kind=1, content="test_delete_event", created_at=int(time.time() - 10)
        )
        result = await self.storage.add_event(again)
        await asyncio.sleep(0.2)

        assert (await self.storage.get_event(again["id"])).id == again["id"]
        # send a delete event
        del_event = self.make_event(PK1, kind=5, tags=[["e", again["id"]]])
        result = await self.storage.add_event(del_event)
        await asyncio.sleep(0.2)

        assert (await self.storage.get_event(again["id"])) is None

    async def test_replaceable_event(self):
        now = int(time.time())
        event = self.make_event(
            PK1, kind=10000, content="test_replaceable_event 1", created_at=now - 10
        )
        result = await self.storage.add_event(event)
        await asyncio.sleep(0.2)

        replaced = self.make_event(
            PK1, kind=10000, content="test_replaceable_event 2", created_at=now
        )
        result = await self.storage.add_event(replaced)
        await asyncio.sleep(0.2)

        old_event = await self.storage.get_event(event["id"])
        assert old_event is None

    async def test_get_stats(self):
        queue = asyncio.Queue()
        sub = await self.storage.subscribe(
            "test",
            "test_subscriptions",
            {"ids": ["abcdef"]},
            queue,
        )
        stats = await self.storage.get_stats()
        assert stats["active_clients"] == 1
        assert stats["active_subscriptions"] == 1

    async def test_get_idp(self):
        idp = await self.storage.get_identified_pubkey("")
        assert idp == {"names": {}, "relays": {}}
