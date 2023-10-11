import asyncio
import unittest
import time
import shutil, os
import secrets

from . import BaseTestsWithStorage, PK1, PK2
from nostr_relay.errors import StorageError
from nostr_relay.config import Config
from nostr_relay.util import ClientID

try:
    from nostr_relay.storage import kv
except ImportError:
    kv = None


@unittest.skipIf(kv is None, "LMDB not available")
class BaseLMDBTests(BaseTestsWithStorage):
    envdir = "/tmp/nostrtests/"
    garbage_collector = None

    def setUp(self):
        try:
            os.makedirs(self.envdir)
        except FileExistsError:
            pass
        # logging.disable(logging.CRITICAL)

    async def asyncSetUp(self):
        self.envdir = f"/tmp/nostrtests/{secrets.token_hex(6)}"
        Config.storage = {
            "class": "nostr_relay.storage.kv.LMDBStorage",
            "path": self.envdir,
        }
        Config.garbage_collector = self.garbage_collector
        Config.analysis_delay = 0.0
        self.storage = kv.LMDBStorage(Config.storage)
        await self.storage.setup()
        self.storage.start_garbage_collector()

    async def asyncTearDown(self):
        await self.storage.close()

    def tearDown(self):
        if os.path.isdir(self.envdir):
            shutil.rmtree(self.envdir)

    async def get_events(self, query):
        results = []
        async for event in self.storage.run_single_query(query):
            results.append(event)
        return results

    async def add_event(self, event, wait=True):
        result = await self.storage.add_event(event)
        if wait:
            await self.storage.wait_for_writer()
        return result


class LMDBStorageTests(BaseLMDBTests):
    async def test_add_event(self):
        event = self.make_event(PK1, kind=1, content="hello world")
        result = await self.add_event(event)
        assert event["id"] == result[0].id

        event = self.make_event(
            PK1,
            kind=0,
            content="hello world",
            created_at=int(time.time() - 10),
            tags=[["p", event["id"]], ["expiration", "12345"]],
        )
        result = await self.add_event(event)
        assert event["id"] == result[0].id

        event = self.make_event(PK1, kind=0, content="hello world 2")
        result = await self.add_event(event)
        assert event["id"] == result[0].id

    async def test_add_bad_events(self):
        with self.assertRaises(StorageError):
            await self.storage.add_event(["foo"])

    async def test_get_event(self):
        event = self.make_event(PK1, kind=1, content="test_get_event")
        await self.add_event(event)

        retrieved = await self.storage.get_event(event["id"])
        assert event["id"] == retrieved.id

    @unittest.skipUnless(
        hasattr(BaseTestsWithStorage, "assertNoLogs"), "assertNoLogs not available"
    )
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
                kind=7,
                content=f"test_good_queries {now} {i}",
                created_at=now - i,
            )
            await self.storage.add_event(event)

        await self.storage.wait_for_writer()
        query = {"kinds": [1]}
        results = []
        async for event in self.storage.run_single_query(query):
            assert 1 == event.kind
            results.append(event)

        assert 10 == len(results)

        query = [{"kinds": [1], "since": now - 3}, {"kinds": [7], "until": now - 4}]
        results = []
        with self.assertNoLogs("nostr_relay", level="INFO"):
            async for event in self.storage.run_single_query(query):
                assert event.kind in (1, 7)
                results.append(event)

        assert 6 == len(results)
        with self.assertLogs("nostr_relay", level="INFO") as cm:
            await self.get_events([{}])
            assert ["INFO:nostr_relay.storage.kv:No empty queries allowed"] == cm.output
        with self.assertLogs("nostr_relay", level="INFO") as cm:
            await self.get_events([{"foo": 1}])
            assert [
                "INFO:nostr_relay.storage.kv:No range scans allowed ()"
            ] == cm.output

    @unittest.skipUnless(
        hasattr(BaseTestsWithStorage, "assertNoLogs"), "assertNoLogs not available"
    )
    async def test_good_query_plan(self):
        now = int(time.time())
        for i in range(10):
            event = self.make_event(
                PK1,
                kind=1,
                content=f"test_good_query_plan {now} {i}",
                created_at=now - i,
                tags=[["t", "zebra"]],
            )
            await self.storage.add_event(event)
            event = self.make_event(
                PK1,
                kind=1,
                content=f"test_good_query_plan {now} {i}",
                created_at=now - i,
                tags=[["t", "foobar"]],
            )
            await self.storage.add_event(event)
            event = self.make_event(
                PK1,
                kind=4,
                content=f"test_good_query_plan {now} {i}",
                created_at=now - i,
                tags=[["t", "bazbar"]],
            )
            await self.storage.add_event(event)
            event = self.make_event(
                PK1,
                kind=2,
                content=f"test_good_query_plan {now} {i - 1}",
                created_at=now - i - 1,
                tags=[["t", "bazbar"]],
            )
            await self.storage.add_event(event)

        await self.storage.wait_for_writer()

        query = {
            "kinds": [1, 4],
            "#t": ["foobar"],
            "since": now - 20,
        }
        results = []
        with self.assertNoLogs("nostr_relay.kvquery", level="INFO"):
            async for event in self.storage.run_single_query(query):
                assert 1 == event.kind
                results.append(event)
            assert 10 == len(results)
            await self.storage.wait_for_writer()

    async def test_subscription(self):
        now = int(time.time())
        for i in range(10):
            event = self.make_event(
                PK1, kind=1, content=f"test_good_queries {now} {i}", created_at=now - i
            )
            # print(event["pubkey"])
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
            content="test_good_queries tag",
            tags=[
                [
                    "p",
                    "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d",
                ]
            ],
        )
        await self.add_event(event)

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
            {"ids": [event["id"]]},
        ]
        client = ClientID("test")
        queue = asyncio.Queue()
        await self.storage.subscribe(
            client,
            "test_subscriptions",
            query,
            queue,
        )

        count = 0
        while True:
            sub_id, event = await queue.get()
            if event is None:
                break
            assert "test_subscriptions" == sub_id
            assert 1 == event.kind
            count += 1
        assert 11 == count
        await self.storage.unsubscribe(client, "test_subscriptions")

    async def test_prefix_queries(self):
        self.skipTest("prefix searching disabled")
        events = [
            {
                "id": "021bbc32a8c2ec2d20b6616615d7a5b5e30a23af9d5072a4bfb4d2be1dac0618",
                "pubkey": "1a0e81e33b5058880c3af6a13bea54aa98339f4813a45600832ac6f740d8504e",
                "created_at": 1676678145,
                "kind": 1,
                "tags": [],
                "content": "4883a6d6ace66072",
                "sig": "c6d35766308450ee479d81d142dd7bcd9a650bfc47a945809b5b4f9ae9ec12a54738b1825d16b3696758bf30060c820ca08e89648aff30fb434df9d5a17af608",
            },
            {
                "id": "029e3e14e058b687ac58ec527415dd062a201128179551aeb31ce61b4d921f08",
                "pubkey": "1a0e81e33b5058880c3af6a13bea54aa98339f4813a45600832ac6f740d8504e",
                "created_at": 1676678145,
                "kind": 1,
                "tags": [],
                "content": "a4ed90abe6d21a55",
                "sig": "2ef16d496616984a42994efe01c007b1db46efbe6a1035cde4dc41fee6c8766a860cb5500221c8bdbafad9b12a29d8ce0373f6832249ef6c28ae380f8bf9a9d1",
            },
            {
                "id": "02f981e30ebb11a4e9b2f3e74dcfe07cdffb487faa521abbdc789bc3f9f220bc",
                "pubkey": "1a0e81e33b5058880c3af6a13bea54aa98339f4813a45600832ac6f740d8504e",
                "created_at": 1676678145,
                "kind": 1,
                "tags": [],
                "content": "29036ca36ea23798",
                "sig": "62805304c814bc42160f992d7f460dcca77b3f77a06ecfeb7c63a3084d3e7bf7a6f64a062b5c157ba6c7a539a65e9f9e6c464e62a7e6ec6b4182eb6ebfb9f489",
            },
            {
                "id": "0a3414004af9d59a5156fd3ceb7640db83d5330cd92f5a3d4f9066353f4f20d6",
                "pubkey": "1a0e81e33b5058880c3af6a13bea54aa98339f4813a45600832ac6f740d8504e",
                "created_at": 1676678145,
                "kind": 1,
                "tags": [],
                "content": "a822e387de0740d7",
                "sig": "d43f25a2402e44f0641201a25542887b4dde25f0a62fbee3d346bf18e9213c4bf4ab0cbd377303a6b6cbb36a676bb8e7807906cbb5c0ab9bf1292c0f4b5e8a12",
            },
            {
                "id": "0ab9255f1854078e8563bd1ca393ca6b786cc4bdd9f5b9b2a69603efeea2d569",
                "pubkey": "1a0e81e33b5058880c3af6a13bea54aa98339f4813a45600832ac6f740d8504e",
                "created_at": 1676678145,
                "kind": 1,
                "tags": [],
                "content": "0cfb9879efdafa44",
                "sig": "b9c7cc4ef13f74236ee6adaf2a32d5351b0645e066453ce3a3b633c97cc574b3bc9416a34a12bcb9740cef0ef75f8ecf5e302d2ee212dc76fceb22195e055acc",
            },
            {
                "id": "0af1812b07cdb0557918eb5a89f2ed1482bef039ec8d6d9bdb9b7da41d46db3e",
                "pubkey": "1a0e81e33b5058880c3af6a13bea54aa98339f4813a45600832ac6f740d8504e",
                "created_at": 1676678145,
                "kind": 1,
                "tags": [],
                "content": "e2bbf1d4548e4aec",
                "sig": "a14e7518c7debba536870dfcf9b57f4ea92183f33744d05f8dca7989831848fcdd79d4fa52cf8f222f16b9cc795eb9d2f97b938975f717c66c3af1a4ca3bed1b",
            },
            {
                "id": "0c66420e2211f20e92d1262c396d77d1fb25d00c773d45625b187e8f31abe0c7",
                "pubkey": "1a0e81e33b5058880c3af6a13bea54aa98339f4813a45600832ac6f740d8504e",
                "created_at": 1676678145,
                "kind": 1,
                "tags": [],
                "content": "8e1b16d52baa0074",
                "sig": "8d1f19d905c653462d754e41c8bca448ed3be8a73a7c8241553687a71455d3b48d31a99c9406602c2fa24d42c2f25038701023083bff59c71bdd567a3325383f",
            },
        ]

        for event in events:
            await self.storage.add_event(event)
        await self.storage.wait_for_writer()

        query = {"ids": ["02"]}
        found = await self.get_events(query)
        assert 3 == len(found)

        query = {"authors": ["1A0E8"], "since": 1676678130}
        found = await self.get_events(query)
        assert 7 == len(found)

    async def test_tag_query(self):
        event = self.make_event(
            PK2,
            kind=1,
            content="test_good_queries tag",
            tags=[
                [
                    "p",
                    "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d",
                ]
            ],
        )
        await self.add_event(event)

        query = [
            {"#p": ["5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d"]}
        ]
        plan = kv.planner(query, log=self.storage.log)[0]

        plan, results = kv.execute_one_plan(self.storage.db, plan, log=self.storage.log)
        assert event["id"] == results[0].id

    async def test_date_scan(self):
        now = int(time.time())
        for i in range(10):
            event = self.make_event(
                PK1, kind=1, content=f"test_good_queries {now} {i}", created_at=now - i
            )
            # print(event['pubkey'])
            await self.storage.add_event(event)
        await self.storage.wait_for_writer()

        results = []

        query = {
            # "since": now - 100,
            "until": now
            + 100,
        }
        async for event in self.storage.run_single_query(query):
            assert 1 == event.kind
            results.append(event)
            # print(str(event))
        assert 10 == len(results)

        results.clear()
        query = {
            "since": now - 5,
            "until": now - 4,
        }
        async for event in self.storage.run_single_query(query):
            assert 1 == event.kind
            results.append(event)
        assert 1 == len(results)

        results.clear()
        query = {
            "since": now - 100,
            # "until": now + 100,
        }
        async for event in self.storage.run_single_query(query):
            assert 1 == event.kind
            results.append(event)
        assert 10 == len(results)

    async def test_delete_event(self):
        event = self.make_event(PK1, kind=1, content="test_delete_event")
        await self.add_event(event)

        retrieved = await self.storage.get_event(event["id"])
        assert event["id"] == retrieved.id

        await self.storage.delete_event(event["id"])
        await self.storage.wait_for_writer()

        retrieved = await self.storage.get_event(event["id"])
        assert None is retrieved

        again = self.make_event(
            PK1, kind=1, content="test_delete_event", created_at=int(time.time() - 10)
        )
        await self.add_event(again)

        assert (await self.storage.get_event(again["id"])).id == again["id"]
        # send a delete event
        del_event = self.make_event(PK1, kind=5, tags=[["e", again["id"]]])
        await self.add_event(del_event)

        assert (await self.storage.get_event(again["id"])) is None

    async def test_replaceable_event(self):
        now = int(time.time())
        event = self.make_event(
            PK1, kind=10000, content="test_replaceable_event 1", created_at=now - 10
        )
        await self.add_event(event)

        replaced = self.make_event(
            PK1, kind=10000, content="test_replaceable_event 2", created_at=now
        )
        await self.add_event(replaced)

        old_event = await self.storage.get_event(event["id"])
        assert old_event is None

    async def test_parameterized_replaceable_event(self):
        now = int(time.time())
        event = self.make_event(
            PK1,
            kind=30000,
            content="test_replaceable_event 1",
            tags=[["d", "test"]],
            created_at=now - 10,
        )
        await self.add_event(event)

        replaced = self.make_event(
            PK1,
            kind=30000,
            content="test_replaceable_event 2",
            tags=[["d", "test"]],
            created_at=now,
        )
        await self.add_event(replaced)

        old_event = await self.storage.get_event(event["id"])
        assert old_event is None

        # event with different "d" tag will not replace

        not_replaced = self.make_event(
            PK1,
            kind=30000,
            content="test_replaceable_event 3",
            tags=[["d", "foo"]],
            created_at=now,
        )
        await self.add_event(not_replaced)

        old_event = await self.storage.get_event(replaced["id"])
        assert old_event is not None

        # event without a "d" tag also works

        not_replaced = self.make_event(
            PK1, kind=30001, content="test_replaceable_event 4", created_at=now
        )
        await self.add_event(not_replaced)

        old_event = await self.storage.get_event(replaced["id"])
        assert old_event is not None

        # replace it with an equivalent

        equiv = self.make_event(
            PK1,
            kind=30001,
            content="test_replaceable_event 5",
            tags=[["d"]],
            created_at=now,
        )
        await self.add_event(equiv)

        old_event = await self.storage.get_event(not_replaced["id"])
        assert old_event is None

        # replace it again
        equiv = self.make_event(
            PK1, kind=30001, content="test_replaceable_event 6", created_at=now
        )
        await self.add_event(equiv)

        events = await self.get_events({"kinds": [30001]})
        assert 1 == len(events)
        assert equiv["id"] == events[0].id

        # older event will not replace newer one
        self.make_event(PK1, kind=30001, content="ancient", created_at=now - 10)
        await self.add_event(equiv)

        events = await self.get_events({"kinds": [30001]})
        assert 1 == len(events)
        assert equiv["id"] == events[0].id

    async def test_get_stats(self):
        queue = asyncio.Queue()
        await self.storage.subscribe(
            ClientID("test"),
            "test_subscriptions",
            [{"kinds": [1]}],
            queue,
        )
        stats = await self.storage.get_stats()
        assert stats["active_clients"] == 1
        assert stats["active_subscriptions"] == 1

    async def test_get_idp(self):
        await self.storage.set_identified_pubkey(
            "test@falconframework.org",
            "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d",
            relays=["ws://localhost:6969"],
        )
        await self.storage.set_identified_pubkey(
            "foo@falconframework.org",
            "8f50290eaa19f3cefc831270f3c2b5ddd3f26d11b0b72bc957067d6811bc618d",
        )
        await self.storage.set_identified_pubkey(
            "test@example.com",
            "8f50290eaa19f3cefc831270f3c2b5ddd3f26d11b0b72bc957067d6811bc618a",
        )
        await self.storage.wait_for_writer()
        idp = await self.storage.get_identified_pubkey("foo@falconframework.org")
        assert {
            "names": {
                "foo": "8f50290eaa19f3cefc831270f3c2b5ddd3f26d11b0b72bc957067d6811bc618d"
            },
            "relays": {},
        } == idp
        idp = await self.storage.get_identified_pubkey(domain="falconframework.org")
        assert {
            "names": {
                "foo": "8f50290eaa19f3cefc831270f3c2b5ddd3f26d11b0b72bc957067d6811bc618d",
                "test": "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d",
            },
            "relays": {
                "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d": [
                    "ws://localhost:6969"
                ]
            },
        } == idp

        await self.storage.set_identified_pubkey("test@falconframework.org", "")
        await self.storage.wait_for_writer()
        idp = await self.storage.get_identified_pubkey("test@falconframework.org")
        assert {
            "names": {},
            "relays": {},
        } == idp

    async def test_context_manager(self):
        async with self.storage:
            hello = self.make_event(PK1)
            await self.add_event(hello)
            await self.storage.get_event(hello["id"])
        assert self.storage.db is None

    async def test_authentication(self):
        roles = await self.storage.get_auth_roles(
            "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d"
        )
        assert set("a") == roles

        await self.storage.set_auth_roles(
            "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d", "rw"
        )
        await self.storage.wait_for_writer()
        roles = await self.storage.get_auth_roles(
            "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d"
        )
        assert set("rw") == roles
        await self.storage.set_auth_roles(
            "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394a", "r"
        )
        await self.storage.wait_for_writer()
        all_roles = []
        async for pubkey, roles in self.storage.get_all_auth_roles():
            all_roles.append((pubkey, roles))
        all_roles.sort()
        assert [
            (
                "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394a",
                set("r"),
            ),
            (
                "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d",
                set("rw"),
            ),
        ] == all_roles

    @unittest.skipUnless(
        hasattr(BaseTestsWithStorage, "assertNoLogs"), "assertNoLogs not available"
    )
    async def test_bogus_queries(self):
        now = int(time.time())
        first = None
        for i in range(10):
            added = self.make_event(
                PK1, kind=1, content=f"test_bogus_queries {now} {i}", created_at=now - i
            )
            await self.storage.add_event(added)
            if first is None:
                first = added
        await self.storage.wait_for_writer()

        queries = [
            {"authors": ["garbage"]},
            {"ids": ["garbage", first["id"]]},
            {"ids": ["1"]},
            {"since": "abc"},
            {"until": "abc"},
            {"limit": "abc"},
            {"authors": ["npubblabab", "ab"]},
            {"kinds": []},
            {"kinds": [], "authors": [""]},
            {"kinds": [None], "authors": [None]},
            {"kinds": [None]},
            {"ids": ["ABC"]},
            {"#t": ["test", None, 1]},
            {"#t": 12},
        ]
        for query in queries:
            print(query)
            with self.assertNoLogs("nostr_relay", level="ERROR"):
                assert not list(await self.get_events(query))

    @unittest.skipUnless(
        hasattr(BaseTestsWithStorage, "assertNoLogs"), "assertNoLogs not available"
    )
    async def test_skip_queries(self):
        """
        Test that empty queries are not executed
        """
        queries = [
            {"ids": []},
            {"#t": []},
            {"authors": []},
            {"kinds": []},
        ]
        for query in queries:
            with self.assertNoLogs("nostr_relay", level="DEBUG"):
                assert not list(await self.get_events(query))

    async def test_ephemeral_not_saved(self):
        """
        Test that ephemeral events are not saved
        """
        query = [{"kinds": [22222]}]
        queue = asyncio.Queue()
        await self.storage.subscribe(
            ClientID("ephem"),
            "test_ephemeral",
            query,
            queue,
        )
        done = await queue.get()
        assert ("test_ephemeral", None) == done

        event = self.make_event(PK1, kind=22222, content="hello world")
        result = await self.add_event(event)
        assert event["id"] == result[0].id
        assert result[1] is True
        results = await self.get_events(query)
        assert [] == results

        notified = await queue.get()
        assert event == notified[1].to_json_object()


class KVGCTests(BaseLMDBTests):
    garbage_collector = {"collect_interval": 2}

    async def test_collect_expired(self):
        now = int(time.time())
        event1 = self.make_event(
            PK1,
            kind=1,
            content=f"will expire {now} + 2",
            tags=[["expiration", str(now + 2)]],
            created_at=now,
        )

        event2 = self.make_event(
            PK1,
            kind=1,
            content=f"will expire {now} - 3",
            tags=[["expiration", str(now - 3)]],
            created_at=now,
        )
        event3 = self.make_event(
            PK1,
            kind=1,
            content=f"will expire {now} + 30",
            tags=[["expiration", str(now + 30)]],
            created_at=now,
        )
        with self.assertLogs("nostr_relay.storage:gc", level="INFO") as cm:
            await self.add_event(event1)
            await self.add_event(event2)
            await self.add_event(event3)

            results = []
            async for event in self.storage.run_single_query({"kinds": [1]}):
                assert event.kind == 1
                results.append(event)
            assert 3 == len(results)

            await asyncio.sleep(4)

            results = await self.get_events({"kinds": [1]})
            assert 1 == len(results)
        assert [
            "INFO:nostr_relay.storage:gc:Collected garbage (1 events)",
            "INFO:nostr_relay.storage:gc:Collected garbage (1 events)",
        ] == cm.output


class FTSTests(BaseLMDBTests):
    def setUp(self):
        super().setUp()
        Config.fts_enabled = True
        kv.INDEXES["search"].enabled = True
        kv.INDEXES["search"]._index = None
        # kv.INDEXES["search"].index_path = Config.fts_index_path

    async def add_events(self):
        for content in [
            "hello unicorn",
            "hello banana",
            "hello pizza",
            "goodbye april",
        ]:
            event = self.make_event(
                PK1,
                kind=1,
                content=content,
                created_at=1676678145,
            )
            await self.storage.add_event(event)
        await self.storage.wait_for_writer()

    async def test_fts_search(self):
        await self.add_events()

        old_event = self.make_event(
            PK1,
            kind=1,
            content="hello earlier",
            created_at=1676678135,
        )
        await self.add_event(old_event)

        query = {"search": "hello", "since": 1676678140}
        results = await self.get_events(query)
        assert 3 == len(results)

        query = {"search": "pizza"}
        results = await self.get_events(query)
        assert 1 == len(results)

        query = {"search": "hello", "until": 1676678137}
        results = await self.get_events(query)
        assert 1 == len(results)

        await self.storage.delete_event(old_event["id"])
        await self.storage.wait_for_writer()
        query = {"search": "hello earlier"}
        results = await self.get_events(query)
        assert 0 == len(results)

    async def test_bulk_index(self):
        import time

        await self.add_events()

        until = int(time.time()) + 10
        with self.assertLogs("nostr_relay", level="INFO") as cm:
            await self.storage.reindex("search", until=until)
            await self.storage.wait_for_writer()
        assert [
            f"INFO:nostr_relay.storage.kv:Starting reindex for search from 1 to {until}",
            "INFO:nostr_relay.storage.kv:Bulk updating search index. batch size: 4/500 count: 4",
            "INFO:nostr_relay.storage.kv:Done bulk updating search. 4 events indexed",
            # "DEBUG:nostr_relay.fts:Indexed 8140d6aea54a718ae626dd4595ccec1ac8f5771af519e1c41ec115183ed598b5",
            # "DEBUG:nostr_relay.fts:Indexed 5792c3d4bd3a47cb1f445e2dcaa4af54c7b32ec01ed039722c8e55c48d8a1c35",
            # "DEBUG:nostr_relay.fts:Indexed 4bef6009b4b7b8862201a0ee052aa42908da9f2df6f54bde90f34466375b5927",
            # "DEBUG:nostr_relay.fts:Indexed 3ec01a9a03b9dbae8c79559639633011514ed696374b40c8d862a0774a9db489",
        ] == cm.output
        await self.storage.reindex("kinds")
        # await self.storage.wait_for_writer()
