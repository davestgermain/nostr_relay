import asyncio
import os.path
import os
import unittest
import logging
import time
import threading

from falcon import testing

from nostr_relay.config import Config
from nostr_relay.web import create_app, get_storage

PK1 = 'f6d7c79924aa815d0d408bc28c1a23af208209476c1b7691df96f7d7b72a2753'
PK2 = '8f50290eaa19f3cefc831270f3c2b5ddd3f26d11b0b72bc957067d6811bc618d'

EVENTS = [
    {"id": "0d7721e1ee4a343f623cfb86374cc2d4688784bd264b5bc26079843169f28d88", "pubkey": "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d", "created_at": 1672325827, "kind": 0, "tags": [], "content": "{\"name\": \"test1\", \"nip05\": \"test@st.germa.in\", \"description\": \"test account\"}", "sig": "bb748c84613eb9073d4dc99fc6df0c96aa43fd380c2c058a71f67a8f0fe012e5c40d74aa81c4c233a092eb307c7b5b8a4499e3403e0a54469ad1db1575916cf5"},
    {"id": "46981a47c7e28f720a2609a5872c62b6e8b9ae612db3f8320b68be446ad77e11", "pubkey": "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d", "created_at": 1672412227, "kind": 1, "tags": [], "content": "hello world", "sig": "d81c89008bcb9e27a0fc3454fef6b481034cfeaaf0c48e65299fa9af85e35e2c27e36a2478ba8a402a14ed3c332e4b7a43d7682e9d5baea0a7a9083fc26dba00"},
    {"id": "ea54cf0e912b3fc482ee842195c8db670df6d958b2ae86e039b7c56690d81ef9", "pubkey": "44c7a871ce6224d8d274b494f8f68827cb966e3aaba723a14db8dd22e0542e7d", "created_at": 1672325827, "kind": 1, "tags": [["p", "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d"], ["e", "46981a47c7e28f720a2609a5872c62b6e8b9ae612db3f8320b68be446ad77e11"]], "content": "hello pk1", "sig": "73106b28855337fd2546a04d02cec76c1673428811acc42bc2b69cedc03554024b8ca8bb841b9991b728db3be89760486be147b2fb03e3f4e16ca83a356d56ef"},
]

REPLACE_EVENTS = [
    {"id": "ae0816fff8d0363a189f22cf8ade674c386963cab39d68b28b19a7b5eae90cbd", "pubkey": "44c7a871ce6224d8d274b494f8f68827cb966e3aaba723a14db8dd22e0542e7d", "created_at": 1672324827, "kind": 11111, "tags": [], "content": "replace me", "sig": "0f9bfe1047dfc37a020c8b0196703ec553b22cf9b6258c3e7b4e8a67991fd788813018e90558f18d3337e48152e2e0fa50a8b925e35ad91a8db603c517092f28"},
    {"id": "2270926268812f4886d4499bc9a29a84223e5554985496843278e1472fab3837", "pubkey": "44c7a871ce6224d8d274b494f8f68827cb966e3aaba723a14db8dd22e0542e7d", "created_at": 1672326827, "kind": 11111, "tags": [], "content": "replaced", "sig": "d48846d5a7d344d60ba66c97ebe923d5f61b2e3970fd92682795ba5ff91137f288fd234ce5cafdaddc9b0f6408085c46c7255994eb3850e99ebbc05e3108f2c6"}
]

DELEGATION_EVENT = { "id": "a080fd288b60ac2225ff2e2d815291bd730911e583e177302cc949a15dc2b2dc",  "pubkey": "62903b1ff41559daf9ee98ef1ae67cc52f301bb5ce26d14baba3052f649c3f49",  "created_at": 1660896109,  "kind": 1, "tags":[["delegation","86f0689bd48dcd19c67a19d994f938ee34f251d8c39976290955ff585f2db42e","kind=1&created_at>1640995200","c33c88ba78ec3c760e49db591ac5f7b129e3887c8af7729795e85a0588007e5ac89b46549232d8f918eefd73e726cb450135314bfda419c030d0b6affe401ec1"]],  "content": "Hello world",  "sig": "cd4a3cd20dc61dcbc98324de561a07fd23b3d9702115920c0814b5fb822cc5b7c5bcdaf3fa326d24ed50c5b9c8214d66c75bae34e3a84c25e4d122afccb66eb6"}


class TestEvents(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.app = create_app(os.path.join(os.path.dirname(__file__), './test_config.yaml'))
        cls.storage = get_storage()

    def setUp(self):
        pass
        logging.disable(logging.CRITICAL)


    async def asyncSetUp(self):
        self.conductor = testing.ASGIConductor(self.app)
        await self.storage.setup_db()

    async def asyncTearDown(self):
        await self.storage.db.execute("DELETE from event")
        await self.storage.db.commit()
        await self.storage.close()

    @classmethod
    def tearDownClass(cls):
        super().tearDownClass()
        cls.delete_db_files()
        logging.disable(logging.NOTSET)

    @classmethod
    def delete_db_files(cls):
        filename = cls.storage.filename
        for fname in [filename, filename + '-shm', filename + '-wal']:
            if os.path.exists(fname):
                os.unlink(fname)


    async def test_get_event(self):
        async with self.conductor.simulate_ws('/') as ws:
            await self.send_event(ws, EVENTS[0], True)
        result = await self.conductor.simulate_get(f'/event/{EVENTS[0]["id"]}')
        data = result.json
        assert data == EVENTS[0]

    async def test_get_meta(self):
        doc = {
            'active_subscriptions': 0,
            'contact': 5678,
            'description': 'test relay description',
            'name': 'test relay',
            'pubkey': 1234,
            'software': 'https://code.pobblelabs.org/fossil/nostr_relay.fossil',
            'supported_nips': [
                1,
                2,
                5,
                9,
                11,
                12,
                15,
                20,
                26,
            ],
            'version': '1.0.2',
        }
        result = await self.conductor.simulate_get('/', headers={'Accept': 'application/nostr+json'})

        assert result.json == doc

    async def test_get_index(self):
        result = await self.conductor.simulate_get('/')
        assert result.text == 'try using a nostr client :-)'
        Config.redirect_homepage = 'https://nostr.net/'
        result = await self.conductor.simulate_get('/')
        assert result.headers['location'] == 'https://nostr.net/'

    async def send_event(self, ws, event, get_response=False):
        await ws.send_json(["EVENT", event])
        if get_response:
            return await ws.receive_json()

    async def get_event(self, ws, event_id, check=True, exists=True, req_name="checkid"):
        await ws.send_json(["REQ", req_name, {"ids": [event_id]}])
        data = await ws.receive_json()
        if check:
            if exists:
                assert data[0] == 'EVENT'
                assert data[1] == req_name
                assert data[2]["id"] == event_id
            else:
                assert data == ["EOSE", req_name]
        if exists:
            end = await ws.receive_json()
            assert end == ["EOSE", req_name]
        return data

    async def test_bad_protocol(self):
        async with self.conductor.simulate_ws('/') as ws:
            await ws.send_json({"bad": 1})
            await ws.send_json([{"bad": 1}])
            await ws.send_json(["BAD", {"bad": 1}])

    async def test_send_bad_events(self):
        async with self.conductor.simulate_ws('/') as ws:
            # send a too-large event
            large_event = EVENTS[1].copy()
            large_event['content'] = 'x' * 8192
            await self.send_event(ws, large_event)
            data = await ws.receive_json()
            assert data[2] == False
            assert data[3] == 'invalid: 280 characters should be enough for anybody'
            # send an event with wrong signature
            bad_sig = EVENTS[1].copy()
            bad_sig['content'] = 'bad'
            await self.send_event(ws, bad_sig)
            data = await ws.receive_json()
            assert data[2] == False
            assert data[3] == 'invalid: Bad signature'
            # send a non-json event
            await self.send_event(ws, 'bad event')
            data = await ws.receive_json()
            assert data[2] == False
            assert data[3] == 'invalid: Bad JSON'

    async def test_send_event(self):
        async with self.conductor.simulate_ws('/') as ws:
            data = await self.send_event(ws, EVENTS[0], True)
            assert data == ['OK', '0d7721e1ee4a343f623cfb86374cc2d4688784bd264b5bc26079843169f28d88', True, '']
            data = await self.send_event(ws, EVENTS[0], True)
            assert data == ['OK', '0d7721e1ee4a343f623cfb86374cc2d4688784bd264b5bc26079843169f28d88', False, 'duplicate: exists']

            # check for existence
            data = await self.get_event(ws, EVENTS[0]["id"])

            for event in EVENTS:
                await self.send_event(ws, event, True)


    async def test_req(self):
        async with self.conductor.simulate_ws('/') as ws:
            for event in EVENTS:
                await self.send_event(ws, event, True)
            await ws.send_json(["REQ", "test", {"ids": [EVENTS[1]["id"]]}, {"#p": EVENTS[1]['pubkey']}])
            data = await ws.receive_json()
            assert data == ['EVENT', 'test', EVENTS[1]]
            await ws.send_json(["CLOSE", "test"])
            data = await ws.receive_json()
            assert data == ['EOSE', 'test']

            await ws.send_json(["REQ", "test2", {"kinds": [0]}])
            data = await ws.receive_json()
            assert data[2]['id'] == '0d7721e1ee4a343f623cfb86374cc2d4688784bd264b5bc26079843169f28d88'
            data = await ws.receive_json()
            assert data == ['EOSE', 'test2']

            # test replacing a subscription
            await ws.send_json(["REQ", "test2", {"kinds": [1]}])
            data = await ws.receive_json()
            assert data[2]['kind'] == 1

    async def test_req_after_add(self):
        """
        test subscribing then receiving new additions
        """
        async with self.conductor.simulate_ws('/') as ws:
            await self.send_event(ws, EVENTS[1], True)
            await ws.send_json(["REQ", "test", {
                    "kinds": [1], 
                    "authors": ["5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d"],
                    "since": 1671325827,
                    "until": 1672328000
                },
                {
                    "#e": [EVENTS[1]["id"]],
                    "authors": [EVENTS[2]['pubkey']], 
                    "ids": [EVENTS[2]["id"]],
                    "limit": "10"
                },
            ])

            data = await ws.receive_json()
            assert data == ['EVENT', 'test', EVENTS[1]]
            
            data = await ws.receive_json()
            assert data == ['EOSE', 'test']

            # now add a new event
            data = await self.send_event(ws, EVENTS[2], True)
            if data[0] == 'OK':
                await asyncio.sleep(1)
                data = await ws.receive_json()
            assert data == ['EVENT', 'test', EVENTS[2]]
            
    async def test_bad_req(self):
        async with self.conductor.simulate_ws('/') as ws:
            # unsubscribe from nonexistent req
            await ws.send_json(["REQ", "unknown", {}])
            data = await ws.receive_json()
            assert data == ['EOSE', 'unknown']

    async def test_replace_events(self):
        async with self.conductor.simulate_ws('/') as ws:
            await self.send_event(ws, REPLACE_EVENTS[0], True)
            await self.send_event(ws, REPLACE_EVENTS[1], True)
            await ws.send_json(["REQ", "test", {"kinds": [11111]}])
            data = await ws.receive_json()
            assert data == ['EVENT', 'test', REPLACE_EVENTS[1]]
            data = await ws.receive_json()
            assert data == ['EOSE', 'test']

    async def test_delegation_event(self):
        async with self.conductor.simulate_ws('/') as ws:
            await self.send_event(ws, DELEGATION_EVENT, True)
            await ws.send_json(["REQ", "delegation", {"authors": ["86f0689bd48dcd19c67a19d994f938ee34f251d8c39976290955ff585f2db42e"]}])
            data = await ws.receive_json()
            assert data == ["EVENT", "delegation", DELEGATION_EVENT]

    async def test_delete_event(self):
        async with self.conductor.simulate_ws('/') as ws:
            await self.send_event(ws, EVENTS[1], True)
            await self.send_event(ws, EVENTS[2], True)

            # send valid delete event
            response = await self.send_event(ws, {"id": "6b91c6821f4b480c493b4a07e736675ed078fdcdf71a3918fe59e3d1ef2da907", "pubkey": "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d", "created_at": 1672325827, "kind": 5, "tags": [["e", "46981a47c7e28f720a2609a5872c62b6e8b9ae612db3f8320b68be446ad77e11"]], "content": "delete mistake", "sig": "f6a41a55c4b1bf964db2d67a14b1680c6ba39f7216fc45b195d760eb6b9308c73694dd7cc61283562409328daedea3a733e8e692ded46bb3e61e2195ab0ce655"}, True)
            assert response == ["OK", "6b91c6821f4b480c493b4a07e736675ed078fdcdf71a3918fe59e3d1ef2da907", True, '']

            # event should be deleted
            await ws.send_json(["REQ", "deleted", {"ids": ["46981a47c7e28f720a2609a5872c62b6e8b9ae612db3f8320b68be446ad77e11"]}])
            data = await ws.receive_json()
            assert data == ['EOSE', 'deleted']

            # send invalid delete event
            response = await self.send_event(ws, {"id": "61b5e7adaefaefc600ae838806667d87ca9aef19993707190f0fffa2dbee71b0", "pubkey": "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d", "created_at": 1672325827, "kind": 5, "tags": [["e", "ea54cf0e912b3fc482ee842195c8db670df6d958b2ae86e039b7c56690d81ef9"]], "content": "bad deletion", "sig": "6a274fa7c18fe34fcc29aa4690286941c288ef57d1ff083645f96549276ea7c4655ddc03a05fcc7167483247f4aadde71d89794421f24da7f7958e065e44744c"}, True)
            assert response == ["OK", "61b5e7adaefaefc600ae838806667d87ca9aef19993707190f0fffa2dbee71b0", True, '']

            # event will not be deleted
            await ws.send_json(["REQ", "deleted", {"ids": ["ea54cf0e912b3fc482ee842195c8db670df6d958b2ae86e039b7c56690d81ef9"]}])
            data = await ws.receive_json()
            assert data == ['EVENT', 'deleted', EVENTS[2]]

    async def test_ephemeral_event(self):
        from nostr_relay.db import start_garbage_collector
        self.storage.garbage_collector.stop()
        self.storage.garbage_collector = start_garbage_collector({'collect_interval': 3})
        ephemeral_event = {"id": "2696df86ce47142b7d272408e222b7a9fc4b2cc3a428bf2debf5d730ae2f42c7", "pubkey": "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d", "created_at": 1672325827, "kind": 22222, "tags": [], "content": "ephemeral", "sig": "66f8a055bb3c3fc3fe0ca0ead4d5558d69627dc4f40c7320228d9e4c266509f6ac8a2ff085abbd1a9d3b0c733529bf3fcd87d43f731990467181ed1995aad5bc"}

        async with self.conductor.simulate_ws("/") as ws:
            data = await self.send_event(ws, ephemeral_event, True)
            assert data[2] == True
            data = await self.get_event(ws, ephemeral_event["id"], exists=True)
            await asyncio.sleep(3.5)
            data = await self.get_event(ws, ephemeral_event["id"], exists=False)
        self.storage.garbage_collector.stop()


if __name__ == "__main__":
    unittest.main()


