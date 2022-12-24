import asyncio
import os.path
import os
import unittest
import logging

from falcon import testing

from nostr_relay.config import Config
from nostr_relay.web import create_app, get_storage

PK1 = 'f6d7c79924aa815d0d408bc28c1a23af208209476c1b7691df96f7d7b72a2753'
PK2 = '8f50290eaa19f3cefc831270f3c2b5ddd3f26d11b0b72bc957067d6811bc618d'

EVENTS = [
    {"id": "5e7eea1d97adfe7d902b17311544525540c7a1dafa5f813557b4e3f596126331", "pubkey": "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d", "created_at": 1671826886.1679559, "kind": 0, "tags": [], "content": "{\"name\": \"test1\", \"nip05\": \"test@st.germa.in\", \"description\": \"test account\"}", "sig": "1b6cc3a7ac0578af9935b4fb539c97cfd4f382c7764a73cf7a61acc6431c682db067172144ac85746f4117ec0caccfeae83d97290cf10e7525c34065b7d35a07"},
    {"id": "5bf7318709526f5a92b1fb44ec77e7d9a17584d7056bd786721b134bbe6f3219", "pubkey": "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d", "created_at": 1671913286.168229, "kind": 1, "tags": [], "content": "hello world", "sig": "d0094842dd1acbe1f62813451cbf74dfb88f38750ff30a412f20ce9b5cfedfb722f4fcf6c111d6d112a331246faf6e29677cd30b8d9a7362b927bd7066005766"},
    {"id": "ab045ac2385c4c7b96cfd1ca0afc2da8c757f44a9464aff2eb16409bb50cfe2d", "pubkey": "44c7a871ce6224d8d274b494f8f68827cb966e3aaba723a14db8dd22e0542e7d", "created_at": 1671826886.168395, "kind": 1, "tags": [["p", "5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d"], ["e", "5bf7318709526f5a92b1fb44ec77e7d9a17584d7056bd786721b134bbe6f3219"]], "content": "hello pk1", "sig": "88cbc8dd942f4f682284ae31b2aa0055e9d97939bd983b843b12c50edcf12a23693cc01345b6c1182337f679cf7d0088412fe69e66bfec0a2fca3092f01ec9b1"},
]

REPLACE_EVENTS = [
    {"id": "b822691d42a216c5f0bb09514189d4972b9e6f68b2e01b015b55820586a2510d", "pubkey": "44c7a871ce6224d8d274b494f8f68827cb966e3aaba723a14db8dd22e0542e7d", "created_at": 1671838428.322944, "kind": 11111, "tags": [], "content": "replace me", "sig": "9643d71ab3cc47809e27d88ec811655b29f4e3beeb8961e5733db40d93c5ab5551c09ba2a3cac1586c7f8c8a71a82adaefafa5579a20f4a073db22d2281a2805"},
    {"id": "e639f400569367766f370a5224477d039ce51284d99f97f232cce1c810653873", "pubkey": "44c7a871ce6224d8d274b494f8f68827cb966e3aaba723a14db8dd22e0542e7d", "created_at": 1671840428.323037, "kind": 11111, "tags": [], "content": "replaced", "sig": "fe02574d39ffa6c8e74b271256e019bf100992b0f75a219605a85ca5485959f22c2d864ead5ebec8040e147beef0000baea5cab93429e4263405c56526796b6e"}
]

DELEGATION_EVENT = { "id": "a080fd288b60ac2225ff2e2d815291bd730911e583e177302cc949a15dc2b2dc",  "pubkey": "62903b1ff41559daf9ee98ef1ae67cc52f301bb5ce26d14baba3052f649c3f49",  "created_at": 1660896109,  "kind": 1, "tags":[["delegation","86f0689bd48dcd19c67a19d994f938ee34f251d8c39976290955ff585f2db42e","kind=1&created_at>1640995200","c33c88ba78ec3c760e49db591ac5f7b129e3887c8af7729795e85a0588007e5ac89b46549232d8f918eefd73e726cb450135314bfda419c030d0b6affe401ec1"]],  "content": "Hello world",  "sig": "cd4a3cd20dc61dcbc98324de561a07fd23b3d9702115920c0814b5fb822cc5b7c5bcdaf3fa326d24ed50c5b9c8214d66c75bae34e3a84c25e4d122afccb66eb6"}


class TestEvents(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.app = create_app(os.path.join(os.path.dirname(__file__), './test_config.yaml'))
        logging.disable(logging.CRITICAL)

    async def asyncSetUp(self):
        self.conductor = testing.ASGIConductor(self.app)
        self.storage = get_storage()
        await self.storage.setup_db()

    async def asyncTearDown(self):
        await self.storage.close()

    def tearDown(self):
        filename = self.storage.filename
        for fname in [filename, filename + '-shm', filename + '-wal']:
            if os.path.exists(fname):
                os.unlink(fname)
        logging.disable(logging.NOTSET)


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
            'version': '0.9.5',
        }
        result = await self.conductor.simulate_get('/')
        assert result.text == 'try using a nostr client :-)'
        result = await self.conductor.simulate_get('/', headers={'Accept': 'application/nostr+json'})

        assert result.json == doc

    async def send_event(self, ws, event, get_response=False):
        await ws.send_json(["EVENT", event])
        if get_response:
            await ws.receive_json()

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
            await self.send_event(ws, EVENTS[0])
            data = await ws.receive_json()
            assert data == ['OK', '5e7eea1d97adfe7d902b17311544525540c7a1dafa5f813557b4e3f596126331', True, '']
            await self.send_event(ws, EVENTS[0])
            data = await ws.receive_json()
            assert data == ['OK', '5e7eea1d97adfe7d902b17311544525540c7a1dafa5f813557b4e3f596126331', False, 'duplicate: exists']

            for event in EVENTS:
                await self.send_event(ws, event)

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
            assert data[2]['id'] == '5e7eea1d97adfe7d902b17311544525540c7a1dafa5f813557b4e3f596126331'
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
            await ws.send_json(["REQ", "test", {"kinds": [1]}])
            data = await ws.receive_json()
            assert data == ['EVENT', 'test', EVENTS[1]]
            
            data = await ws.receive_json()
            assert data == ['EOSE', 'test']

            # now add a new event
            await self.send_event(ws, EVENTS[2])
            data = await ws.receive_json()
            if data[0] == 'OK':
                await asyncio.sleep(1.6)
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



if __name__ == "__main__":
    unittest.main()


