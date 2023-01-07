import unittest
import time
import rapidjson
import os.path
import asyncio
from nostr_relay.db import get_storage
from nostr_relay.config import Config
from nostr_relay.auth import Authenticator, Action, Role
from nostr_relay.errors import AuthenticationError


class AuthTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        Config.load(os.path.join(os.path.dirname(__file__), './test_config.yaml'))
        import logging
        logging.basicConfig()

    async def test_parse_options(self):
        auth = Authenticator(None, {'actions': {Action.save: Role.writer, Action.query: Role.reader}})
        assert auth.actions == {'save': set('w'), 'query': set('r')}
        auth = Authenticator(None, {})
        assert auth.actions == {'save': set('a'), 'query': set('a')}

    async def test_can_perform(self):
        auth = Authenticator(None, {'enabled': True, 'actions': {Action.save: Role.writer, Action.query: Role.reader}})

        token = {
            'roles': set((Role.writer.value, Role.reader.value))
        }

        assert await auth.can_do(token, 'save', {'foo': 1})

        token = {
            'roles': set((Role.anonymous.value))
        }
        assert not await auth.can_do(token, 'save')

    async def test_authentication(self):
        storage = get_storage(reload=True)
        await storage.setup_db()

        from nostr_relay.event import Event, PrivateKey
        auth = Authenticator(storage, {'actions': {Action.save: Role.writer, Action.query: Role.reader}})

        privkey1 = 'f6d7c79924aa815d0d408bc28c1a23af208209476c1b7691df96f7d7b72a2753'
        pubkey1 = '5faaae4973c6ed517e7ed6c3921b9842ddbc2fc5a5bc08793d2e736996f6394d'

        wrong_kind = Event(kind=22241, pubkey=pubkey1, created_at=time.time(), content='ws://localhost:6969')
        wrong_kind.sign(privkey1)

        with self.assertRaises(AuthenticationError) as e:
            await auth.authenticate(wrong_kind.to_json_object())

        assert e.exception.args[0] == 'invalid: Wrong kind. Must be 22242.'

        wrong_domain = Event(kind=22242, pubkey=pubkey1, created_at=time.time(), content='ws://relay.foo.biz')
        wrong_domain.sign(privkey1)

        with self.assertRaises(AuthenticationError) as e:
            await auth.authenticate(wrong_domain.to_json_object())

        assert e.exception.args[0] == 'invalid: Wrong domain'

        too_old = Event(kind=22242, pubkey=pubkey1, created_at=time.time() - 605, content='ws://localhost:6969')
        too_old.sign(privkey1)

        with self.assertRaises(AuthenticationError) as e:
            await auth.authenticate(too_old.to_json_object())

        assert e.exception.args[0] == 'invalid: Too old'

        good_event = Event(kind=22242, pubkey=pubkey1, created_at=time.time(), content='ws://localhost:6969')
        good_event.sign(privkey1)

        token = await auth.authenticate(good_event.to_json_object())
        assert token['pubkey'] == pubkey1
        assert token['roles'] == set('a')

        # now create a role
        await auth.set_roles(pubkey1, 'rw')

        token = await auth.authenticate(good_event.to_json_object())
        assert token['pubkey'] == pubkey1
        assert token['roles'] == set('rw')
        await storage.close()