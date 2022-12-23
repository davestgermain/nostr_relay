import asyncio
import collections
import logging
import secrets
import falcon
import rapidjson
from falcon import media

LOG = logging.getLogger('nostr_relay.web')

import falcon.asgi

from .db import Storage
from . import __version__
from .config import Config


class Client:
    def __init__(self, ws, req):
        self.ws = ws
        self.id = f'{req.remote_addr}:{secrets.token_hex(2)}'
        LOG.info(f'Accepted {self.id} from Origin: {req.get_header("origin")}')
        self.messages = collections.deque()
        self.listen_task = None
        self.running = True

    @property
    def is_alive(self):
        return not self.listen_task.done()
    
    def validate_message(self, message):
        if not isinstance(message, list):
            return False
        if len(message) < 2:
            return False
        if message[0] not in ['EVENT', 'REQ', 'CLOSE']:
            return False
        return True

    async def receive_messages(self):
        while self.running:
            try:
                message = await self.ws.receive_media()
            except falcon.WebSocketDisconnected:
                break
            except rapidjson.JSONDecodeError:
                LOG.debug("json decoding")
                continue
            if self.validate_message(message):
                self.messages.append(message)

    async def start(self, storage):
        self.listen_task = asyncio.create_task(self.receive_messages())
        ws = self.ws
        client_id = self.id

        while self.is_alive:
            while ws.ready and not self.messages:
                sent = 0
                for sub_id, event in storage.read_subscriptions(client_id):
                    if event is not None:
                        message = ["EVENT", sub_id, event.to_json_object()]
                    else:
                        # done with stored events
                        message = ["EOSE", sub_id]
                    await ws.send_media(message)
                    sent += 1
                if sent:
                    LOG.debug(f'Sent {sent} events to {client_id}')
                await asyncio.sleep(1.0)

            if not self.messages:
                continue
            try:
                message = self.messages.popleft()
                LOG.debug("RECEIVED: %s", message)
                if message[0] == 'REQ':
                    sub_id = str(message[1])
                    storage.subscribe(client_id, sub_id, message[2:])
                elif message[0] == 'CLOSE':
                    sub_id = str(message[1])
                    storage.unsubscribe(client_id, sub_id)
                elif message[0] == 'EVENT':
                    try:
                        event, result = await storage.add_event(message[1])
                    except Exception as e:
                        LOG.error(str(e))
                        result = False
                        reason = str(e)
                        eventid = ''
                    else:
                        eventid = event.id
                        reason = '' if result else 'duplicate: exists'
                        LOG.info("%s added %s from %s", client_id, event.id, event.pubkey)
                    await ws.send_media(['OK', eventid, result, reason])
            except falcon.WebSocketDisconnected:
                break

    async def stop(self):
        self.running = False
        self.listen_task.cancel()
        try:
            await self.listen_task
        except asyncio.CancelledError:
            pass

    def __str__(self):
        return self.id


class Resource:
    def __init__(self, storage):
        self.connections = 0
        self.storage = storage

    async def on_get(self, req: falcon.Request, resp: falcon.Response):
        if req.accept == 'application/nostr+json':
            media = {
                'name': Config.relay_name,
                'description': Config.relay_description,
                'pubkey': Config.sysop_pubkey,
                'contact': Config.sysop_contact,
                'supported_nips': [1, 2, 5, 9, 11, 12, 15, 20],
                'software': 'https://code.pobblelabs.org/fossil/nostr_relay.fossil',
                'version': __version__,
                'active_subscriptions': (await self.storage.num_subscriptions())['total']
            }
            resp.media = media
        else:
            resp.text = 'try using a nostr client :-)'
        resp.append_header('Access-Control-Allow-Origin', '*')
        resp.append_header('Access-Control-Allow-Headers', '*')
        resp.append_header('Access-Control-Allow-Methods', '*')

    async def on_websocket(self, req: falcon.Request, ws: falcon.asgi.WebSocket):
        if Config.origin_blacklist:
            origin = req.get_header('origin').lower()
            if origin in Config.origin_blacklist:
                LOG.warning("Blocked origin %s from connecting", origin)
                return

        try:
            await ws.accept()
            self.connections += 1
        except falcon.WebSocketDisconnected:
            return

        client = Client(ws, req)

        try:
            await client.start(self.storage)
        except Exception:
            LOG.exception("client loop")
        finally:
            await client.stop()
            self.storage.unsubscribe(client.id)
            self.connections -= 1
            LOG.info('Done %s', client)


class ViewEventResource:
    def __init__(self, storage):
        self.storage = storage

    async def on_get(self, req: falcon.Request, resp: falcon.Response, event_id: str):
        event = await self.storage.get_event(event_id)
        if event:
            resp.media = event.to_json_object()
        else:
            raise falcon.HTTPNotFound


class SetupMiddleware:
    def __init__(self, storage):
        self.storage = storage

    async def process_startup(self, scope, event):
        import random
        await asyncio.sleep(random.random() * 2)
        await self.storage.setup_db()

    async def process_shutdown(self, scope, event):
        await self.storage.close()


def create_app(conf_file=None):
    import os
    import os.path
    import logging, logging.config
    from functools import partial


    if conf_file is None:
        conf_file = os.getenv('NOSTR_CONFIG', os.path.abspath(os.path.join(os.path.dirname(__file__), '../config/config.yaml')))

    print(f"Loading configuration from {conf_file}")

    Config.load(conf_file)
    if Config.DEBUG:
        print(Config)

    storage = Storage(Config.db_filename)

    json_handler = media.JSONHandlerWS(
        dumps=partial(
            rapidjson.dumps,
            ensure_ascii=False
        ),
        loads=rapidjson.loads,
    )
    if Config.logging:
        logging.config.dictConfig(Config.logging)
    else:
        logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s %(message)s', level=logging.DEBUG if Config.DEBUG else logging.INFO)

    app = falcon.asgi.App(middleware=SetupMiddleware(storage))
    app.add_route('/', Resource(storage))
    app.add_route('/event/{event_id}', ViewEventResource(storage))
    app.ws_options.media_handlers[falcon.WebSocketPayloadType.TEXT] = json_handler
    
    return app

