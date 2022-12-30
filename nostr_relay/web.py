import asyncio
import collections
import logging
import secrets
import falcon
import rapidjson
from falcon import media

LOG = logging.getLogger('nostr_relay.web')

import falcon.asgi

from .db import get_storage
from . import __version__
from .config import Config


class Client:
    def __init__(self, ws, req):
        self.ws = ws
        self.id = f'{req.remote_addr}-{secrets.token_hex(2)}'
        LOG.info(f'Accepted {self.id} from Origin: {req.get_header("origin")}')
        self.running = True
        self.subscription_queue = asyncio.Queue()
        self.send_task = None

    def validate_message(self, message):
        if not isinstance(message, list):
            return False
        if len(message) < 2:
            return False
        if message[0] not in ('EVENT', 'REQ', 'CLOSE'):
            return False
        return True

    async def send_subscriptions(self):
        subscription_queue = self.subscription_queue
        ws = self.ws

        sent = 0
        LOG.debug("%s waiting for subs", self.id)
        while self.running and ws.ready:
            try:
                sub_id, event = await subscription_queue.get()
                if event is not None:
                    message = f'["EVENT", "{sub_id}", {event}]'
                    # sent += 1
                else:
                    # done with stored events
                    message = f'["EOSE", "{sub_id}"]'
                await ws.send_text(message)
                LOG.debug("SENT: %s", message)
            except falcon.WebSocketDisconnected:
                break
            except asyncio.CancelledError:
                break
            except Exception:
                LOG.exception("subs")

    async def start(self, storage):
        ws = self.ws
        client_id = self.id

        while ws.ready:
            try:
                message = await self.ws.receive_media()
                if not self.validate_message(message):
                    continue

                LOG.debug("RECEIVED: %s", message)
                if message[0] == 'REQ':
                    sub_id = str(message[1])
                    if self.send_task is None:
                        self.send_task = asyncio.create_task(self.send_subscriptions())
                        await asyncio.sleep(0)
                    await storage.subscribe(client_id, sub_id, message[2:], self.subscription_queue)
                elif message[0] == 'CLOSE':
                    sub_id = str(message[1])
                    await storage.unsubscribe(client_id, sub_id)
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
            except rapidjson.JSONDecodeError:
                LOG.debug("json decoding")
                continue

    async def stop(self):
        self.running = False
        if self.send_task:
            self.send_task.cancel()
            try:
                await self.send_task
            except asyncio.CancelledError:
                pass

    def __str__(self):
        return self.id


class NostrAPI:
    """
    Handles nostr websocket interface
    """
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
                'supported_nips': [1, 2, 5, 9, 11, 12, 15, 20, 26],
                'software': 'https://code.pobblelabs.org/fossil/nostr_relay.fossil',
                'version': __version__,
                'active_subscriptions': (await self.storage.num_subscriptions())['total']
            }
            resp.media = media
        elif Config.redirect_homepage:
            raise falcon.HTTPFound(Config.redirect_homepage)
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
                await ws.close(code=1000)
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
            await self.storage.unsubscribe(client.id)
            self.connections -= 1
            LOG.info('Done %s', client)


class ViewEventResource:
    def __init__(self, storage):
        self.storage = storage

    async def on_get(self, req: falcon.Request, resp: falcon.Response, event_id: str):
        event = await self.storage.get_event(event_id)
        if event:
            resp.text = event
            resp.content_type = 'application/json'
        else:
            raise falcon.HTTPNotFound


class SetupMiddleware:
    def __init__(self, storage):
        self.storage = storage

    async def process_startup(self, scope, event):
        import random
        if Config.DEBUG:
            asyncio.get_running_loop().set_debug(True)
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

    storage = get_storage()

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

    LOG.info("Starting version %s", __version__)
    app = falcon.asgi.App(middleware=SetupMiddleware(storage))
    app.add_route('/', NostrAPI(storage))
    app.add_route('/event/{event_id}', ViewEventResource(storage))
    app.ws_options.media_handlers[falcon.WebSocketPayloadType.TEXT] = json_handler
    
    return app

