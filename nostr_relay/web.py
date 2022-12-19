import asyncio
import collections
import logging
import secrets
import falcon
import rapidjson
from falcon import media


import falcon.asgi

from .db import Storage


class Client:
    def __init__(self, ws):
        self.ws = ws
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

            if self.validate_message(message):
                self.messages.append(message)

    def start(self):
        self.listen_task = asyncio.create_task(self.receive_messages())

    async def stop(self):
        self.running = False
        self.listen_task.cancel()
        try:
            await self.listen_task
        except asyncio.CancelledError:
            pass


class Resource:
    def __init__(self, storage):
        self.log = logging.getLogger('nostr_relay.web')
        self.connections = 0
        self.storage = storage

    async def on_get(self, req: falcon.Request, resp: falcon.Response):
        media = {
            'name': 'python relay',
            'description': 'relay written in python',
            'pubkey': '',
            'contact': '',
            'supported_nips': ['20'],
            'software': 'https://code.pobblelabs.org/fossil/nostr_relay.fossil',
            'version': '0.1'
        }
        resp.media = media
        resp.append_header('Access-Control-Allow-Origin', '*')
        resp.append_header('Access-Control-Allow-Headers', '*')
        resp.append_header('Access-Control-Allow-Methods', '*')

    async def on_websocket(self, req: falcon.Request, ws: falcon.asgi.WebSocket):

        try:
            await ws.accept()
            self.connections += 1
        except falcon.WebSocketDisconnected:
            return

        client_id = f'{req.remote_addr}:{secrets.token_hex(2)}'
        self.log.info(f'Accepted {client_id}')

        client = Client(ws)
        client.start()

        try:
            while client.is_alive:
                while ws.ready and not client.messages:
                    sent = 0
                    for sub_id, event in self.storage.read_subscriptions(client_id):
                        message = ["EVENT", sub_id, event.to_json_object()]
                        # print(f'SENDING: {message}')
                        await ws.send_media(message)
                        sent += 1
                    if sent:
                        self.log.debug(f'Sent {sent} events to {client_id}')
                    await asyncio.sleep(1.0)

                if not client.messages:
                    continue
                try:
                    message = client.messages.popleft()
                    self.log.debug(message)
                    if message[0] == 'REQ':
                        sub_id = message[1]
                        self.storage.subscribe(client_id, sub_id, message[2:])
                    elif message[0] == 'CLOSE':
                        sub_id = message[1]
                        self.storage.unsubscribe(client_id, sub_id)
                    elif message[0] == 'EVENT':
                        result, event = await self.storage.add_event(message[1])
                        await ws.send_media(['OK', event.id, result, ""])
                except falcon.WebSocketDisconnected:
                    break
        except Exception:
            self.log.exception("ws loop")
        
        await client.stop()
        self.storage.unsubscribe(client_id)
        self.connections -= 1
        self.log.info(f'Done {client_id}')


class SetupMiddleware:
    def __init__(self, storage):
        self.storage = storage

    async def process_startup(self, scope, event):
        await self.storage.setup_db()

    async def process_shutdown(self, scope, event):
        await self.storage.db.close()


def create_app():
    import os
    import logging


    db_filename = os.getenv('NOSTR_DB_FILENAME', 'nostr.sqlite3')

    storage = Storage(db_filename)

    json_handler = media.JSONHandlerWS(
        dumps=rapidjson.dumps,
        loads=rapidjson.loads,
    )
    logging.basicConfig(format='%(asctime)s %(name)s %(levelname)s %(message)s', level=logging.INFO)

    app = falcon.asgi.App(middleware=SetupMiddleware(storage))
    app.add_route('/', Resource(storage))
    app.ws_options.media_handlers[falcon.WebSocketPayloadType.TEXT] = json_handler
    
    return app

