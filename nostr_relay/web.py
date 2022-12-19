import asyncio
import collections
import secrets
import falcon

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
    def __init__(self):
        self.connections = 0
        self.storage = Storage()

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
        print(f'Accepted from {client_id}')

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
                        print(f'Sent {sent} events to {client_id}')
                    await asyncio.sleep(1.0)

                if not client.messages:
                    continue
                try:
                    message = client.messages.popleft()
                    print(message)
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
            import traceback
            traceback.print_exc()
        
        await client.stop()
        self.storage.unsubscribe(client_id)
        self.connections -= 1
        print('done done')

from falcon import media

import rapidjson

json_handler = media.JSONHandlerWS(
    dumps=rapidjson.dumps,
    loads=rapidjson.loads,
)

app = falcon.asgi.App()
app.add_route('/', Resource())
app.ws_options.media_handlers[falcon.WebSocketPayloadType.TEXT] = json_handler

