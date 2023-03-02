"""
notifier is a simple TCP server/client that broadcasts event ids to all connected clients
"""
import asyncio
import logging
from aionostr.event import Event


class NotifyServer:
    def __init__(self, port=6000):
        self.connections = {}
        self.port = port
        self.log = logging.getLogger("nostr_relay.notify:server")
        self._task = None

    async def handle_notify(self, reader, writer):
        addr = writer.get_extra_info("peername")
        self.log.debug(addr)
        self.connections[addr] = writer

        while True:
            data = await reader.read(32)
            if not data:
                break
            self.log.debug(
                "Broadcasting %s to %s connections",
                data.hex(),
                len(self.connections) - 1,
            )

            for peer in self.connections.values():
                if peer != writer:
                    peer.write(data)
                    await peer.drain()
        del self.connections[addr]

    async def run(self):
        try:
            server = await asyncio.start_server(
                self.handle_notify, "127.0.0.1", self.port
            )
        except OSError:
            self.log.debug("could not start")
            return
        self.log.info("Starting notify server on port %s", self.port)
        async with server:
            await server.serve_forever()

    def start(self):
        self._task = asyncio.create_task(self.run())


class NotifyClient:
    def __init__(self, storage, port=6000, address="127.0.0.1"):
        self.storage = storage
        self.writer = None
        self.port = port
        self.address = address
        self.log = logging.getLogger("nostr_relay.notify:client")
        self._task = None

    async def notify(self, event: Event):
        self.log.debug("notifying about %s", event.id)
        self.writer.write(event.id_bytes)
        await self.writer.drain()

    async def connect(self):
        await asyncio.sleep(2)
        self.log.info("Connecting to notify server on port %s", self.port)
        reader, self.writer = await asyncio.open_connection(self.address, self.port)

        while True:
            data = await reader.read(32)
            if not data:
                break
            event = await self.storage.get_event(data.hex())
            if event:
                self.log.debug("Got %s", data.hex())
                await self.storage.notify_all_connected(event)
        self.log.info("Closed")

    def start(self):
        self._task = asyncio.create_task(self.connect())
