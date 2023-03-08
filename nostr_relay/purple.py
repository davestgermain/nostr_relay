"""
The purple server aims to be the fastest 
Python implementation of the nostr websocket API.

Currently, the implementation uses the `websockets` library.
"""

import asyncio
import collections
import multiprocessing
import socket
import signal
import time
import threading

import logging, logging.config

import websockets

from .config import Config
from .rate_limiter import get_rate_limiter
from .storage import get_storage
from .web import Client
from .notifier import NotifyServer
from .util import Periodic, easy_profiler


multiprocessing.allow_connection_pickling()
spawn = multiprocessing.get_context("spawn")


async def main(config, sock=None):
    if config.logging:
        logging.config.dictConfig(config.logging)
    else:
        logging.basicConfig(
            format="%(asctime)s %(name)s %(levelname)s %(message)s",
            level=logging.INFO,
        )

    log = logging.getLogger("nostr_relay.web")

    kwargs = {}
    if sock:
        kwargs["sock"] = sock
    else:
        kwargs["host"] = config.purple["host"]
        kwargs["port"] = config.purple["port"]

    if config.purple.get("disable_compression"):
        kwargs["compression"] = None
        log.info("Disabled compression")

    loop = asyncio.get_running_loop()
    stop = loop.create_future()
    loop.add_signal_handler(signal.SIGTERM, stop.set_result, None)

    rate_limiter = get_rate_limiter(config)
    counters = collections.defaultdict(int)

    run_profiler = config.purple.get("run_profiler", False)

    if run_profiler:
        log.info("Enabled profiler")

    async with get_storage(config.storage) as storage:
        if config.garbage_collector and config.purple.get("workers", 1) > 1:
            notify_server = NotifyServer()
            notify_server.start()
        await Periodic.start_pending()

        async def nostr_api(websocket):
            client = Client(
                websocket,
                None,
                rate_limiter=rate_limiter,
                log=log,
                timeout=config.get("message_timeout", 1800),
            )

            try:
                counters["clients"] += 1
                if not run_profiler:
                    await client.start(storage, websocket.send, websocket.recv)
                else:
                    with easy_profiler():
                        await client.start(storage, websocket.send, websocket.recv)
            except (
                websockets.ConnectionClosedError,
                websockets.ConnectionClosedOK,
            ):
                pass
            finally:
                await client.stop()
                counters["clients"] -= 1
                counters["served"] += 1

        log.info("Starting server...")
        async with websockets.serve(nostr_api, **kwargs):
            try:
                await stop
            except asyncio.exceptions.CancelledError:
                log.info("Done. Served %d clients", counters["served"])
                return


def worker_process(conf_file=None, sock=None, gc=True):
    Config.load(conf_file)
    if Config.purple.get("workers", 1) > 1:
        Config.run_notifier = True
    # HACK: only run the gc in the first worker process
    if not gc:
        Config.garbage_collector = None
    try:
        asyncio.run(main(Config, sock=sock))
    except KeyboardInterrupt:
        return


def serve(config):
    """
    Serve the nostr API using the websockets server
    """
    opts = config.purple
    if not opts:
        print("Please configure the `purple` section in the config file")
        return -1
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.set_inheritable(True)
    sock.bind((opts["host"], opts["port"]))
    should_exit = threading.Event()
    procs = []

    def _handle_exit(*args, **kwargs):
        should_exit.set()

    signal.signal(signal.SIGINT, _handle_exit)
    signal.signal(signal.SIGTERM, _handle_exit)

    num_workers = opts.get("workers", 0) or min(multiprocessing.cpu_count(), 4)
    for i in range(num_workers):
        proc = spawn.Process(
            name=f"Worker-{i}",
            target=worker_process,
            kwargs={"conf_file": config.config_file, "sock": sock, "gc": i == 0},
        )
        proc.start()
        procs.append(proc)

    try:
        should_exit.wait()
    finally:
        for proc in procs:
            proc.terminate()
            proc.join()
        sock.close()


if __name__ == "__main__":
    serve(Config)
