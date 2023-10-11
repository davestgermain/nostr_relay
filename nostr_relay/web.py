import asyncio
import logging
import multiprocessing
import falcon

from time import time
from falcon import media

try:
    from websockets.exceptions import ConnectionClosedError, ConnectionClosedOK
except ImportError:
    from wsproto.utilities import ProtocolError

    ConnectionClosedError = ConnectionClosedOK = ProtocolError

import falcon.asgi

from .rate_limiter import get_rate_limiter
from . import __version__
from .config import Config
from .util import (
    ClientID,
    timeout,
    json_dumps,
    json_loads,
    event_as_json,
    JSONDecodeError,
)
from .errors import AuthenticationError, StorageError


def validate_message(message):
    if not isinstance(message, list):
        return False
    if len(message) < 2:
        return False
    if message[0] not in ("EVENT", "REQ", "CLOSE", "AUTH"):
        return False
    return True


async def send_subscriptions(get_from_storage, ws_send, log):
    sent = 0
    while True:
        try:
            sub_id, event = await get_from_storage()
            if event is not None:
                message = event_as_json(sub_id, event)
            else:
                # done with stored events
                message = f'["EOSE","{sub_id}"]'

            await ws_send(message)
            # log.debug("SENT: %s", message)
            sent += len(message)
        except (
            falcon.WebSocketDisconnected,
            ConnectionClosedError,
            ConnectionClosedOK,
            asyncio.CancelledError,
        ):
            break
        except Exception:
            log.exception("subs")
    return sent


async def start_client(
    storage,
    ws_send,
    ws_recv,
    ws_close,
    log,
    message_timeout=1800,
    rate_limiter=None,
    origin="",
    remote_addr="",
):
    """
    Start the client loop
    """
    client_id = ClientID(remote_addr)
    auth_token = {}
    subscription_queue = asyncio.Queue()
    send_task = None
    start_time = time()

    try:
        log.info(f"Accepted {client_id} from Origin: {origin}")
        if storage.authenticator.is_enabled:
            challenge = storage.authenticator.get_challenge(remote_addr)
            log.debug("Sent challenge %s to %s", challenge, client_id)
            await ws_send(json_dumps(["AUTH", challenge]))
            throttle = await storage.authenticator.should_throttle(auth_token)
        else:
            throttle = 0
        while True:
            try:
                async with timeout(message_timeout):
                    message = json_loads(await ws_recv())

                if not validate_message(message):
                    if throttle:
                        await asyncio.sleep(throttle)
                    continue

                log.debug("RECEIVED: %s", message)
                command = message[0]

                if rate_limiter and rate_limiter.is_limited(remote_addr, message):
                    if command == "EVENT":
                        response = [
                            "OK",
                            message[1]["id"],
                            False,
                            "rate-limited: slow down",
                        ]
                    else:
                        response = ["NOTICE", "rate-limited"]
                    await ws_send(json_dumps(response))
                    throttle = max(throttle, 0.25) * 2
                    await asyncio.sleep(throttle)
                    continue

                if command == "REQ":
                    sub_id = str(message[1])
                    await storage.subscribe(
                        client_id,
                        sub_id,
                        message[2:],
                        subscription_queue,
                        auth_token=auth_token,
                    )
                    if throttle:
                        await asyncio.sleep(throttle)
                    if send_task is None:
                        send_task = asyncio.create_task(
                            send_subscriptions(subscription_queue.get, ws_send, log)
                        )
                        log.debug("%s waiting for subs", client_id)

                elif command == "CLOSE":
                    sub_id = str(message[1])
                    await storage.unsubscribe(client_id, sub_id)
                elif command == "EVENT":
                    try:
                        event, result = await storage.add_event(
                            message[1], auth_token=auth_token
                        )
                    except (StorageError, AuthenticationError) as e:
                        throttle = max(throttle, 1) * 2
                        log.error("Auth error %s. Throttling %s", str(e), throttle)
                        result = False
                        reason = str(e)
                        eventid = ""
                    except Exception as e:
                        log.error(str(e))
                        result = False
                        reason = str(e)
                        eventid = ""
                    else:
                        eventid = event.id
                        reason = "" if result else "duplicate: exists"
                        log.info(
                            "%s added %s from %s", client_id, event.id, event.pubkey
                        )
                    finally:
                        if throttle:
                            await asyncio.sleep(throttle)
                        await ws_send(json_dumps(["OK", eventid, result, reason]))
                elif command == "AUTH" and storage.authenticator.is_enabled:
                    auth_token = await storage.authenticator.authenticate(
                        message[1], challenge=challenge
                    )
                    throttle = await storage.authenticator.should_throttle(auth_token)

            except StorageError as e:
                log.warning("storage error: %s for %s", e, client_id)
                try:
                    await ws_send(json_dumps(["NOTICE", str(e)]))
                except ConnectionClosedError:
                    break
            except AuthenticationError as e:
                log.warning(
                    "Auth error: %s for %s token:%s", str(e), client_id, auth_token
                )
                try:
                    await ws_send(json_dumps(["NOTICE", str(e)]))
                except ConnectionClosedError:
                    break
            except (
                falcon.WebSocketDisconnected,
                ConnectionClosedError,
                ConnectionClosedOK,
            ):
                break
            except JSONDecodeError:
                log.debug("json decoding")
                continue
            except asyncio.TimeoutError:
                log.info("%s timed out", client_id)
                await ws_close(code=1013)
                break
            except Exception:
                log.exception("client loop")
                await ws_close(code=1013)
                break
    except Exception:
        log.exception("client_loop")
    finally:
        await storage.unsubscribe(client_id)
        sent = 0
        if send_task:
            send_task.cancel()
            try:
                sent = await send_task
            except asyncio.CancelledError:
                pass
        rate_limiter.cleanup()
        duration = time() - start_time
        del subscription_queue

        log.info(
            "Done %s@%s. Sent: %d Bytes. Duration: %d Seconds",
            auth_token.get("pubkey", "anon"),
            client_id,
            sent,
            duration,
        )


class BaseResource:
    def __init__(self, storage):
        self.storage = storage
        self.log = logging.getLogger(__name__)


class NostrAPI(BaseResource):
    """
    Handles nostr websocket interface
    """

    def __init__(self, storage, rate_limiter=None):
        super().__init__(storage)
        self.rate_limiter = rate_limiter

    async def on_get(self, req: falcon.Request, resp: falcon.Response):
        if req.accept == "application/nostr+json":
            supported_nips = [1, 2, 5, 9, 11, 12, 15, 20, 26, 33, 40]
            if Config.authentication.get("enabled"):
                supported_nips.append(42)
            if Config.fts_enabled:
                supported_nips.append(50)
            metadata = {
                "name": Config.relay_name,
                "description": Config.relay_description,
                "pubkey": Config.sysop_pubkey,
                "contact": Config.sysop_contact,
                "supported_nips": supported_nips,
                "software": "https://code.pobblelabs.org/fossil/nostr_relay",
                "version": __version__,
            }
            resp.media = metadata
        elif Config.redirect_homepage:
            raise falcon.HTTPFound(Config.redirect_homepage)
        else:
            resp.text = "try using a nostr client :-)"
        resp.append_header("Access-Control-Allow-Origin", "*")
        resp.append_header("Access-Control-Allow-Headers", "*")
        resp.append_header("Access-Control-Allow-Methods", "*")

    async def on_websocket(self, req: falcon.Request, ws: falcon.asgi.WebSocket):
        origin = str(req.get_header("origin")).lower()
        if Config.origin_blacklist:
            if origin in Config.origin_blacklist:
                self.log.warning("Blocked origin %s from connecting", origin)
                await ws.close(code=1008)
                return

        try:
            if self.rate_limiter and self.rate_limiter.is_limited(
                req.remote_addr, ["ACCEPT"]
            ):
                await ws.close(code=1013)
                self.log.warning("rate-limited ACCEPT %s", req.remote_addr)
                return
            await ws.accept()

        except falcon.WebSocketDisconnected:
            return
        # from .util import easy_profiler

        # with easy_profiler():
        await start_client(
            self.storage,
            ws.send_text,
            ws.receive_text,
            ws.close,
            self.log,
            remote_addr=req.remote_addr,
            origin=origin,
            rate_limiter=self.rate_limiter,
            message_timeout=Config.get("message_timeout", 1800),
        )


class NostrStats(BaseResource):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

    async def on_get(self, req: falcon.Request, resp: falcon.Response):
        try:
            resp.media = await self.storage.get_stats()
        except Exception:
            self.log.exception("stats")
        import gc

        gc.collect()
        # self.log.info("\n" + "\n".join(self.tracker.format_diff()))


class ViewEventResource(BaseResource):
    async def on_get(self, req: falcon.Request, resp: falcon.Response, event_id: str):
        try:
            event = await self.storage.get_event(event_id)
        except ValueError:
            raise falcon.HTTPNotFound
        except Exception:
            self.log.exception("get-event")
        if event:
            resp.media = event.to_json_object()
        else:
            raise falcon.HTTPNotFound


class NostrIDP(BaseResource):
    """
    Serve /.well-known/nostr.json
    """

    async def on_get(self, req: falcon.Request, resp: falcon.Response):
        name = req.params.get("name", "")
        domain = req.host
        if name:
            identifier = f"{name}@{domain}"
        else:
            identifier = ""
        try:
            resp.media = await self.storage.get_identified_pubkey(
                identifier, domain=domain
            )
        except Exception:
            self.log.exception("idp")
        # needed for web clients
        resp.append_header("Access-Control-Allow-Origin", "*")
        resp.append_header("Access-Control-Allow-Headers", "*")
        resp.append_header("Access-Control-Allow-Methods", "*")


class SetupMiddleware:
    def __init__(self, storage):
        self.storage = storage

    async def process_request(self, req, resp):
        if req.root_path:
            req.path = req.path.replace(req.root_path, "/", 1)

    async def process_request_ws(self, req, resp):
        if req.root_path:
            req.path = req.path.replace(req.root_path, "/", 1)

    async def process_startup(self, scope, event):
        if Config.DEBUG:
            asyncio.get_running_loop().set_debug(True)
        await self.storage.setup()
        await start_mainprocess_tasks(self.storage)

    async def process_shutdown(self, scope, event):
        await self.storage.optimize()
        await self.storage.close()


is_main_process = multiprocessing.Event()


async def start_mainprocess_tasks(storage):
    if not is_main_process.is_set():
        is_main_process.set()
        storage.start_garbage_collector()
        if Config.should_run_notifier:
            from .notifier import NotifyServer

            NotifyServer().start()

        if Config.foaf:
            from .foaf import FOAFBuilder

            await FOAFBuilder().start()

    if Config.dynamic_lists:
        from .dynamic_lists import ListBuilder

        await ListBuilder().start()

    def _dump_subs(*args, **kwargs):
        logger = logging.getLogger("nostr_relay.stats")
        for client_id, client in storage.clients.items():
            subs = []
            for sub_id, sub in client.items():
                subs.append(f"{sub_id} = {sub.filters}")
            logger.info("%s: %s", client_id, "\n".join(subs))

    import signal

    signal.signal(signal.SIGUSR1, _dump_subs)


def create_app(conf_file=None, storage=None):
    import logging
    import logging.config

    Config.load(conf_file)
    if Config.DEBUG:
        print(Config)

    if Config.logging:
        logging.config.dictConfig(Config.logging)
    else:
        logging.basicConfig(
            format="%(asctime)s %(name)s %(levelname)s %(message)s",
            level=logging.DEBUG if Config.DEBUG else logging.INFO,
        )

    from .storage import get_storage

    store = storage or get_storage()

    rate_limiter = get_rate_limiter(Config)

    json_handler = media.JSONHandlerWS(
        dumps=json_dumps,
        loads=json_loads,
    )

    logging.info("Starting version %s", __version__)
    api = NostrAPI(store, rate_limiter=rate_limiter)
    app = falcon.asgi.App(middleware=SetupMiddleware(store))
    app.add_route("/", api)
    app.add_route("/stats/", NostrStats(store))
    app.add_route("/e/{event_id}", ViewEventResource(store))
    app.add_route("/.well-known/nostr.json", NostrIDP(store))
    app.ws_options.media_handlers[falcon.WebSocketPayloadType.TEXT] = json_handler

    return app


def run_with_gunicorn(conf_file=None):
    """
    Run the app using gunicorn's ASGIApplication
    """
    app = create_app(conf_file)
    from gunicorn.app.base import Application

    class ASGIApplication(Application):
        def load_config(self):
            import sys

            if sys.implementation.name == "pypy":
                # UvicornH11Worker.CONFIG_KWARGS["ws"] = "wsproto"
                worker_class = "uvicorn.workers.UvicornH11Worker"
            else:
                worker_class = "uvicorn.workers.UvicornWorker"
            self.cfg.set("worker_class", worker_class)
            for k, v in Config.gunicorn.items():
                self.cfg.set(k.lower(), v)
            self.cfg.set("preload_app", True)

        def load(self):
            # this should fix a memory leak in websocket compression
            from nostr_relay import monkeypatch

            monkeypatch.monkey()
            # this ensures that the unnecessary MessageLoggerMiddleware isn't added
            from uvicorn.config import logger

            logger.setLevel(logging.WARNING)
            return app

    ASGIApplication().run()


def run_with_uvicorn(conf_file=None, in_thread=False):
    """
    Run the app using uvicorn.
    Optionally, start the server in a daemon thread
    """
    import sys

    from nostr_relay import monkeypatch

    monkeypatch.monkey()
    from uvicorn.config import logger

    logger.setLevel(logging.WARNING)

    import uvicorn

    app = create_app(conf_file)

    options = dict(Config.gunicorn)
    if "bind" in options:
        bind = options.pop("bind")
        options["port"] = int(bind.rsplit(":", 1)[1])
    if "loglevel" in options:
        options["log_level"] = options.pop("loglevel")

    if sys.implementation.name == "pypy":
        options.update({"loop": "asyncio", "http": "h11"})

    uv_config = uvicorn.Config(app, **options)
    server = uvicorn.Server(uv_config)

    if in_thread:
        import threading

        thr = threading.Thread(target=server.run, daemon=True)
        thr.start()
    else:
        server.run()
