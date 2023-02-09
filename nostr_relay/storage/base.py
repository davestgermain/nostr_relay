import asyncio
import logging
import collections
import functools
from aionostr.event import Event
from ..config import Config
from ..errors import StorageError, AuthenticationError
from ..util import (
    StatsCollector,
    catchtime,
    json,
    object_from_path,
    call_from_path,
    Periodic,
)
from ..auth import get_authenticator, Action


class BaseStorage:
    def __init__(self, options):
        self.options = options
        self.log = logging.getLogger("nostr_relay.storage")
        self.clients = collections.defaultdict(dict)
        self.authenticator = None
        self.notifier = None

    async def add_event(self, event_json: dict, auth_token=None):
        raise NotImplentedError()

    async def close(self):
        pass

    async def optimize(self):
        pass

    async def setup(self):
        self.loop = asyncio.get_running_loop()
        if Config.should_run_notifier:
            from nostr_relay.notifier import NotifyClient

            self.notifier = NotifyClient(self)
            self.notifier.start()
        else:
            self.notifier = None
        self.stat_collector = StatsCollector(self.options.get("stats_interval", 60.0))
        await self.stat_collector.start()
        self.authenticator = get_authenticator(self, Config.get("authentication", {}))
        output_validator = Config.get("output_validator")
        if output_validator:
            self.check_output = object_from_path(output_validator)
        else:
            self.check_output = None
        self.garbage_collector_task = start_garbage_collector(self)

    async def __aenter__(self):
        await self.setup()
        return self

    async def __aexit__(self, ex_type, ex, tb):
        await self.close()

    async def get_stats(self):
        return {}

    async def subscribe(
        self, client_id, sub_id, filters, queue, auth_token=None, **kwargs
    ):
        self.log.debug("%s/%s filters: %s", client_id, sub_id, filters)
        if sub_id in self.clients[client_id]:
            await self.unsubscribe(client_id, sub_id)

        if (
            Config.subscription_limit
            and len(self.clients[client_id]) == Config.subscription_limit
        ):
            raise StorageError("rejected: too many subscriptions")
        sub = self.subscription_class(
            self,
            sub_id,
            filters,
            queue=queue,
            client_id=client_id,
            auth_token=auth_token,
            **kwargs,
        )
        if sub.prepare():
            if self.authenticator and not await self.authenticator.can_do(
                auth_token, Action.query.value, sub
            ):
                raise AuthenticationError("restricted: permission denied")
            sub.start()
            self.clients[client_id][sub_id] = sub
            self.log.debug("%s/%s +", client_id, sub_id)

    async def unsubscribe(self, client_id, sub_id=None):
        if sub_id:
            try:
                self.clients[client_id][sub_id].cancel()
                del self.clients[client_id][sub_id]
                self.log.debug("%s/%s -", client_id, sub_id)
            except KeyError:
                pass
        elif client_id in self.clients:
            del self.clients[client_id]

    async def num_subscriptions(self, byclient=False):
        subs = {}
        for client_id, client in self.clients.items():
            subs[client_id] = len(client)
        if byclient:
            return subs
        else:
            return {"total": sum(subs.values())}

    async def notify_other_processes(self, event):
        if self.notifier:
            asyncio.create_task(self.notifier.notify(event))

    def notify_all_connected(self, event):
        # notify all subscriptions
        with self.stat_collector.timeit("notify") as counter:
            for client in self.clients.values():
                for sub in client.values():
                    asyncio.create_task(sub.notify(event))
                    counter["count"] += 1


class BaseSubscription:
    __slots__ = (
        "storage",
        "sub_id",
        "client_id",
        "filters",
        "query",
        "queue",
        "query_task",
        "default_limit",
        "log",
        "auth_token",
    )

    def __init__(
        self,
        storage,
        sub_id,
        filters: list,
        queue=None,
        client_id=None,
        default_limit=6000,
        log=None,
        auth_token=None,
        **kwargs,
    ):
        self.storage = storage
        self.sub_id = sub_id
        self.client_id = client_id
        self.filters = filters
        self.queue = queue
        self.query_task = None
        self.default_limit = default_limit
        self.auth_token = auth_token
        self.log = log or storage.log

    def prepare(self):
        return True

    def cancel(self):
        if self.query_task:
            self.query_task.cancel()

    def start(self):
        self.query_task = asyncio.create_task(self.run_query())

    async def run_query(self):
        raise NotImplentedError()

    async def notify(self, event):
        # every time an event is added, all subscribers are notified.

        with catchtime() as t:
            matched = self.check_event(event, self.filters)

        self.log.debug(
            "%s/%s notify match %s %s duration:%.2fms",
            self.client_id,
            self.sub_id,
            event.id,
            matched,
            t.duration * 1000,
        )
        if matched:
            await self.queue.put((self.sub_id, event))

    def check_event(self, event: Event, filters: list):
        for filter_obj in filters:
            if not filter_obj:
                continue
            matched = set()
            for key, value in filter_obj.items():
                if key == "ids":
                    matched.add(event.id in value)
                elif key == "authors":
                    matched.add(event.pubkey in value)
                    has_delegation, match = event.has_tag("delegation", value)
                    if match:
                        matched.add(True)
                elif key == "kinds":
                    matched.add(event.kind in value)
                elif key == "since":
                    matched.add(event.created_at >= value)
                elif key == "until":
                    matched.add(event.created_at < value)
                elif key[0] == "#" and len(key) == 2:
                    matched.add(all(event.has_tag(key[1], value)))
                elif key == "limit":
                    # limit is irrelevant for broadcasts
                    continue
                else:
                    matched.add(False)
            if all(matched):
                return True
        return False


class BaseGarbageCollector(Periodic):
    def __init__(self, storage, **kwargs):
        self.log = logging.getLogger("nostr_relay.storage:gc")
        self.storage = storage
        self.running = True
        self.collect_interval = kwargs.get("collect_interval", 300)
        self.async_transaction = True
        super().__init__(self.collect_interval, swallow_exceptions=True)
        for k, v in kwargs.items():
            setattr(self, k, v)

    async def collect(self, db):
        pass

    async def start(self):
        self.log.info(
            "Starting garbage collector %s. Interval %s",
            self.__class__.__name__,
            self.collect_interval,
        )
        await super().start()

    async def run_once(self):
        collected = 0
        if self.async_transaction:
            async with self.storage.db.begin() as conn:
                collected = await self.collect(conn)
        else:
            with self.storage.db.begin() as conn:
                collected = await self.collect(conn)
        if collected:
            self.log.info("Collected garbage (%d events)", collected)


def start_garbage_collector(storage, options=None):
    options = options or Config.garbage_collector
    if options:
        gc_obj = call_from_path(
            options.pop("class", storage.DEFAULT_GARBAGE_COLLECTOR),
            storage,
            **options,
        )
        return asyncio.create_task(gc_obj.start())


@functools.lru_cache
def compile_filters(filter_json):
    filters = json.loads(filter_json)
    filter_string = []
    for filter_obj in filters:
        if not filter_obj:
            continue
        filter_clauses = set()
        for key, value in filter_obj.items():
            if key == "ids":
                filter_clauses.add("(event.id in %r)" % value)
            elif key == "authors":
                filter_clauses.add(
                    "(event.pubkey in %r or event.has_tag('delegation', %r)[1])"
                    % (value, value)
                )
            elif key == "kinds":
                filter_clauses.add("(event.kind in %r)" % value)
            elif key == "since":
                filter_clauses.add("(event.created_at >= %r)" % value)
            elif key == "until":
                filter_clauses.add("(event.created_at <= %r)" % value)
            elif key[0] == "#" and len(key) == 2:
                filter_clauses.add("all(event.has_tag(%r, %r))" % (key[1], value))
        filter_string.append(" and ".join(filter_clauses))
    full_string = "(" + ") or (".join(filter_string) + ")"
    function = f"""
def check(event):
    return {full_string}
"""
    loc = {}
    exec(compile(function, "", "exec"), loc)
    return loc["check"]
