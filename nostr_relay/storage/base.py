import asyncio
import logging
import collections
import functools
import typing
import weakref

from aionostr.event import Event
from pydantic import field_validator, BaseModel, Field, validator, ValidationError
from pydantic.functional_validators import AfterValidator
from typing_extensions import Annotated

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
from ..validators import get_validator


class BaseStorage:
    def __init__(self, options):
        self.options = options
        self.log = logging.getLogger("nostr_relay.storage")
        self.clients = weakref.WeakKeyDictionary()

        self.authenticator = None
        self.notifier = None
        self.garbage_collector_task = None
        self.service_privatekey = Config.get("service_privatekey", "")
        self.validate_event = get_validator(
            self.options.pop("validators", ["nostr_relay.validators.is_signed"])
        )
        if self.service_privatekey:
            from aionostr.key import PrivateKey

            self.service_privatekey = PrivateKey(bytes.fromhex(self.service_privatekey))
            self.service_pubkey = (
                self.service_privatekey.public_key.hex()
                if self.service_privatekey
                else ""
            )
        else:
            self.service_pubkey = ""
        self.service_kind = 31494

    async def add_event(self, event_json: dict, auth_token=None):
        raise NotImplementedError()

    async def close(self):
        pass

    async def optimize(self):
        pass

    async def setup(self):
        self._notify_sub_tasks = []
        self.loop = asyncio.get_running_loop()
        if Config.should_run_notifier:
            from nostr_relay.notifier import NotifyClient

            self.notifier = NotifyClient(self)
            self.notifier.start()
        else:
            self.notifier = None
        self.stat_collector = StatsCollector(self.options.pop("stats_interval", 60.0))
        await self.stat_collector.start()
        self.authenticator = get_authenticator(self, Config.get("authentication", {}))

        output_validator = Config.get("output_validator")
        if output_validator:
            self.check_output = object_from_path(output_validator)
        else:
            self.check_output = None

    def start_garbage_collector(self):
        self.garbage_collector_task = start_garbage_collector(self)

    async def __aenter__(self):
        await self.setup()
        return self

    async def __aexit__(self, ex_type, ex, tb):
        await self.close()

    async def get_stats(self):
        return {}

    async def get_event_from_query(self, query):
        """
        Return the first event from the query
        """
        async for event in self.run_single_query([query]):
            return event

    async def subscribe(
        self, client_id, sub_id, filters, queue, auth_token=None, **kwargs
    ):
        self.log.debug("%s/%s filters: %s", client_id, sub_id, filters)
        subs = self.clients.setdefault(client_id, {})
        if sub_id in subs:
            await self.unsubscribe(client_id, sub_id)

        if Config.subscription_limit and len(subs) == Config.subscription_limit:
            raise StorageError("rejected: too many subscriptions")

        cleaned_filters = []
        for raw_query in filters:
            try:
                cleaned_filters.append(NostrQuery.model_validate(raw_query))
            except ValidationError as e:
                self.log.debug(str(e))
        if not cleaned_filters:
            await queue.put((sub_id, None))
            return

        sub = self.subscription_class(
            self,
            sub_id,
            cleaned_filters,
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
            subs[sub_id] = sub
            self.log.debug("%s/%s +", client_id, sub_id)
        else:
            await queue.put((sub_id, None))

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
            self._notify_sub_tasks.append(
                asyncio.create_task(self.notifier.notify(event))
            )

    async def notify_all_connected(self, event):
        with self.stat_collector.timeit("notify") as counter:
            # notify all subscriptions
            if self._notify_sub_tasks:
                await asyncio.wait(self._notify_sub_tasks)
                self._notify_sub_tasks.clear()
            for client in self.clients.values():
                for sub in client.values():
                    self._notify_sub_tasks.append(
                        asyncio.create_task(sub.notify(event))
                    )
                    counter["count"] += 1

    async def add_service_event(
        self, content="", kind=None, tags=None, created_at=None, encrypt=False
    ):
        """
        Add an event for internal data storage
        Currently defined as a parameterized replaceable event of kind 31494
        """
        if tags is None:
            tags = []
        elif isinstance(tags, dict):
            tags = list(tags.items())
        if not self.service_privatekey:
            raise StorageError("Config.service_privatekey is not set")
        if encrypt:
            content = self.service_privatekey.encrypt_message(
                content, self.service_pubkey
            )

        event = Event(
            pubkey=self.service_pubkey,
            content=content,
            kind=kind or self.service_kind,
            created_at=created_at,
            tags=tags,
        )
        event.sign(self.service_privatekey.hex())
        await self.add_event(event.to_json_object())
        return event

    async def get_auth_roles(self, pubkey: str):
        query = {
            "kinds": [self.service_kind],
            "#d": [f"auth:{pubkey}"],
            "authors": [self.service_pubkey],
        }
        event = await self.get_event_from_query(query)
        if event:
            return set(event.content)
        else:
            return self.authenticator.default_roles

    async def get_all_auth_roles(self):
        query = {
            "kinds": [self.service_kind],
            "#t": ["auth"],
            "authors": [self.service_pubkey],
        }
        async for event in self.run_single_query([query]):
            for tag in event.tags:
                if tag[0] == "p":
                    role = event.content
                    yield tag[1], set((role or "").lower())

    async def set_auth_roles(self, pubkey: str, roles: str):
        tags = {"t": "auth", "d": f"auth:{pubkey}", "p": pubkey}
        content = str(roles).lower()
        await self.add_service_event(content=content, tags=tags)

    async def get_identified_pubkey(self, identifier="", domain=""):
        query = {
            "authors": [self.service_pubkey],
            "kinds": [self.service_kind],
            "#t": ["nip05"],
        }

        if identifier:
            query["#n"] = [identifier]

        data = {"names": {}, "relays": {}}
        self.log.debug("Getting identity for %s %s", identifier, domain)
        async for event in self.run_single_query([query]):
            tags = collections.defaultdict(list)
            for tag in event.tags:
                tags[tag[0]].append(tag[1])
            identifier, id_domain = tags["n"][0].split("@", 1)
            if domain and id_domain != domain:
                continue
            data["names"][identifier] = event.content
            if tags["r"]:
                data["relays"][event.content] = tags["r"]

        return data

    async def set_identified_pubkey(self, identifier, pubkey, relays=None):
        if pubkey:
            tags = [
                ["d", f"nip05:{pubkey}"],
                ["t", "nip05"],
                ["p", pubkey],
                ["n", identifier],
            ]
            if relays:
                for relay in relays:
                    tags.append(["r", relay])
            await self.add_service_event(tags=tags, content=pubkey)
        else:
            query = {
                "#n": [identifier],
                "#t": ["nip05"],
                "authors": [self.service_pubkey],
                "kinds": [self.service_kind],
            }
            event = await self.get_event_from_query(query)
            if event:
                await self.delete_event(event.id)


class BaseSubscription:
    __slots__ = (
        "__weakref__",
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
        default_limit=Config.max_limit,
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
        raise NotImplementedError()

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
        for query in filters:
            matched = set()
            if query.ids is not None:
                matched.add(event.id in query.ids)
            if query.authors is not None:
                matched.add(event.pubkey in query.authors)
                has_delegation, match = event.has_tag("delegation", query.authors)
                if match:
                    matched.add(True)
            if query.kinds is not None:
                matched.add(event.kind in query.kinds)
            if query.since:
                matched.add(event.created_at >= query.since)
            if query.until:
                matched.add(event.created_at < query.until)
            if query.tags:
                for tagname, values in query.tags:
                    matched.add(all(event.has_tag(tagname, values)))
            if matched and all(matched):
                return True
        return False


def ids_are_hex(hexids) -> list[str]:
    new_ids = []
    for hexid in hexids:
        hexid = hexid.lower()
        if any(i not in "abcdef0123456789" for i in hexid):
            raise ValueError(f"{hexid} not hex")
        if len(hexid) < 64:
            raise ValueError(f"'{hexid}' too small")
        new_ids.append(hexid)
    return new_ids


class NostrQuery(BaseModel):
    """
    Represents a valid nostr REQ query
    """

    ids: Annotated[list[str], AfterValidator(ids_are_hex)] = None
    authors: Annotated[list[str], AfterValidator(ids_are_hex)] = None
    kinds: typing.Optional[list[int]] = None
    since: typing.Optional[int] = Field(None, ge=0, lt=2145934800)
    until: typing.Optional[int] = Field(None, ge=0, lt=2145934800)
    limit: typing.Optional[int] = Field(ge=0, default=Config.max_limit)
    search: typing.Optional[str] = None
    tags: typing.Optional[list[tuple[str, set]]] = None

    @field_validator("tags")
    @classmethod
    def check_tags(cls, values):
        for tag, tagvalue in values:
            for val in tagvalue:
                if not isinstance(val, str):
                    raise ValueError(f"{val} is not a string")
        return values

    @field_validator("ids", "authors", "kinds")
    @classmethod
    def sort_fields(cls, values):
        return sorted(set(values), reverse=True)

    @classmethod
    def model_validate(cls, obj):
        if isinstance(obj, cls):
            return obj
        tags = []
        try:
            for k, v in obj.items():
                if k.startswith("#") and len(k) == 2 and isinstance(v, list):
                    tags.append((k[1], set(v)))
            tags.sort(reverse=True)
        except AttributeError:
            raise StorageError("not a query")
        if tags:
            obj["tags"] = tags
        else:
            obj.pop("tags", None)
        return super().model_validate(obj)


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
        return await super().start()

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
