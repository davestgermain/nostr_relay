"""
Experimental storage using LMDB
"""

import lmdb
import logging
import asyncio
import collections
import threading
import functools
import traceback

from contextlib import contextmanager
from queue import SimpleQueue

from aionostr.event import Event, EventKind
from msgpack import packb, unpackb
from concurrent import futures

from .base import BaseStorage, BaseSubscription
from ..auth import get_authenticator
from ..config import Config
from ..util import catchtime, Periodic, StatsCollector
from ..validators import get_validator


# ids: b'\x00<32 bytes of id>'
# created: b'\x01<4 bytes time>\x00<32 bytes id>'
# kind: b'\x02<4 bytes kind>\x00<32 bytes of id>'
# author: b'\x03<32 bytes pubkey>\x00<32 bytes id>'
# tag: b'\x09<tag string>\x00<tag value>\x00<32 bytes id>'

DONE = object()

QUERY_POOL = futures.ThreadPoolExecutor(max_workers=20)


VERSION = 1

FIELDS_TO_COLUMNS = {
    "id": 1,
    "created_at": 2,
    "kind": 3,
    "pubkey": 4,
    "content": 5,
    "tags": 6,
    "sig": 7,
}

QueryPlan = collections.namedtuple(
    "QueryPlan", ("query", "index", "matches", "limit", "since", "until")
)


class Index:
    def write(self, event, txn, operation="put"):
        event_id = event.id_bytes
        ctime = event.created_at.to_bytes(4, "big")
        func = getattr(txn, operation)
        for key in self.convert(event):
            to_save = b"%s\x00%s\x00%s" % (key, ctime, event_id)
            func(to_save, b"")

    def clear(self, event, txn):
        self.write(event, txn, operation="delete")

    def __repr__(self):
        return f"{self.__class__.__name__}({self.prefix})"

    @contextmanager
    def scanner(self, txn, matches: list, since=None, until=None):
        cursor = txn.cursor()
        # compile the matches to the expected format for the index,
        # to make substring checks quicker
        compiled_matches = []
        for match in sorted(matches, reverse=True):
            compiled_matches.append(self.to_key(match))
        if since is not None:
            since = max(int(since), 1).to_bytes(4, "big")
        if until is not None:
            until = max(int(until), 1).to_bytes(4, "big")
            add_time = b"\x00%s\x00" % until
        else:
            add_time = b""

        prev = cursor.prev
        get_key = cursor.key

        def skip(key):
            # print(f'skipping to {key}')
            return cursor.set_range(key + b"\xff")

        if matches:
            next_match = iter(compiled_matches).__next__
            match = next_match()
            stop = compiled_matches[-1]
            if since:
                stop += b"\x00%s" % since
            skip(match + add_time)
        else:
            match = None
            if until:
                start = self.prefix + until + b"\xff"
            else:
                start = (int.from_bytes(self.prefix, "big") + 1).to_bytes(1, "big")
            cursor.set_range(start)
            stop = self.prefix
            if since:
                stop += since + b"\xff"
            # print(f'{start} -> {stop}')

        def iterator(match):
            key = bytes(get_key())

            while key > stop:
                if not prev():
                    break
                key = bytes(get_key())
                # print(key, int.from_bytes(key[-37:-33]))

                if match is not None:
                    time_miss = False
                    if since or until:
                        ts = key[-37:-33]
                        if since and ts < since:
                            time_miss = True
                        if until and ts > until:
                            time_miss = True
                        # print(f"miss? {int.from_bytes(ts, 'big')} {time_miss} {int.from_bytes(since)} -> {int.from_bytes(until)}")
                        # if time_miss:
                        #     continue
                    if key[: len(match)] != match or time_miss:
                        try:
                            match = next_match()
                        except StopIteration:
                            break
                        skipped = skip(match + add_time)
                        if skipped:
                            # print(f"found {match} {skipped} {self.cursor.key()}")
                            # yield key[-32:]
                            continue
                        else:
                            break
                yield key[-32:]

        try:
            yield iterator(match)
        finally:
            cursor.close()


class IdIndex(Index):
    prefix = b"\x00"

    def to_key(self, value):
        return self.prefix + bytes.fromhex(value)

    def write(self, event, txn, operation="put"):
        if operation == "put":
            txn.put(self.to_key(event.id), encode_event(event))
        elif operation == "delete":
            txn.delete(self.to_key(event.id))


class CreatedIndex(Index):
    prefix = b"\x01"

    def to_key(self, value):
        return self.prefix + value.to_bytes(4, "big")

    def convert(self, event):
        yield self.to_key(event.created_at)


class KindIndex(Index):
    prefix = b"\x02"

    def to_key(self, value):
        return self.prefix + value.to_bytes(4, "big")

    def convert(self, event):
        yield self.to_key(event.kind)


class PubkeyIndex(Index):
    prefix = b"\x03"

    def to_key(self, value):
        return self.prefix + bytes.fromhex(value)

    def convert(self, event):
        yield self.to_key(event.pubkey)


class TagIndex(Index):
    prefix = b"\x09"

    def to_key(self, value):
        return b"%s%s\x00%s" % (self.prefix, value[0].encode(), value[1].encode())

    def convert(self, event):
        tags = []
        for tag in event.tags:
            if len(tag[0]) == 1 or tag[0] in ("expiration", "delegation"):
                yield self.to_key((tag[0], tag[1]))


class AuthorKindIndex(Index):
    prefix = b"\x04"

    def to_key(self, value):
        return b"%s%s\x00%s" % (
            self.prefix,
            bytes.fromhex(value[0]),
            value[1].to_bytes(4, "big"),
        )

    def convert(self, event):
        yield self.to_key((event.pubkey, event.kind))


INDEXES = {
    "ids": IdIndex(),
    "created_at": CreatedIndex(),
    "kinds": KindIndex(),
    "authors": PubkeyIndex(),
    "authorkinds": AuthorKindIndex(),
    "tags": TagIndex(),
}


class WriterThread(threading.Thread):
    def __init__(self, env, stat_collector):
        super().__init__()
        self.running = True
        self.env = env
        self.stat_collector = stat_collector
        self.queue = SimpleQueue()

    def run(self):
        env = self.env
        qget = self.queue.get
        stat_collector = self.stat_collector
        write_indexes = INDEXES.values()
        log = logging.getLogger("nostr_relay.writer")

        def delete_event(event_id, txn):
            event_data = txn.get(b"\x00%s" % event_id)
            event = decode_event(event_data)
            for index in reversed(write_indexes):
                index.clear(event, txn)

        while self.running:
            task = qget()
            if task is None:
                break
            event, operation = task
            try:
                with stat_collector.timeit("write") as counter:
                    with env.begin(write=True, buffers=True) as txn:
                        if operation == "add":
                            for index in write_indexes:
                                index.write(event, txn)
                            counter["count"] += 1
                            if event.kind in (
                                EventKind.SET_METADATA,
                                EventKind.CONTACTS,
                            ):
                                # delete older metadata events
                                with INDEXES["authorkinds"].scanner(
                                    txn,
                                    [(event.pubkey, event.kind)],
                                    until=event.created_at - 1,
                                ) as scanner:
                                    for event_id in scanner:
                                        log.info("Deleting %s", event_id)
                                        delete_event(event_id, txn)
                                        counter["count"] += 1
                        elif operation == "del":
                            delete_event(bytes.fromhex(event), txn)
                            counter["count"] += 1
            except Exception as e:
                log.exception("writer")


class LMDBStorage(BaseStorage):
    def __init__(self, options):
        self.options = options
        self.options.pop("class")
        self.log = logging.getLogger(__name__)
        self.subscription_class = Subscription

    async def __aenter__(self):
        await self.setup()
        return self

    async def __aexit__(self, ex_type, ex, tb):
        await self.close()

    async def optimize(self):
        pass

    async def close(self):
        self.writer_queue.put(None)
        self.writer_thread.join()
        self.env.close()

    async def setup(self):
        await super().setup()
        self.validate_event = get_validator(
            self.options.pop("validators", ["nostr_relay.validators.is_signed"])
        )
        self.env = lmdb.open(**self.options)
        self.writer_thread = WriterThread(self.env, self.stat_collector)
        self.writer_queue = self.writer_thread.queue
        self.writer_thread.start()
        self.garbage_collector_task = asyncio.create_task(
            KVGarbageCollector(self, collect_interval=300).start()
        )

    def delete_event(self, event_id):
        self.writer_queue.put((event_id, "del"))

    async def add_event(self, event_json: dict, auth_token=None):
        """
        Add an event from json object
        Return (status, event)
        """
        try:
            event = Event(**event_json)
        except Exception as e:
            self.log.error("bad json")
            raise StorageError("invalid: Bad JSON")

        await self.validate_event(event, Config)

        self.writer_queue.put((event, "add"))
        await self.post_save(event)
        return event, True

    async def post_save(self, event):
        query = None
        if event.is_replaceable or event.is_paramaterized_replaceable:
            query = {
                "authors": [event.pubkey],
                "until": event.created_at - 1,
                "kinds": [event.kind],
            }
            if event.is_paramaterized_replaceable:
                query["#d"] = [tag[1] for tag in event.tags if tag[0] == "d"][0]
            self.log.debug(query)
        elif event.kind == EventKind.DELETE:
            # delete the referenced events
            ids = []
            for tag in event.tags:
                name = tag[0]
                if name == "e":
                    ids.append(tag[1])
            query = {
                "authors": [event.pubkey],
                "ids": ids,
                "until": event.created_at - 1,
            }
        if query:
            task, events = executor(self.env, [query], loop=self.loop)
            await task
            for event in events:
                if event is not None:
                    self.delete_event(event.id)
                    self.log.info(
                        "Deleted/Replaced event %s kind=%d pubkey=%s",
                        event.id,
                        event.kind,
                        event.pubkey,
                    )

        self.notify_all_connected(event)
        # notify other processes
        await self.notify_other_processes(event)

    async def get_event(self, event_id: str):
        with self.env.begin(buffers=True) as txn:
            event_data = txn.get(b"\x00%s" % bytes.fromhex(event_id))
            if event_data:
                return decode_event(event_data)

    async def run_single_query(self, filters):
        task, events = executor(self.env, filters, default_limit=600000)
        await task
        for event in events:
            if event is not None:
                yield event

    async def get_identified_pubkey(self, identifier, domain=""):
        data = {"names": {}, "relays": {}}
        return data

    async def get_stats(self):
        stats = {}
        subs = await self.num_subscriptions(True)
        num_subs = 0
        num_clients = 0
        for k, v in subs.items():
            num_clients += 1
            num_subs += v
        stats["active_subscriptions"] = num_subs
        stats["active_clients"] = num_clients
        stats.update(self.env.stat())
        return stats


class Subscription(BaseSubscription):
    __slots__ = (
        "filter_json",
        # "check_event"
    )

    def prepare(self):
        self.query = self.filters
        return True

    async def run_query(self):
        with self.storage.stat_collector.timeit("query") as counter:
            task, events = executor(
                self.storage.env,
                self.query,
                loop=self.storage.loop,
                default_limit=self.default_limit,
            )
            await task

            for event in events:
                await self.queue.put((self.sub_id, event))
                counter["count"] += 1
        self.log.debug("Done with query")


class KVGarbageCollector(Periodic):
    def __init__(self, storage, **kwargs):
        super().__init__(
            kwargs.get("collect_interval", 300),
            run_at_start=False,
            swallow_exceptions=True,
        )
        self.log = logging.getLogger("nostr_relay.kv:gc")
        self.storage = storage

    async def run_once(self):
        self.log.info("Starting gc")

        to_del = []
        with self.storage.env.begin(buffers=False) as txn:
            cursor = txn.cursor()
            start = INDEXES["kinds"].to_key(20000)
            end = INDEXES["kinds"].to_key(29999)
            if cursor.set_range(start):
                for key in cursor.iternext(values=False):
                    if key > end:
                        break
                    event_id = key[-32:].hex()
                    to_del.append(event_id)
        if to_del:
            for event_id in to_del:
                self.storage.delete_event(event_id)
            self.log.info("Deleted %d events", len(to_del))


def planner(filters, default_limit=6000):
    """
    Create a list of QueryPlans for the the list of REQ filters
    """
    plans = []
    for query in filters:
        matches = []
        tags = []
        scores = []
        has_kinds = has_authors = False

        if "ids" in query:
            # if there are ids in the query, always use the id index
            scores.append((100, "ids", query["ids"]))
        else:
            # otherwise, try to choose an index based on cardinality
            for key, value in query.items():
                if key[0] == "#" and len(key) == 2:
                    tag = key[1]
                    tags.extend([(tag, val) for val in value])

                    scores.append((len(tags), "tags", tags))
                elif key == "kinds":
                    scores.append((len(value) * 3, "kinds", value))
                    has_kinds = True
                elif key == "authors":
                    scores.append((len(value) * 3, "authors", value))
                    has_authors = True

        if has_kinds and has_authors:
            idx = "authorkinds"
            kinds = sorted(query["kinds"], reverse=True)
            authorkinds = []
            for author in sorted(query["authors"], reverse=True):
                for k in kinds:
                    authorkinds.append((author, k))
            scores.append((len(authorkinds) * 4, "authorkinds", authorkinds))

        scores.sort(reverse=True)

        if scores:
            topscore, idx, matches = scores[0]
        else:
            idx = "created_at"

        try:
            limit = min(max(0, int(query["limit"])), default_limit)
        except KeyError:
            limit = default_limit

        plans.append(
            QueryPlan(
                query,
                INDEXES[idx],
                matches,
                limit,
                query.get("since"),
                query.get("until"),
            )
        )
    return plans


def matcher(txn, event_id_iterator, query):
    """
    Given an event_id (bytes) iterator, match the events according to the passed in query
    Yields Event objects
    """
    query_items = []
    for key, value in query.items():
        query_items.append((key, tuple(value) if isinstance(value, list) else value))

    match = compile_match_from_query(tuple(query_items))

    def get_event_data(event_id):
        try:
            return unpackb(txn.get(b"\x00%s" % event_id), encoding="utf8")
        except TypeError:
            return None

    hits = misses = 0
    for event_id in event_id_iterator:
        event_tuple = get_event_data(event_id)
        if match(event_tuple):
            event = Event(
                id=event_tuple[1].hex(),
                created_at=event_tuple[2],
                kind=event_tuple[3],
                pubkey=event_tuple[4].hex(),
                content=event_tuple[5],
                tags=event_tuple[6],
                sig=event_tuple[7].hex(),
            )
            yield event
            hits += 1
        else:
            misses += 1


@functools.lru_cache()
def compile_match_from_query(query_items):
    filter_clauses = set()
    for key, value in query_items:
        if key == "ids":
            col = FIELDS_TO_COLUMNS["id"]
            filter_clauses.add(f"(et[{col}].hex() in {value!r})")
        elif key == "authors":
            col = FIELDS_TO_COLUMNS["pubkey"]
            filter_clauses.add(f"(et[{col}].hex() in {value!r})")
        elif key == "kinds":
            col = FIELDS_TO_COLUMNS["kind"]
            filter_clauses.add(f"(et[{col}] in {value!r})")
        elif key == "since" and value:
            col = FIELDS_TO_COLUMNS["created_at"]
            filter_clauses.add(f"(et[{col}] >= {value!r})")
        elif key == "until" and value:
            col = FIELDS_TO_COLUMNS["created_at"]
            filter_clauses.add(f"(et[{col}] <= {value!r})")
        elif key[0] == "#" and len(key) == 2:
            col = FIELDS_TO_COLUMNS["tags"]
            filter_clauses.add(
                f"bool([t for t in et[{col}] if t[0] == {key[1]!r} and len(t) > 1 and t[1] in {value!r}])"
            )
    filter_string = " and ".join(filter_clauses)
    function = f"""
def check(et):
    try:
        return {filter_string}
    except:
        import traceback;traceback.print_exc()
"""
    loc = {}
    # print(function)
    exec(compile(function, "", "exec"), loc)
    return loc["check"]


def executor(lmdb_environment, filters, loop=None, default_limit=6000):
    """
    Execute one or several queries.
    Returns (future, event_list)
    await the future before accessing the list
    """
    plans = planner(filters, default_limit=default_limit)
    loop = loop or asyncio.get_running_loop()

    try:
        # in the common case, when there's only one query,
        # we can avoid some complexity.
        # when there are multiple queries in the filter,
        # we run them simultaneously and collect the results with collect_filters()

        events = collections.deque()

        if len(plans) == 1:
            return (
                loop.run_in_executor(
                    None, execute_one_plan, lmdb_environment, plans[0], events.append
                ),
                events,
            )
        else:
            return (
                loop.run_in_executor(
                    None, execute_many_plans, lmdb_environment, plans, events.append
                ),
                events,
            )
    except:
        traceback.print_exc()


def execute_one_plan(lmdb_environment: lmdb.Environment, plan: QueryPlan, on_event):
    """
    Run a single query plan, calling on_event for each event
    """
    try:
        limit = plan.limit
        with lmdb_environment.begin(buffers=True) as txn:
            with plan.index.scanner(
                txn,
                plan.matches,
                since=plan.since,
                until=plan.until,
            ) as scanner:
                for event in matcher(txn, scanner, plan.query):
                    if limit < 1:
                        break
                    on_event(event)
                    limit -= 1
    except:
        traceback.print_exc()

    finally:
        on_event(None)


def execute_many_plans(lmdb_environment: lmdb.Environment, plans, on_event):
    """
    Run multiple plans and coalesce the results
    """
    try:
        tqueue = SimpleQueue()
        getqueue = tqueue.get

        tasks = []
        for plan in plans:
            tasks.append(
                QUERY_POOL.submit(execute_one_plan, lmdb_environment, plan, tqueue.put)
            )

        event = getqueue()

        to_find = len(filters)
        while True:
            if event is not None:
                on_event(event)
            else:
                to_find -= 1
            if to_find:
                event = getqueue()
            else:
                break
    except:
        traceback.print_exc()
    finally:
        on_event(None)


def encode_event(event):
    row = (
        VERSION,
        event.id_bytes,
        event.created_at,
        event.kind,
        bytes.fromhex(event.pubkey),
        event.content,
        event.tags,
        bytes.fromhex(event.sig),
    )
    return packb(row, use_bin_type=True, encoding="utf8")


def decode_event(data):
    et = unpackb(data, encoding="utf8")
    assert et[0] == 1, "unknown version"
    event = Event(
        id=et[1].hex(),
        created_at=et[2],
        kind=et[3],
        pubkey=et[4].hex(),
        content=et[5],
        tags=et[6],
        sig=et[7].hex(),
    )
    return event


if __name__ == "__main__":
    import sys

    path = sys.argv[1]
    env = lmdb.open(path)
    with env:
        with env.begin() as txn:
            with txn.cursor() as c:
                c.set_range(b"\xff")
                for key in c.iterprev(values=False):
                    print(repr(key)[2:])
