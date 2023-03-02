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
from time import perf_counter, time

from aionostr.event import Event, EventKind
from msgpack import packb, unpackb
from concurrent import futures

from .base import BaseStorage, BaseSubscription, BaseGarbageCollector
from ..auth import get_authenticator
from ..config import Config
from ..errors import StorageError
from ..validators import get_validator


# ids: b'\x00<32 bytes of id>'
# created: b'\x01<4 bytes time>\x00<32 bytes id>'
# kind: b'\x02<4 bytes kind>\x00<32 bytes of id>'
# author: b'\x03<32 bytes pubkey>\x00<32 bytes id>'
# tag: b'\x09<tag string>\x00<tag value>\x00<32 bytes id>'

DONE = object()


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
    "QueryPlan", ("query", "index", "matches", "limit", "since", "until", "stats")
)


class FakeContainer:
    def __contains__(self, anything):
        return True


class Index:
    cardinality = 1

    def __init__(self):
        self.hits = self.misses = 0

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
    def scanner(
        self, txn, matches: list, since=None, until=None, events=FakeContainer()
    ):
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
            found = cursor.set_range(key + b"\xff")
            if found:
                prev()
            return found

        if matches:
            next_match = iter(compiled_matches).__next__
            match = next_match()
            if len(compiled_matches) > 1:
                stop = compiled_matches[-1]
            else:
                stop = self.prefix
            if since:
                stop += b"\x00" + since
            skip(match + add_time)
        else:
            match = None
            if until:
                start = self.prefix + until + b"\x00"
            else:
                start = self.prefix + b"\xff"
            cursor.set_range(start)
            stop = self.prefix
            if since:
                stop += since + b"\xff"
            # print(f'{start} -> {stop}')

        def iterator(match):
            key = bytes(get_key())

            if match is not None:
                matchlen = len(match)
                while match:
                    # breakpoint()
                    ts = key[-37:-33]
                    # print(key, match, ts, since, until)

                    if (
                        key[:matchlen] != match
                        or (since and ts < since)
                        or (until and ts > until)
                    ):
                        try:
                            match = next_match()
                            matchlen = len(match)
                        except StopIteration:
                            break

                        skipped = skip(match + add_time)
                        if skipped:
                            # print(f"found {match} {skipped} {cursor.key()}")
                            key = bytes(get_key())
                            continue
                        else:
                            break
                    elif key < stop:
                        break

                    if key[-32:] in events:
                        yield key[-32:]
                    if not prev():
                        break
                    key = bytes(get_key())

            else:
                while key > stop:
                    # print(key, int.from_bytes(key[-37:-33]))
                    # breakpoint()
                    if key[0:1] == self.prefix:
                        yield key[-32:]

                    if not prev():
                        break
                    key = bytes(get_key())

        try:
            yield iterator(match)
        finally:
            cursor.close()


class IdIndex(Index):
    prefix = b"\x00"
    cardinality = 1000

    def to_key(self, value):
        return self.prefix + bytes_from_hex(value)

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
        return self.prefix + bytes_from_hex(value)

    def convert(self, event):
        yield self.to_key(event.pubkey)


class TagIndex(Index):
    prefix = b"\x09"
    cardinality = 100

    def to_key(self, value):
        return b"%s%s\x00%s" % (self.prefix, value[0].encode(), value[1].encode())

    def convert(self, event):
        tags = []
        for tag in event.tags:
            if len(tag[0]) == 1 or tag[0] in ("expiration", "delegation"):
                yield self.to_key((tag[0], str(tag[1])))


class AuthorKindIndex(Index):
    prefix = b"\x04"
    cardinality = 20

    def to_key(self, value):
        return b"%s%s\x00%s" % (
            self.prefix,
            bytes_from_hex(value[0]),
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


class MultiIndex:
    __slots__ = ("indexes", "_has_ids")
    """
    This class is a container for multiple indexes.
    The `scanner()` method will chain the results of each index.
    """

    def __init__(self):
        self.indexes = []
        self._has_ids = None

    def __repr__(self):
        return f"MultiIndex({[i[0].__class__.__name__ for i in self.indexes]})"

    def sort_key(self, item):
        index, matches = item
        key = index.cardinality * len(matches)
        return key

    def add(self, indexname: str, matches: list):
        self.indexes.append((INDEXES[indexname], matches))
        if indexname == "ids":
            self._has_ids = len(self.indexes) - 1

    def finalize(self):
        # if there are no matches, this is a date range scan
        if not self.indexes:
            best_index = INDEXES["created_at"]
            matches = []
        elif len(self.indexes) == 1:
            # if there's only one index needed in the query,
            # just use that one instead of going through MultiIndex
            best_index, matches = self.indexes.pop(0)
        elif self._has_ids is not None:
            # if there are ids in the query, use them instead of anything else
            best_index, matches = self.indexes[self._has_ids]
        else:
            self.indexes.sort(key=self.sort_key, reverse=True)
            best_index = self
            matches = [k[1] for k in self.indexes]
        return best_index, matches

    @contextmanager
    def scanner(self, txn, matches: list, since=None, until=None, events=None):
        def iterator(events):
            for (index, _), imatches in zip(self.indexes, matches):
                with index.scanner(
                    txn, imatches, since=since, until=until, events=events
                ) as scanner:
                    events = set(scanner)
            yield from events

        yield iterator(events or FakeContainer())


class WriterThread(threading.Thread):
    def __init__(self, env, stat_collector):
        super().__init__()
        self.running = True
        self.env = env
        self.stat_collector = stat_collector
        self.queue = SimpleQueue()
        self.write_indexes = INDEXES.values()

    def run(self):
        env = self.env
        qget = self.queue.get
        qsize = self.queue.qsize
        stat_collector = self.stat_collector
        log = logging.getLogger("nostr_relay.writer")

        while self.running:
            task = qget()
            if task is None:
                break
            event, operation = task
            try:
                with stat_collector.timeit("write") as counter:
                    with env.begin(write=True, buffers=True) as txn:
                        if operation == "add" and not get_event_data(
                            txn, event.id_bytes
                        ):
                            for index in self.write_indexes:
                                index.write(event, txn)
                                # log.debug("index %s event %s", index, event)
                            self._post_save(txn, event, counter, log)
                        elif operation == "del":
                            event = decode_event(
                                get_event_data(txn, bytes.fromhex(event))
                            )
                            if event:
                                self._delete_event(txn, event, log)
                        counter["count"] += 1
                qs = qsize()
                if qs >= 1000 and qs % 1000 == 0:
                    # since we can do about 1,000 writes per second (end-to-end),
                    # this would indicate very heavy load
                    log.warning("Write queue size: %d", qs)
            except Exception as e:
                log.exception("writer")

    def _delete_event(self, txn, event, log):
        for index in reversed(self.write_indexes):
            index.clear(event, txn)
        log.info(
            "Deleted event %s kind=%d pubkey=%s", event.id, event.kind, event.pubkey
        )

    def _post_save(self, txn, event, counter, log):
        if (
            event.kind
            in (
                EventKind.SET_METADATA,
                EventKind.CONTACTS,
            )
            or event.is_replaceable
            or event.is_paramaterized_replaceable
        ):
            saved_id = event.id_bytes
            until = event.created_at - 1
            if event.is_paramaterized_replaceable:
                d_tag = [tag[1] for tag in event.tags if tag[0] == "d"][0]
            else:
                d_tag = None

            # delete older replaceable events
            with INDEXES["authorkinds"].scanner(
                txn,
                [(event.pubkey, event.kind)],
                until=until,
            ) as scanner:
                for event_id in scanner:
                    if event_id == saved_id:
                        continue
                    candidate = decode_event(get_event_data(txn, event_id))
                    if d_tag is not None:
                        if not all(candidate.has_tag("d", d_tag)):
                            continue
                    self._delete_event(txn, candidate, log)
                    counter["count"] += 1

        elif event.kind == EventKind.DELETE:
            # delete the referenced events
            ids = set((bytes_from_hex(tag[1]) for tag in event.tags if tag[0] == "e"))
            if not ids:
                return
            with INDEXES["authors"].scanner(
                txn,
                [event.pubkey],
                until=event.created_at - 1,
            ) as scanner:
                for event_id in scanner:
                    if event_id in ids:
                        candidate = decode_event(get_event_data(txn, event_id))
                        if candidate:
                            self._delete_event(txn, candidate, log)
                            counter["count"] += 1


class LMDBStorage(BaseStorage):
    DEFAULT_GARBAGE_COLLECTOR = "nostr_relay.storage.kv.KVGarbageCollector"

    def __init__(self, options):
        super().__init__(options)
        pool_size = self.options.pop("pool_size", 20)
        self.options.pop("class")
        # set this to the number of read threads
        self.options.setdefault("max_spare_txns", pool_size)
        self.log = logging.getLogger(__name__)
        self.subscription_class = Subscription
        self.db = None
        self.query_pool = futures.ThreadPoolExecutor(max_workers=pool_size)

    async def close(self):
        if self.db:
            if self.garbage_collector_task:
                self.garbage_collector_task.cancel()
            self.writer_queue.put(None)
            self.writer_thread.join()
            self.db.close()
            self.db = None
            self.log.debug("Closed storage %s", self.options["path"])

    async def setup(self):
        await super().setup()
        self.validate_event = get_validator(
            self.options.pop("validators", ["nostr_relay.validators.is_signed"])
        )
        self.db = lmdb.open(**self.options)
        self.write_tombstone()
        self.writer_thread = WriterThread(self.db, self.stat_collector)
        self.writer_queue = self.writer_thread.queue
        self.writer_thread.start()

    def write_tombstone(self):
        """
        Write a record at the end of the db, to avoid a weird edge case
        """
        with self.db.begin(write=True) as txn:
            txn.put(b"\xee", b"")

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

    async def post_save(self, event, **kwargs):
        await self.notify_all_connected(event)
        # notify other processes
        await self.notify_other_processes(event)

    async def get_event(self, event_id: str):
        with self.db.begin(buffers=True) as txn:
            return decode_event(get_event_data(txn, bytes.fromhex(event_id)))

    async def run_single_query(self, filters):
        if isinstance(filters, dict):
            filters = [filters]
        task, events = executor(
            self.db, filters, default_limit=600000, pool=self.query_pool
        )
        await task
        for event in events:
            if event is not None:
                yield event
        analyze(task, loop=self.loop, pool=self.query_pool)

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
        stats.update(self.db.stat())
        return stats


class Subscription(BaseSubscription):
    __slots__ = (
        "filter_json",
        # "check_event"
    )

    def prepare(self):
        self.query = planner(
            self.filters, default_limit=self.default_limit, log=self.log
        )
        return bool(self.query)

    async def run_query(self):
        check_output = self.storage.check_output
        sub_id = self.sub_id
        queue = self.queue
        with self.storage.stat_collector.timeit("query") as counter:
            task, events = executor(
                self.storage.db,
                self.query,
                loop=self.storage.loop,
                default_limit=self.default_limit,
                log=self.log,
                pool=self.storage.query_pool,
            )

            # we could start consuming events before all queries are complete,
            # but it creates more complexity and is actually slower because of thread contention
            await task
            try:
                if check_output:
                    context = {
                        "config": Config,
                        "client_id": self.client_id,
                        "auth_token": self.auth_token,
                    }
                    for event in events:
                        if check_output(event, context):
                            await queue.put((sub_id, event))
                            counter["count"] += 1
                else:
                    for event in events:
                        await queue.put((sub_id, event))
                        counter["count"] += 1
            except:
                self.log.exception("run_query")
            finally:
                await queue.put((sub_id, None))
                analyze(task, loop=loop, pool=self.storage.query_pool)

        self.log.debug("Done with query")


class KVGarbageCollector(BaseGarbageCollector):
    def __init__(self, storage, **kwargs):
        super().__init__(
            storage,
            async_transaction=False,
            **kwargs,
        )

    async def collect(self, conn):
        to_del = []
        cursor = conn.cursor()
        # remove all ephemeral events
        start = INDEXES["kinds"].to_key(20000)
        end = INDEXES["kinds"].to_key(29999)
        if cursor.set_range(start):
            for key in cursor.iternext(values=False):
                if key > end:
                    break
                event_id = key[-32:].hex()
                to_del.append(event_id)
        # remove all expired events
        start = INDEXES["tags"].to_key(("expiration", "0"))
        end = INDEXES["tags"].to_key(("expiration", str(int(time()))))
        if cursor.set_range(start):
            for key in cursor.iternext(values=False):
                if key > end:
                    break
                event_id = key[-32:].hex()
                to_del.append(event_id)

        cursor.close()
        if to_del:
            for event_id in to_del:
                self.storage.delete_event(event_id)
        return len(to_del)


def planner(filters, default_limit=6000, log=None):
    """
    Create a list of QueryPlans for the the list of REQ filters
    """
    plans = []
    for query in filters:
        if isinstance(query, QueryPlan):
            plans.append(query)
            continue
        if not query:
            if log:
                log.info("No empty queries allowed")
            continue
        tags = set()
        query_items = []

        since = query.pop("since", None)
        if since is not None:
            query_items.append(("since", since))
        until = query.pop("until", None)
        if until is not None:
            query_items.append(("until", until))
        try:
            limit = min(max(0, int(query.pop("limit"))), default_limit)
        except KeyError:
            limit = default_limit

        best_index = MultiIndex()
        if "ids" in query:
            ids = tuple(
                sorted((i for i in query.pop("ids") if len(i) >= 2), reverse=True)
            )
            if ids:
                query_items.append(("ids", ids))
                best_index.add("ids", ids)
            else:
                continue
        if "kinds" in query and "authors" in query:
            kinds = tuple(sorted(query.pop("kinds"), reverse=True))
            authors = tuple(
                sorted(
                    (a for a in query.pop("authors") if len(a) >= 4 and a[0] != "n"),
                    reverse=True,
                )
            )
            authormatches = []
            for author in authors:
                for k in kinds:
                    authormatches.append((author, k))
            if authormatches:
                query_items.append(("kinds", kinds))
                query_items.append(("authors", authors))
                best_index.add("authorkinds", authormatches)
            else:
                continue
        elif "kinds" in query:
            kinds = tuple(sorted(query.pop("kinds"), reverse=True))
            if kinds:
                query_items.append(("kinds", kinds))
                best_index.add("kinds", kinds)
            else:
                continue
        elif "authors" in query:
            authors = tuple(
                sorted(
                    (a for a in query.pop("authors") if len(a) >= 4 and a[0] != "n"),
                    reverse=True,
                )
            )
            if authors:
                query_items.append(("authors", authors))
                best_index.add("authors", authors)
            else:
                continue

        for key, value in query.items():
            if key[0] == "#" and len(key) == 2 and isinstance(value, list):
                tag = key[1]
                tags.update((tag, str(val)) for val in value)
                query_items.append((key, tuple(value)))
        if tags:
            best_index.add("tags", sorted(tags, reverse=True))

        query_items = tuple(query_items)

        # set the final order of the multiindex
        best_index, matches = best_index.finalize()

        if best_index is INDEXES["created_at"] and not (since or until):
            # don't allow range scans
            if log:
                log.info("No range scans allowed %s", query_items)
            continue

        plan = QueryPlan(
            query_items,
            best_index,
            matches,
            limit,
            since,
            until,
            {},
        )
        if log:
            log.debug("Plan: %s.", plan)
        plans.append(plan)
    return plans


def matcher(txn, event_id_iterator, query_items: tuple, stats: dict):
    """
    Given an event_id (bytes) iterator, match the events according to the passed in query
    Yields Event objects
    """
    match = compile_match_from_query(query_items)

    stats["index_hits"] = stats["index_misses"] = 0
    for event_id in event_id_iterator:
        event_tuple = get_event_data(txn, event_id)
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
            stats["index_hits"] += 1
        else:
            stats["index_misses"] += 1


@functools.lru_cache()
def compile_match_from_query(query_items):
    filter_clauses = set()
    for key, value in query_items:
        if key == "ids":
            col = FIELDS_TO_COLUMNS["id"]
            if all(len(v) == 64 for v in value):
                filter_clauses.add(f"(et[{col}].hex() in {value!r})")
            else:
                filter_clauses.add(
                    f"any(et[{col}].hex().startswith(v) for v in {value!r})"
                )
        elif key == "authors":
            col = FIELDS_TO_COLUMNS["pubkey"]
            if all(len(v) == 64 for v in value):
                filter_clauses.add(f"(et[{col}].hex() in {value!r})")
            else:
                filter_clauses.add(
                    f"any(et[{col}].hex().startswith(v) for v in {value!r})"
                )
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


def executor(
    lmdb_environment: lmdb.Environment,
    filters: list,
    loop=None,
    default_limit=6000,
    log=None,
    pool=None,
):
    """
    Execute one or several queries.
    Returns (future, event_list)
    await the future before accessing the list
    """
    log = log or logging.getLogger("nostr_relay.kvquery")

    plans = planner(filters, default_limit=default_limit, log=log)
    loop = loop or asyncio.get_running_loop()

    try:
        # collections.deque is threadsafe for appends and pops
        # and faster than using a Queue
        events = collections.deque()

        tasks = [
            loop.run_in_executor(
                pool,
                execute_one_plan,
                lmdb_environment,
                plan,
                events.append,
                log,
            )
            for plan in plans
        ]

        if len(tasks) == 1:
            task = tasks[0]
        else:
            task = asyncio.gather(*tasks)

        return task, events
    except:
        log.exception("executor")


def execute_one_plan(
    lmdb_environment: lmdb.Environment, plan: QueryPlan, on_event, log
):
    """
    Run a single query plan, calling on_event for each event
    """
    try:
        limit = plan.limit
        count = 0
        plan.stats["start"] = perf_counter()
        with lmdb_environment.begin(buffers=True) as txn:
            with plan.index.scanner(
                txn,
                plan.matches,
                since=plan.since,
                until=plan.until,
            ) as scanner:
                for event in matcher(txn, scanner, plan.query, plan.stats):
                    if count == limit:
                        break
                    on_event(event)
                    count += 1
        plan.stats["count"] = count
        plan.stats["end"] = perf_counter()
    except:
        log.exception("execute_one_plan")

    finally:
        return plan, log


SLOW_QUERY_THRESHOLD = 500


def _analyze(task):
    result = task.result()
    if not isinstance(result, list):
        plans, log = [result[0]], result[1]

    else:
        plans, log = [r[0] for r in result], result[0][1]

    for plan in plans:
        log.debug("Executed plan. Stats: %s", plan.stats)
        stats = plan.stats
        duration = (stats["end"] - stats["start"]) * 1000  # milliseconds
        if duration > SLOW_QUERY_THRESHOLD:
            log.info(
                "Slowish query: %s since:%s until:%s – took %.2fms",
                plan.query,
                plan.since,
                plan.until,
                duration,
            )

            if stats["index_misses"] > stats["index_hits"]:
                total = stats["index_hits"] + stats["index_misses"]
                log.info(
                    "Misses for query %s since:%s until:%s – %s/%s",
                    plan.query,
                    plan.since,
                    plan.until,
                    stats["index_misses"],
                    total,
                )


def analyze(task, loop=None, pool=None):
    """
    Analyze and log query stats from the completed task
    """
    loop = loop or asyncio.get_running_loop()
    loop.run_in_executor(pool, _analyze, task)


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
    return packb(row, use_bin_type=True)


def decode_event(data):
    if data:
        assert data[0] == 1, "unknown version"
        event = Event(
            id=data[1].hex(),
            created_at=data[2],
            kind=data[3],
            pubkey=data[4].hex(),
            content=data[5],
            tags=data[6],
            sig=data[7].hex(),
        )
        return event


def get_event_data(txn, event_id: bytes):
    try:
        return unpackb(txn.get(b"\x00" + event_id), use_list=False)
    except TypeError:
        return None


def bytes_from_hex(hexstr: str):
    try:
        return bytes.fromhex(hexstr)
    except ValueError:
        if len(hexstr) >= 4 and len(hexstr) % 2 != 0:
            hexstr = hexstr[:-1]
            return bytes.fromhex(hexstr)
        else:
            raise


if __name__ == "__main__":
    import sys

    path = sys.argv[1]
    env = lmdb.open(path)
    with env:
        with env.begin() as txn:
            with txn.cursor() as c:
                c.set_range(b"\xff")
                mapping = {i.prefix: i.__class__.__name__ for i in INDEXES.values()}

                for key in c.iterprev(values=False):
                    idx = key[0:1]
                    event_id = key[-32:].hex()
                    s = f"{mapping[idx]} {key[1:-32]} {event_id}"
                    print(s)
