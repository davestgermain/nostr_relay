"""
Experimental storage using LMDB
"""

import lmdb
import logging
import asyncio
import collections
import threading
import functools
import typing
import re
import os.path
import queue

from contextlib import contextmanager
from time import perf_counter, time

from aionostr.event import Event, EventKind
from msgpack import packb, unpackb
from concurrent import futures

from .base import (
    BaseStorage,
    BaseSubscription,
    BaseGarbageCollector,
    NostrQuery,
    ValidationError,
)
from ..config import Config
from ..errors import StorageError


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


class QueryPlans(list):
    """
    Container for QueryPlan objects
    """

    __slots__ = ("__weakref__",)


class FakeContainer:
    def __contains__(self, anything):
        return True


class Index:
    cardinality = 1
    enabled = True
    prefix = b""

    def __init__(self):
        self.hits = self.misses = 0

    def write(self, event: Event, txn, operation="put"):
        event_id = event.id_bytes
        ctime = event.created_at.to_bytes(4, "big")
        func = getattr(txn, operation)
        for key in self.convert(event):
            to_save = b"%s\x00%s\x00%s" % (key, ctime, event_id)
            func(to_save, b"")

    def clear(self, event: Event, txn):
        self.write(event, txn, operation="delete")

    def bulk_update(self, events, txn):
        for event in events:
            if event:
                self.write(event, txn)

    def to_key(self, match) -> bytes:
        return match

    def __repr__(self):
        return f"{self.__class__.__name__}({self.prefix})"

    @contextmanager
    def scanner(
        self,
        txn,
        matches: list,
        since: typing.Optional[int] = None,
        until: typing.Optional[int] = None,
        events=FakeContainer(),
    ):
        cursor = txn.cursor()
        # compile the matches to the expected format for the index,
        # to make substring checks quicker
        compiled_matches = []
        for match in matches:
            try:
                compiled_matches.append(self.to_key(match))
            except ValueError:
                pass
        if since is not None:
            since = since.to_bytes(4, "big")
        if until is not None:
            until = until.to_bytes(4, "big")
            add_time = b"\x00%s\x00" % until
        else:
            add_time = b""

        prev = cursor.prev
        get_key = cursor.key

        if compiled_matches:
            matchiter = iter(compiled_matches)

            def next_match():
                try:
                    match = next(matchiter)
                except StopIteration:
                    return None, None
                skipped = cursor.set_range(match + add_time + b"\xff")
                if skipped:
                    prev()
                return match, skipped

            if len(compiled_matches) > 1:
                stop = compiled_matches[-1]
            else:
                stop = self.prefix
            if since:
                stop += b"\x00" + since
            match, skipped = next_match()
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
                        match, skipped = next_match()

                        if match is None:
                            break
                        else:
                            matchlen = len(match)

                        if skipped:
                            key = bytes(get_key())
                            continue
                        else:
                            break
                    elif key < stop:
                        break

                    event_id = key[-32:]
                    if event_id in events:
                        yield event_id
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

    def to_key(self, value) -> bytes:
        return self.prefix + bytes_from_hex(value)

    def write(self, event: Event, txn, operation="put"):
        if operation == "put":
            txn.put(self.to_key(event.id), encode_event(event))
        elif operation == "delete":
            txn.delete(self.to_key(event.id))


class CreatedIndex(Index):
    prefix = b"\x01"

    def to_key(self, value) -> bytes:
        return self.prefix + value.to_bytes(4, "big")

    def convert(self, event: Event):
        yield self.to_key(event.created_at)


class KindIndex(Index):
    prefix = b"\x02"

    def to_key(self, value) -> bytes:
        return self.prefix + value.to_bytes(4, "big")

    def convert(self, event: Event):
        yield self.to_key(event.kind)


class PubkeyIndex(Index):
    prefix = b"\x03"

    def to_key(self, value) -> bytes:
        return self.prefix + bytes_from_hex(value)

    def convert(self, event: Event):
        yield self.to_key(event.pubkey)


class TagIndex(Index):
    prefix = b"\x09"
    cardinality = 100

    def to_key(self, value: tuple[str, str]) -> bytes:
        return b"%s%s\x00%s" % (self.prefix, value[0].encode(), value[1].encode())

    def convert(self, event: Event):
        for tag in event.tags:
            if len(tag) >= 2 and (
                len(tag[0]) == 1 or tag[0] in ("expiration", "delegation")
            ):
                yield self.to_key((tag[0], str(tag[1])))


class AuthorKindIndex(Index):
    prefix = b"\x04"
    cardinality = 20

    def to_key(self, value: tuple[str, int]) -> bytes:
        return b"%s%s\x00%s" % (
            self.prefix,
            bytes_from_hex(value[0]),
            value[1].to_bytes(4, "big"),
        )

    def convert(self, event: Event):
        yield self.to_key((event.pubkey, event.kind))


class FTSIndex(Index):
    """
    Full-text index, using whoosh
    """

    cardinality = 1000

    def __init__(self, index_path=Config.fts_index_path, enabled=Config.fts_enabled):
        self.index_path = index_path
        self._schema = None
        self._index = None

        self.log = logging.getLogger("nostr_relay.fts")
        self.enabled = Config.fts_enabled
        self.fts_kinds = Config.get("fts_kinds", (1, 0))

    @property
    def schema(self):
        if self._schema is None:
            from whoosh.fields import Schema, TEXT, ID, NUMERIC
            from whoosh.analysis import StemmingAnalyzer

            self._schema = Schema(
                id=ID(stored=True, unique=True),
                created_at=NUMERIC(stored=True),
                content=TEXT(analyzer=StemmingAnalyzer()),
            )
        return self._schema

    @property
    def index(self):
        if self._index is None:
            storage_path = self.index_path or os.path.join(
                Config.storage["path"], "fts"
            )
            from whoosh.filedb.filestore import FileStorage
            from whoosh.index import EmptyIndexError

            storage = FileStorage(storage_path)
            storage.create()
            try:
                self._index = storage.open_index()
            except EmptyIndexError:
                storage.create()
                self._index = storage.create_index(self.schema)
        return self._index

    def _index_item(self, writer, event: Event):
        writer.update_document(
            id=event.id, content=event.content, created_at=event.created_at
        )
        self.log.debug("Indexed %s", event.id)

    def write(self, event: Event, txn):
        if event.kind in self.fts_kinds:
            from whoosh.writing import AsyncWriter

            with AsyncWriter(self.index) as writer:
                self._index_item(writer, event)

    def bulk_update(self, events, txn):
        from whoosh.writing import BufferedWriter

        with BufferedWriter(self.index, limit=200) as writer:
            for event in events:
                if event:
                    self._index_item(writer, event)
            writer.commit()

    def clear(self, event: Event, txn):
        if event.kind in self.fts_kinds:
            with self.index.writer() as writer:
                writer.delete_by_term("id", event.id)

    @contextmanager
    def scanner(
        self,
        txn,
        matches: list,
        since=None,
        until=None,
        events=FakeContainer(),
        limit=500,
    ):
        from whoosh.query import NumericRange
        from whoosh.qparser import QueryParser

        qp = QueryParser("content", schema=self.schema)
        nr = NumericRange("created_at", since or 1, until or int(time()))

        def iterator():
            with self.index.searcher() as searcher:
                for query in matches:
                    q = qp.parse(query) & nr
                    results = searcher.search(q, limit=limit)

                    for result in results:
                        event_id = bytes.fromhex(result["id"])
                        if event_id in events:
                            yield event_id

        yield iterator()


INDEXES = {
    "ids": IdIndex(),
    "created_at": CreatedIndex(),
    "kinds": KindIndex(),
    "authors": PubkeyIndex(),
    "authorkinds": AuthorKindIndex(),
    "tags": TagIndex(),
    "search": FTSIndex(),
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

    def sort_key(self, item: tuple[Index, list]):
        index, matches = item
        key = index.cardinality * len(matches)
        return key

    def add(self, indexname: str, matches: list):
        self.indexes.append((INDEXES[indexname], matches))
        if indexname == "ids":
            self._has_ids = len(self.indexes) - 1

    def finalize(self) -> tuple[Index, list]:
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
                    # if there are no events further up the chain,
                    # any index after this will not match anything
                    if not events:
                        break
            yield from events

        yield iterator(events or FakeContainer())


class WriterThread(threading.Thread):
    def __init__(self, env, stat_collector):
        super().__init__()
        self.running = True
        self.env = env
        self.stat_collector = stat_collector
        self.queue = queue.SimpleQueue()
        self.write_indexes = [i for i in INDEXES.values() if i.enabled]
        self.processing = False

    def run(self):
        env = self.env
        qget = self.queue.get
        qsize = self.queue.qsize
        stat_collector = self.stat_collector
        log = logging.getLogger("nostr_relay.writer")

        while self.running:
            task = qget()
            self.processing = True
            if task is None:
                break
            operation, args = task
            try:
                with stat_collector.timeit("write") as counter:
                    with env.begin(write=True, buffers=True) as txn:
                        if operation == "add" and not get_event_data(
                            txn, args[0].id_bytes
                        ):
                            event = args[0]
                            for index in self.write_indexes:
                                index.write(event, txn)
                                # log.debug("index %s event %s", index, event)
                            self._post_save(txn, event, counter, log)
                        elif operation == "del":
                            event = decode_event(
                                get_event_data(txn, bytes.fromhex(args[0]))
                            )
                            if event:
                                self._delete_event(txn, event, log)
                        elif operation == "reindex":
                            index_name, event = args
                            INDEXES[index_name].write(event, txn)
                        elif operation == "bulk_update":
                            index_name, events = args
                            INDEXES[index_name].bulk_update(events, txn)
                        counter["count"] += 1
                qs = qsize()
                if qs >= 1000 and qs % 1000 == 0:
                    # since we can do about 1,000 writes per second (end-to-end),
                    # this would indicate very heavy load
                    log.warning("Write queue size: %d", qs)
            except Exception:
                log.exception("writer")
            finally:
                self.processing = False

    def _delete_event(self, txn, event: Event, log):
        for index in reversed(self.write_indexes):
            index.clear(event, txn)
        log.info(
            "Deleted event %s kind=%d pubkey=%s", event.id, event.kind, event.pubkey
        )

    def _post_save(self, txn, event: Event, counter, log):
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
            event.created_at - 1
            if event.is_paramaterized_replaceable:
                try:
                    d_tag = [tag[1] for tag in event.tags if tag[0] == "d"][0]
                except IndexError:
                    d_tag = None
            else:
                d_tag = None

            # delete older replaceable events
            with INDEXES["authorkinds"].scanner(
                txn,
                [(event.pubkey, event.kind)],
                until=event.created_at,
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
            try:
                ids = set(
                    (bytes_from_hex(tag[1]) for tag in event.tags if tag[0] == "e")
                )
            except IndexError:
                ids = []
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
    """
    LMDB Storage backend
    """

    DEFAULT_GARBAGE_COLLECTOR = "nostr_relay.storage.kv.KVGarbageCollector"

    def __init__(self, options):
        super().__init__(options)
        pool_size = self.options.pop("pool_size", 8)
        self.options.pop("class")
        # set this to the number of read threads
        self.options.setdefault("max_spare_txns", pool_size + 1)
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
            self.query_pool.shutdown()
            self.db.close()
            self.db = None
            self.log.debug("Closed storage %s", self.options["path"])

    async def setup(self):
        await super().setup()
        self.db = lmdb.open(**self.options)
        self.write_tombstone()
        self.writer_thread = WriterThread(self.db, self.stat_collector)
        self.writer_queue = self.writer_thread.queue
        self.writer_thread.start()

    async def wait_for_writer(self):
        """
        Wait for the writer thread to be idle.
        ONLY USE THIS FOR TESTING
        """
        await asyncio.sleep(0.1)
        while self.writer_thread.processing or not self.writer_queue.empty():
            await asyncio.sleep(0.1)

    def write_tombstone(self):
        """
        Write a record at the end of the db, to avoid a weird edge case
        """
        with self.db.begin(write=True) as txn:
            txn.put(b"\xee", b"")

    async def delete_event(self, event_id: str):
        self.writer_queue.put(("del", [event_id]))

    async def add_event(
        self, event_json: dict, auth_token: typing.Optional[dict] = None
    ) -> tuple[Event, bool]:
        """
        Add an event from json object
        Return (event, status)
        """
        try:
            event = Event(**event_json)
        except Exception:
            self.log.error("bad json")
            raise StorageError("invalid: Bad JSON")

        await self.validate_event(event, Config)

        if not event.is_ephemeral:
            self.writer_queue.put(("add", [event]))
        await self.post_save(event)
        return event, True

    async def post_save(self, event: Event, **kwargs):
        await self.notify_all_connected(event)
        # notify other processes
        await self.notify_other_processes(event)

    async def reindex(
        self, index_name: str, batch_size=500, kinds=(1, 0), since=1, until=0
    ):
        query = {
            "kinds": kinds,
            "limit": batch_size,
            "until": until or int(time()) + 10,
            "since": since,
        }
        self.log.info(
            "Starting reindex for %s from %d to %d",
            index_name,
            query["since"],
            query["until"],
        )
        full_count = 0
        while True:
            async for plan, events in executor(
                self.db, [query], self.query_pool, default_limit=batch_size
            ):
                while self.writer_queue.qsize() >= 1:
                    await asyncio.sleep(2.0)
                self.writer_queue.put(("bulk_update", [index_name, events]))
                full_count += len(events)
                self.log.info(
                    "Bulk updating %s index. batch size: %d/%d count: %d",
                    index_name,
                    len(events),
                    batch_size,
                    full_count,
                )
            if len(events) == batch_size:
                query["until"] = events[-1].created_at + 2
            else:
                break
        self.log.info(
            "Done bulk updating %s. %d events indexed", index_name, full_count
        )

    async def get_event(self, event_id: str) -> Event:
        with self.db.begin(buffers=True) as txn:
            return decode_event(get_event_data(txn, bytes.fromhex(event_id)))

    async def run_single_query(self, filters: list[NostrQuery]):
        if isinstance(filters, dict):
            filters = [filters]
        plans = QueryPlans()

        async for plan, events in executor(
            self.db, filters, self.query_pool, default_limit=600000, log=self.log
        ):
            for event in events:
                if event is not None:
                    yield event
            plans.append(plan)
        analyze(plans, log=self.log)

    async def get_stats(self) -> dict:
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
    def prepare(self):
        self.query = planner(self.filters, log=self.log)
        return bool(self.query)

    async def run_query(self):
        check_output = self.storage.check_output
        sub_id = self.sub_id
        queue_put = self.queue.put
        with self.storage.stat_collector.timeit("query") as counter:
            plans = QueryPlans()
            iterator = executor(
                self.storage.db,
                self.query,
                self.storage.query_pool,
                log=self.log,
                loop=self.storage.loop,
            )
            try:
                if check_output:
                    context = {
                        "config": Config,
                        "client_id": self.client_id,
                        "auth_token": self.auth_token,
                    }
                    async for plan, events in iterator:
                        for event in events:
                            if check_output(event, context):
                                await queue_put((sub_id, event))
                                counter["count"] += 1
                        plans.append(plan)
                else:
                    async for plan, events in iterator:
                        for event in events:
                            await queue_put((sub_id, event))
                            counter["count"] += 1
                        plans.append(plan)
            except asyncio.exceptions.CancelledError:
                # cancellations are normal
                self.log.debug("Cancelled run_query")
            except Exception:
                self.log.exception("run_query")
            finally:
                await queue_put((sub_id, None))
                analyze(plans)

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
                await self.storage.delete_event(event_id)
        return len(to_del)


def planner(
    filters: list[NostrQuery], default_limit=None, log=None, maximum_plans=5
) -> QueryPlans:
    """
    Create a list of QueryPlans for the the list of REQ filters
    """
    plans = QueryPlans()
    for query in filters[:maximum_plans]:
        if isinstance(query, QueryPlan):
            plans.append(query)
            continue
        elif not isinstance(query, NostrQuery):
            if not query:
                if log:
                    log.info("No empty queries allowed")
                continue
            try:
                query = NostrQuery.model_validate(query)
            except ValidationError as e:
                if log:
                    log.info("invalid query %s", e)
                continue

        query_items = []

        if query.since is not None:
            query_items.append(("since", query.since))

        if query.until is not None:
            query_items.append(("until", query.until))

        best_index = MultiIndex()
        has_authors = has_kinds = False
        if query.ids is not None:
            ids = tuple(query.ids)
            if ids:
                query_items.append(("ids", ids))
                best_index.add("ids", ids)
            else:
                continue
        if query.kinds is not None:
            kinds = tuple(query.kinds)
            if kinds:
                query_items.append(("kinds", kinds))
                has_kinds = True
            else:
                continue
        if query.authors is not None:
            authors = tuple(query.authors)
            if authors:
                query_items.append(("authors", authors))
                has_authors = True
            else:
                continue

        if has_kinds and has_authors:
            authormatches = []
            for author in authors:
                for k in kinds:
                    authormatches.append((author, k))
            best_index.add("authorkinds", authormatches)
        elif has_kinds:
            best_index.add("kinds", kinds)
        elif has_authors:
            best_index.add("authors", authors)

        if query.search is not None and Config.fts_enabled:
            # NIP-50 specifies name:value as extensions
            # let's just remove them from the query
            search_term = re.sub(r"([\w]+:[\w]+)", "", query.search).strip()
            if len(search_term) > 3:
                query_items.append(("search", search_term))
                best_index.add("search", (search_term,))

        if query.tags:
            tags = set()
            has_empty_tags = False
            for tag, values in query.tags:
                if not values:
                    has_empty_tags = True
                    break
                for val in values:
                    tags.add((tag, val))
                query_items.append((tag, tuple(values)))
            if has_empty_tags:
                continue
            best_index.add("tags", sorted(tags, reverse=True))

        query_items = tuple(query_items)

        # set the final order of the multiindex
        best_index, matches = best_index.finalize()

        if best_index is INDEXES["created_at"] and not (query.since or query.until):
            # don't allow range scans
            if log:
                log.info("No range scans allowed %s", query_items)
            continue

        plan = QueryPlan(
            query_items,
            best_index,
            matches,
            default_limit or query.limit,
            query.since,
            query.until,
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
        if event_tuple and match(event_tuple):
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
def compile_match_from_query(query_items: tuple):
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
        elif key == "search" and Config.fts_enabled:
            # assume that the index matched correctly
            filter_clauses.add("True")
        else:
            col = FIELDS_TO_COLUMNS["tags"]
            filter_clauses.add(
                f"bool([t for t in et[{col}] if t[0] == {key!r} and len(t) > 1 and t[1] in {value!r}])"
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


async def executor(
    lmdb_environment: lmdb.Environment,
    plans: list,
    pool,
    default_limit=None,
    log=None,
    loop=None,
):
    """
    Execute one or several queries.
    Returns iterator of (plan, events)
    """
    log = log or logging.getLogger("nostr_relay.kvquery")

    if not isinstance(plans, QueryPlans):
        plans = planner(plans, default_limit=default_limit, log=log)
    if len(plans) == 1:
        fut = asyncio.wrap_future(
            pool.submit(execute_one_plan, lmdb_environment, plans[0], log), loop=loop
        )
        await fut
        yield fut.result()
    else:
        submitted = [
            asyncio.wrap_future(
                pool.submit(execute_one_plan, lmdb_environment, plan, log), loop=loop
            )
            for plan in plans
        ]
        for fut in submitted:
            await fut
            yield fut.result()


def execute_one_plan(
    lmdb_environment: lmdb.Environment, plan: QueryPlan, log: logging.Logger
):
    """
    Run a single query plan, calling on_event for each event
    """
    events: list[Event] = []
    on_event = events.append
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
    except Exception:
        log.exception("execute_one_plan")

    finally:
        return plan, events


def analysis_thread(work_queue: queue.Queue, slow_query_threshold=500, delay=0.5):
    from time import sleep

    log = logging.getLogger("nostr_relay.queries")
    # log.debug("Running")
    while True:
        plans = work_queue.get()

        sleep(delay)
        log.debug("Analyzing %s", plans)
        try:
            _analyze(plans, log, slow_query_threshold=slow_query_threshold)
        except Exception:
            log.exception("analyze")
        del plans
        log.debug("Done")


def _analyze(plans: QueryPlans, log: logging.Logger, slow_query_threshold=500):
    while plans:
        plan = plans.pop()
        log.debug("Executed plan. Stats: %s", plan.stats)
        stats = plan.stats
        duration = (stats["end"] - stats["start"]) * 1000  # milliseconds
        if duration > slow_query_threshold:
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


ANALYSIS_QUEUE = queue.Queue(maxsize=30)


def analyze(plans: QueryPlans, later=True, log=None):
    """
    Analyze and log query stats from the completed task
    """
    if later:
        if analyze.ANALYSIS_THREAD is None:
            analyze.ANALYSIS_THREAD = threading.Thread(
                target=analysis_thread,
                args=(ANALYSIS_QUEUE,),
                kwargs={"delay": Config.get("analysis_delay", 0.5)},
            )
            analyze.ANALYSIS_THREAD.daemon = True
            analyze.ANALYSIS_THREAD.start()
        try:
            ANALYSIS_QUEUE.put_nowait(plans)
        except queue.Full:
            log.debug("Analysis queue full")
    else:
        _analyze(plans, log)


analyze.ANALYSIS_THREAD = None


def encode_event(event: Event) -> bytes:
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


def decode_event(data: tuple) -> Event:
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


def bytes_from_hex(hexstr: str) -> bytes:
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
