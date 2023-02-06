"""
Experimental storage using LMDB
"""

import lmdb
import logging
import asyncio
import collections
import threading
import functools

from contextlib import contextmanager
from queue import Queue

from aionostr.event import Event, EventKind
from msgpack import packb, unpackb
from concurrent.futures import ThreadPoolExecutor

from .base import BaseStorage, BaseSubscription, compile_filters
from ..auth import get_authenticator
from ..config import Config
from ..util import catchtime, StatsCollector, json
from ..validators import get_validator

# ids: b'\x00<32 bytes of id>'
# created: b'\x01<4 bytes time>\x00<32 bytes id>'
# kind: b'\x02<4 bytes kind>\x00<32 bytes of id>'
# author: b'\x03<32 bytes pubkey>\x00<32 bytes id>'
# tag: b'\x09<tag string>\x00<tag value>\x00<32 bytes id>'


version = 1


def encode_event(event):
    row = (
        version,
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
    def __init__(self, env):
        super().__init__()
        self.running = True
        self.env = env
        self.queue = Queue()

    def run(self):
        env = self.env
        qget = self.queue.get
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
                with env.begin(write=True, buffers=True) as txn:
                    if operation == "add":
                        for index in write_indexes:
                            index.write(event, txn)
                        if event.kind in (EventKind.SET_METADATA, EventKind.CONTACTS):
                            # delete older metadata events
                            with INDEXES["authorkinds"].scanner(
                                txn, [(event.pubkey, event.kind)]
                            ) as scanner:
                                for event_id in scanner:
                                    log.info("Deleting %s", event_id)
                                    delete_event(event_id, txn)
                    elif operation == "del":
                        delete_event(bytes.fromhex(event_id), txn)
            except Exception as e:
                log.exception("writer")


class LMDBStorage(BaseStorage):
    def __init__(self, options):
        self.options = options
        self.options.pop("class")
        self.log = logging.getLogger(__name__)
        self.subscription_class = Subscription
        self.clients = collections.defaultdict(dict)

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
        self.writer_thread = WriterThread(self.env)
        self.writer_queue = self.writer_thread.queue
        self.writer_thread.start()

    def delete_event(self, event_id):
        self.writer_queue.put((event, "del"))

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

        return event, True

    async def get_event(self, event_id: str):
        with self.env.begin(buffers=True) as txn:
            event_data = txn.get(b"\x00%s" % bytes.fromhex(event_id))
            if event_data:
                return decode_event(event_data)

    async def run_single_query(self, filters):
        queue = asyncio.Queue()
        sub = Subscription(self, "", filters, queue=queue)
        sub.prepare()
        sub.start()
        event = await queue.get()
        while event is not None:
            yield event
            event = await queue.get()
        yield None

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
        return stats


class Subscription(BaseSubscription):
    execution_pool = ThreadPoolExecutor(max_workers=20)
    __slots__ = ("filter_json", "check_event")

    def prepare(self):
        with self.storage.stat_collector.timeit("prepare") as counter:
            self.filter_json = json.dumps(self.filters)
            self.check_event = compile_filters(self.filter_json)
            self.query = self.build_query(self.filters)
            counter["count"] += 1
        self.log.debug(f"Took {counter['duration']*1000:.2f}ms to prepare")
        return True

    def build_query(self, filters):
        queries = []
        for query in filters:
            matches = []
            to_run = {
                "since": query.pop("since", None),
                "until": query.pop("until", None),
                "limit": query.pop("limit", self.default_limit),
            }

            tags = []
            scores = []
            has_kinds = has_authors = False

            if "ids" in query:
                scores.append((100, "ids", query["ids"]))
            else:
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
                    elif key == "ids":
                        scores.append((len(value) * 3, "ids", value))

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

            self.log.debug("Scored query %s", scores)

            to_run["index"] = INDEXES[idx]
            to_run["matches"] = matches
            queries.append(to_run)
        return queries

    def collect_filters(self, filters, loop, queue, cancel_event):
        try:
            tqueue = Queue()
            for query in filters:
                self.execution_pool.submit(
                    self.execute_one_query, query, tqueue, cancel_event
                )

            to_find = len(filters)
            # breakpoint()
            call_soon = loop.call_soon_threadsafe
            getqueue = tqueue.get
            event = getqueue()
            check_event = self.check_event
            hits = 0
            misses = 0
            while to_find:
                if event is None:
                    to_find -= 1
                    continue
                elif check_event(event):
                    call_soon(queue.put_nowait, event)
                    hits += 1
                else:
                    misses += 1
                event = getqueue()

            call_soon(queue.put_nowait, None)
            cancel_event.set()
            self.log.info("index stats: %d hits %d misses", hits, misses)
            if misses > hits:
                self.log.info("query: %s", self.filter_json)
        except:
            self.log.exception("collect_filters")
            raise

    def execute_one_query(self, query, queue, cancel_event):
        try:
            limit = self.default_limit
            if "limit" in query:
                limit = min(query["limit"], limit)
            with self.storage.env.begin(buffers=True) as txn:
                with query["index"].scanner(
                    txn,
                    query["matches"],
                    since=query.get("since"),
                    until=query.get("until"),
                ) as scanner:
                    for event_id in scanner:
                        # if cancel_event.is_set():
                        #     break
                        event_data = txn.get(b"\x00%s" % event_id)
                        event = decode_event(event_data)

                        queue.put(event)
                        limit -= 1
                        if not limit:
                            break
            queue.put(None)
        except:
            self.log.exception("execute_one_query")
            queue.put(None)
            raise

    async def run_query(self):
        inqueue = asyncio.Queue()
        cancel_event = threading.Event()
        send_to_subscriber = self.queue.put

        with self.storage.stat_collector.timeit("query") as counter:
            task = self.storage.loop.run_in_executor(
                None,
                self.collect_filters,
                self.query,
                self.storage.loop,
                inqueue,
                cancel_event,
            )

            event = await inqueue.get()

            while event:
                await send_to_subscriber((self.sub_id, event))
                event = await inqueue.get()
                counter["count"] += 1
            await send_to_subscriber((self.sub_id, None))


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
