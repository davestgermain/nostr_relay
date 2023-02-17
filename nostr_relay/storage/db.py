import os
import os.path
import asyncio
import logging
from datetime import datetime
from time import time

import sqlalchemy as sa
from sqlalchemy.engine.base import Engine

from aionostr.event import Event, EventKind
from ..config import Config
from ..verification import Verifier
from ..auth import get_authenticator, Action
from ..errors import StorageError, AuthenticationError
from ..util import (
    object_from_path,
    catchtime,
    Periodic,
    json,
)
from . import get_metadata
from .base import BaseStorage, BaseSubscription, BaseGarbageCollector


force_hex_translation = str.maketrans(
    "abcdef0213456789",
    "abcdef0213456789",
    "ghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
)


def validate_id(obj_id):
    obj_id = str(obj_id or "").lower().strip()
    if obj_id.isalnum():
        obj_id = obj_id.translate(force_hex_translation)
        return obj_id
    return ""


class DBStorage(BaseStorage):
    DEFAULT_GARBAGE_COLLECTOR = "nostr_relay.storage.db.QueryGarbageCollector"

    def __init__(self, options):
        super().__init__(options)
        self.options, self.sqlalchemy_options = self.parse_options(options)
        self.subscription_class = self.options["subscription_class"]
        self.validate_event = self.options["validator_function"]
        self.db_url = self.sqlalchemy_options.pop(
            "url", "sqlite+aiosqlite:///nostr.sqlite3"
        )
        self.is_postgres = "postgresql" in self.db_url

        self.db = None
        self.garbage_collector_task = None

        if not self.is_postgres:
            # add event listener to set appropriate PRAGMA items
            sa.event.listen(Engine, "connect", self._set_sqlite_pragma)

    def parse_options(self, options):
        from nostr_relay.validators import get_validator

        sqlalchemy_options = {}
        storage_options = {
            "validators": ["nostr_relay.validators.is_signed"],
            "subscription_class": Subscription,
        }
        for key, value in options.items():
            if key.startswith("sqlalchemy."):
                sqlalchemy_options[key.replace("sqlalchemy.", "")] = value
            elif key == "subscription_class":
                storage_options["subscription_class"] = object_from_path(value)
            else:
                storage_options[key] = value
        storage_options["validator_function"] = get_validator(
            storage_options.pop("validators")
        )
        return storage_options, sqlalchemy_options

    def _set_sqlite_pragma(self, dbapi_connection, connection_record):
        cursor = dbapi_connection.cursor()
        pragma = """
                PRAGMA journal_mode = wal;
                PRAGMA locking_mode = NORMAL;
                PRAGMA synchronous = normal;
                PRAGMA temp_store = memory;
                PRAGMA mmap_size = 30000000000;
                PRAGMA foreign_keys = ON;
        """
        for stmt in pragma.split(";"):
            stmt = stmt.strip()
            if stmt:
                cursor.execute(stmt)
        cursor.close()

    async def close(self):
        if self.garbage_collector_task:
            self.garbage_collector_task.cancel()
        await self.verifier.stop()
        await self.db.dispose()

    async def optimize(self):
        if not self.is_postgres:
            async with self.db.begin() as conn:
                await conn.execute(sa.text("PRAGMA analysis_limit=400"))
                await conn.execute(sa.text("PRAGMA optimize"))

    async def setup(self):
        await super().setup()
        from sqlalchemy.ext.asyncio import create_async_engine

        # limit the amount of concurrent inserts/selects
        self.add_slot = asyncio.Semaphore(
            int(self.options.pop("num_concurrent_adds", 4))
        )
        self.query_slot = asyncio.Semaphore(
            int(self.options.pop("num_concurrent_reqs", 10))
        )

        self.db = create_async_engine(
            self.db_url,
            json_deserializer=json.loads,
            json_serializer=json.dumps,
            pool_pre_ping=True,
            **self.sqlalchemy_options,
        )
        self.log.info("Connected to %s", self.db.url)

        metadata = get_metadata()
        self.EventTable = metadata.tables["events"]
        self.IdentTable = metadata.tables["identity"]
        self.AuthTable = metadata.tables["auth"]
        TagTable = metadata.tables["tags"]
        if self.is_postgres:
            from sqlalchemy.dialects.postgresql import insert

            self.tag_insert_query = insert(TagTable).on_conflict_do_nothing(
                index_elements=["id", "name", "value"]
            )
            self.event_insert_query = insert(self.EventTable).on_conflict_do_nothing(
                index_elements=["id"]
            )
        else:
            self.tag_insert_query = sa.insert(TagTable).prefix_with("OR IGNORE")
            self.event_insert_query = sa.insert(self.EventTable).prefix_with(
                "OR IGNORE"
            )

        self.verifier = Verifier(self, Config.get("verification", {}))
        await self.verifier.start(self.db)
        self.log.debug("done setting up")

    async def get_event(self, event_id):
        """
        Shortcut for retrieving an event by id
        """
        async with self.query_slot:
            async with self.db.connect() as conn:
                result = await conn.execute(
                    sa.select(self.EventTable).where(
                        self.EventTable.c.id == bytes.fromhex(event_id)
                    )
                )
                row = result.first()
        if row:
            return Event.from_tuple(row)

    async def delete_event(self, event_id):
        async with self.db.begin() as conn:
            await conn.execute(
                self.EventTable.delete().where(
                    self.EventTable.c.id == bytes.fromhex(event_id)
                )
            )

    async def add_event(self, event_json, auth_token=None):
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
        # check authentication
        if not await self.authenticator.can_do(auth_token, Action.save.value, event):
            raise AuthenticationError("restricted: permission denied")

        changed = False
        with self.stat_collector.timeit("insert") as counter:
            async with self.add_slot:
                async with self.db.begin() as conn:
                    do_save = await self.pre_save(conn, event)
                    if do_save:
                        result = await conn.execute(
                            self.event_insert_query.values(
                                id=event.id_bytes,
                                created_at=event.created_at,
                                pubkey=bytes.fromhex(event.pubkey),
                                sig=bytes.fromhex(event.sig),
                                content=event.content,
                                kind=event.kind,
                                tags=event.tags,
                            )
                        )
                        changed = result.rowcount == 1
                        await self.post_save(event, connection=conn, changed=changed)
            counter["count"] += 1
        if changed:
            await self.notify_all_connected(event)
            # notify other processes
            await self.notify_other_processes(event)
        return event, changed

    async def pre_save(self, conn, event):
        """
        Pre-process the event to check permissions, duplicates, etc.
        Return None to skip adding the event.
        """
        # check NIP05 verification, if enabled
        await self.verifier.verify(event)

        if event.is_replaceable or event.is_paramaterized_replaceable:
            # check for older event from same pubkey
            query = sa.select(
                self.EventTable.c.id,
                self.EventTable.c.created_at,
                self.EventTable.c.tags,
            ).where(
                (self.EventTable.c.pubkey == bytes.fromhex(event.pubkey))
                & (self.EventTable.c.kind == event.kind)
                & (self.EventTable.c.created_at < event.created_at)
            )
            result = await conn.execute(query)

            delete_id = None
            if event.is_paramaterized_replaceable:
                # according to nip-33, an event with a matching "d" tag will be replaced
                # empty tags include [], [["d"]], and [["d", ""]]
                d_tag = ""
                for tag in event.tags:
                    if tag[0] == "d":
                        if len(tag) > 1:
                            d_tag = tag[1]
                        break
                for old_id, created_at, tags in result:
                    found_tag = [tag for tag in tags if tag[0] == "d"]
                    if not d_tag:
                        if (
                            not found_tag
                            or len(found_tag[0]) == 1
                            or found_tag[0][1] == ""
                        ):
                            delete_id = old_id
                            old_ts = created_at
                            break
                    else:
                        tag = found_tag[0]
                        if len(tag) > 1 and tag[1] == d_tag:
                            delete_id = old_id
                            old_ts = created_at
                            break

            else:
                row = result.first()
                if row:
                    delete_id = row[0]
                    old_ts = row[1]
            if delete_id:
                self.log.info(
                    "Replacing event %s from %s@%s with %s",
                    delete_id,
                    event.pubkey,
                    old_ts,
                    event.id,
                )
                await conn.execute(
                    self.EventTable.delete().where(self.EventTable.c.id == delete_id)
                )
        return True

    async def process_tags(self, conn, event):
        if event.tags:
            # update mentions
            # single-letter tags can be searched
            # delegation tags are also searched
            # expiration tags are also added for the garbage collector
            tags = set()
            for tag in event.tags:
                if tag[0] in ("delegation", "expiration"):
                    tags.add((tag[0], tag[1]))
                elif len(tag[0]) == 1:
                    tags.add((tag[0], tag[1] if len(tag) > 1 else ""))
            if tags:
                result = await conn.execute(
                    self.tag_insert_query,
                    [
                        {"id": event.id_bytes, "name": tag[0], "value": tag[1]}
                        for tag in tags
                    ],
                )

            if event.kind == EventKind.DELETE:
                # delete the referenced events
                for tag in event.tags:
                    name = tag[0]
                    if name == "e":
                        event_id = tag[1]
                        query = sa.delete(self.EventTable).where(
                            (self.EventTable.c.pubkey == bytes.fromhex(event.pubkey))
                            & (self.EventTable.c.id == bytes.fromhex(event_id))
                        )
                        result = await conn.execute(query)
                        self.log.info("Deleted event %s", event_id)

    async def post_save(self, event, connection=None, changed=None):
        """
        Post-process event
        (clear old metadata, update tag references)
        """

        if changed:
            if event.kind in (EventKind.SET_METADATA, EventKind.CONTACTS):
                # older metadata events can be cleared
                await connection.execute(
                    self.EventTable.delete().where(
                        (self.EventTable.c.pubkey == bytes.fromhex(event.pubkey))
                        & (self.EventTable.c.kind == event.kind)
                        & (self.EventTable.c.created_at < event.created_at)
                    )
                )
            await self.process_tags(connection, event)

    async def run_single_query(self, query_filters):
        """
        Run a single query, yielding json events
        """
        queue = asyncio.Queue()
        sub = self.subscription_class(
            self, "", query_filters, queue=queue, default_limit=600000
        )
        sub.prepare()
        async for event in self.run_query(sub.query):
            yield event

    async def run_query(self, query, if_long=None):
        self.log.debug(query)
        try:
            with self.stat_collector.timeit("query") as counter:
                async with self.query_slot:
                    async with self.db.connect() as conn:
                        async with conn.stream(query) as result:
                            async for row in result:
                                yield Event.from_tuple(row)
                                counter["count"] += 1
            duration = counter["duration"]
            if duration > 1.0 and if_long:
                if_long(duration)
        except Exception:
            self.log.exception("subscription")

    async def get_stats(self):
        stats = {"total": 0}
        async with self.db.connect() as conn:
            result = await conn.stream(
                sa.text(
                    "SELECT kind, COUNT(*) FROM events GROUP BY kind order by 2 DESC"
                )
            )
            kinds = {}
            async for kind, count in result:
                kinds[kind] = count
                stats["total"] += count
            stats["kinds"] = kinds

            if self.is_postgres:
                result = await conn.execute(
                    sa.text(
                        """
                        SELECT
                            SUM(pg_total_relation_size(table_name ::text))
                        FROM (
                            -- tables from 'public'
                            SELECT table_name
                            FROM information_schema.tables
                            where table_schema = 'public' and table_type = 'BASE TABLE'
                        ) AS all_tables
                """
                    )
                )
                row = result.first()
                stats["db_size"] = int(row[0])
            else:
                try:
                    result = await conn.execute(
                        sa.text(
                            'SELECT SUM("pgsize") FROM "dbstat" WHERE name in ("events", "tags")'
                        )
                    )
                    row = result.first()
                    stats["db_size"] = row[0]
                except (sa.exc.OperationalError, sa.exc.ProgrammingError):
                    pass
        subs = await self.num_subscriptions(True)
        num_subs = 0
        num_clients = 0
        for k, v in subs.items():
            num_clients += 1
            num_subs += v
        stats["active_subscriptions"] = num_subs
        stats["active_clients"] = num_clients
        return stats

    async def get_identified_pubkey(self, identifier, domain=""):
        query = sa.select(
            self.IdentTable.c.pubkey,
            self.IdentTable.c.identifier,
            self.IdentTable.c.relays,
        )
        pars = []
        if domain:
            query = query.where(self.IdentTable.c.identifier.like(f"%@{domain}"))
        if identifier:
            query = query.where(self.IdentTable.c.identifier == identifier)
        data = {"names": {}, "relays": {}}
        self.log.debug("Getting identity for %s %s", identifier, domain)
        async with self.query_slot:
            async with self.db.connect() as conn:
                result = await conn.stream(query)
                async for pubkey, identifier, relays in result:
                    data["names"][identifier.split("@")[0]] = pubkey
                    if relays:
                        data["relays"][pubkey] = relays

        return data

    async def set_identified_pubkey(self, identifier, pubkey, relays=None):
        async with self.db.begin() as conn:
            if not pubkey:
                await conn.execute(
                    self.IdentTable.delete().where(
                        self.IdentTable.c.identifier == identifier
                    )
                )
            elif not (validate_id(pubkey) and len(pubkey) == 64):
                raise StorageError("invalid public key")
            else:
                pars = [identifier, pubkey, json.dumps(relays or [])]
                await conn.execute(
                    sa.delete(self.IdentTable).where(
                        self.IdentTable.c.identifier == identifier
                    )
                )
                stmt = sa.insert(self.IdentTable).values(
                    {"identifier": identifier, "pubkey": pubkey, "relays": relays}
                )
                await conn.execute(stmt)

    async def get_auth_roles(self, pubkey):
        """
        Get the roles assigned to the public key
        """
        async with self.db.begin() as conn:
            result = await conn.execute(
                sa.select(self.AuthTable.c.roles).where(
                    self.AuthTable.c.pubkey == pubkey
                )
            )
            row = result.fetchone()
        if row:
            return set(row[0].lower())
        else:
            return self.authenticator.default_roles

    async def get_all_auth_roles(self):
        """
        Return all roles in authentication table
        """
        async with self.db.begin() as conn:
            result = await conn.stream(
                sa.select(self.AuthTable.c.pubkey, self.AuthTable.c.roles)
            )
            async for pubkey, role in result:
                yield pubkey, set((role or "").lower())

    async def set_auth_roles(self, pubkey: str, roles: str):
        """
        Assign roles to the given public key
        """
        async with self.db.begin() as conn:
            try:
                await conn.execute(
                    sa.insert(self.AuthTable).values(
                        pubkey=pubkey, roles=roles, created=datetime.now()
                    )
                )
            except sa.exc.IntegrityError:
                await conn.execute(
                    sa.update(self.AuthTable)
                    .where(self.AuthTable.c.pubkey == pubkey)
                    .values(roles=roles)
                )


class Subscription(BaseSubscription):
    __slots__ = ("is_postgres",)

    def __init__(
        self,
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.is_postgres = self.storage.is_postgres

    def prepare(self):
        try:
            self.query, self.filters = self.build_query(self.filters)
        except Exception:
            self.log.exception("build_query")
            return False
        return True

    async def run_query(self):
        sub_id = self.sub_id
        queue = self.queue
        check_output = self.storage.check_output

        results = self.storage.run_query(
            self.query,
            if_long=lambda duration: logging.getLogger(
                "nostr_relay.long-queries"
            ).warning(
                f"{self.client_id}/{self.sub_id} Long query: '{self.filters}' took %dms",
                duration,
            ),
        )
        if check_output:
            context = {
                "config": Config,
                "client_id": self.client_id,
                "auth_token": self.auth_token,
            }
            async for event in results:
                if check_output(event, context):
                    await queue.put((sub_id, event))
        else:
            async for event in results:
                await queue.put((sub_id, event))
        await queue.put((sub_id, None))

    def evaluate_filter(self, filter_obj, subwhere):
        for key, value in filter_obj.items():
            if key == "ids":
                if not isinstance(value, list):
                    value = [value]
                ids = set(value)
                if ids:
                    exact = []
                    for eid in ids:
                        eid = validate_id(eid)
                        if eid:
                            if len(eid) == 64:
                                if self.is_postgres:
                                    exact.append(f"'\\x{eid}'")
                                else:
                                    exact.append(f"x'{eid}'")
                            elif len(eid) > 2:
                                if self.is_postgres:
                                    subwhere.append(f"encode(id, 'hex') LIKE '{eid}%'")
                                else:
                                    subwhere.append(f"lower(hex(id)) LIKE '{eid}%'")
                    if exact:
                        idstr = ",".join(exact)
                        subwhere.append(f"events.id IN ({idstr})")
                else:
                    # invalid query
                    raise ValueError("ids")
            elif key == "authors" and isinstance(value, list):
                if value:
                    exact = set()
                    hexexact = set()
                    for pubkey in value:
                        pubkey = validate_id(pubkey)
                        if pubkey:
                            if len(pubkey) == 64:
                                if self.is_postgres:
                                    exact.add(f"'\\x{pubkey}'")
                                else:
                                    exact.add(f"x'{pubkey}'")
                                hexexact.add(f"'{pubkey}'")
                            # no prefix searches, for now
                    if exact:
                        astr = ",".join(exact)
                        subwhere.append(
                            f"(pubkey IN ({astr}) OR id IN (SELECT id FROM tags WHERE name = 'delegation' AND value IN ({','.join(hexexact)})))"
                        )
                    else:
                        raise ValueError("authors")
                else:
                    # query with empty list should be invalid
                    raise ValueError("authors")
            elif key == "kinds":
                if isinstance(value, list) and all(isinstance(k, int) for k in value):
                    subwhere.append(
                        "kind IN ({})".format(",".join(str(int(k)) for k in value))
                    )
                else:
                    raise ValueError("kinds")
            elif key == "since":
                if value:
                    subwhere.append("created_at >= %d" % int(value))
            elif key == "until":
                if value:
                    subwhere.append("created_at < %d" % int(value))
                else:
                    raise ValueError("until")
            elif key == "limit" and isinstance(value, int):
                filter_obj["limit"] = min(max(value, 0), self.default_limit)
            elif key[0] == "#" and len(key) == 2 and value:
                pstr = []
                for val in set(value):
                    if val:
                        val = val.replace("'", "''")
                        pstr.append(f"'{val}'")
                if pstr:
                    pstr = ",".join(pstr)
                    subwhere.append(
                        f"id IN (SELECT id FROM tags WHERE name = '{key[1]}' AND value IN ({pstr})) "
                    )
        return filter_obj

    def build_query(self, filters):
        select = """
            SELECT id, created_at, kind, pubkey, tags, sig, content FROM events
        """
        where = set()
        limit = None
        new_filters = []
        for filter_obj in filters:
            subwhere = []
            try:
                filter_obj = self.evaluate_filter(filter_obj, subwhere)
            except ValueError:
                self.log.debug("bad query %s", filter_obj)
                filter_obj = {}
                subwhere = []
            if subwhere:
                subwhere = " AND ".join(subwhere)
                where.add(subwhere)
            else:
                where.add("false")
            if "limit" in filter_obj:
                limit = filter_obj["limit"]
            new_filters.append(filter_obj)
        if where:
            select += " WHERE (\n\t"
            select += "\n) OR (\n".join(where)
            select += ")"
        if limit is None:
            limit = self.default_limit
        select += f"""
            ORDER BY created_at DESC
            LIMIT {limit}
        """
        return sa.text(select), new_filters


class QueryGarbageCollector(BaseGarbageCollector):
    query = """
        DELETE FROM events WHERE events.id IN
        (
            SELECT events.id FROM events
            LEFT JOIN tags on tags.id = events.id
            WHERE 
                (kind >= 20000 and kind < 30000)
            OR
                (tags.name = 'expiration' AND tags.value < '%NOW%')
        )
    """

    async def collect(self, conn):
        result = await conn.execute(
            sa.text(self.query.replace("%NOW%", str(int(time()))))
        )
        return max(0, result.rowcount)
