import os.path
import asyncio
import sqlite3
import time
import collections
import logging
import traceback
import aiosqlite
import rapidjson
from .event import Event, EventKind
from .config import Config
from .verification import Verifier


LOG = logging.getLogger(__name__)

force_hex_translation = str.maketrans('abcdef0213456789','abcdef0213456789', 'ghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')

def validate_id(obj_id):
    obj_id = obj_id.lower().strip()
    if obj_id.isalnum():
        obj_id = obj_id.translate(force_hex_translation)
        return obj_id


class StorageException(Exception):
    pass

STORAGE = None

def get_storage():
    global STORAGE
    if STORAGE is None:
        STORAGE = Storage(Config.db_filename)
    return STORAGE


class Storage:
    INSERT_EVENT = "INSERT OR IGNORE INTO event (id, event) VALUES (?, ?)"
    CHECK_QUERY = 'SELECT 1 from event where id = ?'

    def __init__(self, filename='nostr.sqlite3'):
        self.filename = filename
        self.clients = collections.defaultdict(dict)
        self.db = None
        self.verifier = Verifier()
        self.newevent_event = asyncio.Event()

    async def close(self):
        await self.verifier.stop()
        await self.db.close()

    async def setup_db(self):
        LOG.info(f"Database file {self.filename} {'exists' if os.path.exists(self.filename) else 'does not exist'}")
        async with aiosqlite.connect(self.filename) as db:
            await migrate(db)
        self.db = await aiosqlite.connect(self.filename)
        await self.db.execute('pragma journal_mode=wal')
        await self.db.execute('pragma synchronous = normal')
        await self.db.execute('pragma temp_store = memory')
        await self.db.execute('pragma mmap_size = 30000000000')
        await self.verifier.start(self.db)
        self.newevent_event = asyncio.Event()

    async def get_event(self, event_id):
        """
        Shortcut for retrieving an event by id
        """
        async with self.db.cursor() as cursor:
            await cursor.execute('select event from event where id = ?', (bytes.fromhex(event_id), ))
            row = await cursor.fetchone()
            if row:
                return row[0]

    async def add_event(self, event_json):
        """
        Add an event from json object
        Return (status, event)
        """
        try:
            event = Event(**event_json)
        except Exception as e:
            LOG.error("bad json")
            raise StorageException("invalid: Bad JSON")

        await asyncio.get_running_loop().run_in_executor(None, self.validate_event, event)
        changed = False
        async with self.db.cursor() as cursor:
            do_save = await self.pre_save(cursor, event)
            if do_save:
                await cursor.execute(self.INSERT_EVENT, (event.id_bytes, str(event)))
                changed = cursor.rowcount == 1
                await self.post_save(cursor, event)
            await self.db.commit()
        if changed:
            # notify all subscriptions
            self.newevent_event.set()
        return event, changed

    def validate_event(self, event):
        """
        Validate basic format and signature
        """
        if Config.max_event_size and len(event.content) > Config.max_event_size:
            LOG.error("Received large event %s from %s size:%d max_size:%d",
                event.id, event.pubkey, len(event.content), Config.max_event_size
            )
            raise StorageException("invalid: 280 characters should be enough for anybody")
        if not event.verify():
            raise StorageException("invalid: Bad signature")

    async def pre_save(self, cursor, event):
        """
        Pre-process the event to check permissions, duplicates, etc.
        Return None to skip adding the event.
        """
        await cursor.execute(self.CHECK_QUERY, (event.id_bytes, ))
        if cursor.rowcount == 1:
            # duplicate
            return False
        # check NIP05 verification, if enabled
        await self.verifier.verify(cursor, event)
        if event.is_ephemeral or event.kind > 30000:
            # don't save ephemeral or unspecified events
            return False
        elif event.is_replaceable:
            # check for older event from same pubkey
            await cursor.execute('select id, created_at from event where pubkey = ? and kind = ? and created_at < ?', (event.pubkey, event.kind, event.created_at))
            row = await cursor.fetchone()
            if row:
                old_id = row[0]
                old_ts = row[1]
                LOG.info("Replacing event %s from %s@%s with %s", old_id, event.pubkey, old_ts, event.id)
                await cursor.execute('delete from event where id = ?', (old_id, ))
        return True

    async def post_save(self, cursor, event):
        """
        Post-process event
        (clear old metadata, update tag references)
        """

        if cursor.rowcount:
            if event.kind in (EventKind.SET_METADATA, EventKind.CONTACTS):
                # older metadata events can be cleared
                query = 'DELETE FROM event WHERE pubkey = ? AND kind = ? AND created_at < ?'
                LOG.debug("q:%s kind:%s, key:%s", query, event.kind, event.pubkey)
                await cursor.execute(query, (event.pubkey, event.kind, event.created_at))
            elif event.kind in (EventKind.TEXT_NOTE, EventKind.ENCRYPTED_DIRECT_MESSAGE) and event.tags:
                # update mentions
                for tag in event.tags:
                    name = tag[0]
                    if len(name) == 1 or name == 'delegation':
                        # single-letter tags can be searched
                        # delegation tags are also searched
                        ptag = validate_id(tag[1])
                        if ptag:
                            await cursor.execute('INSERT OR IGNORE INTO tag (id, name, value) VALUES (?, ?, ?)', (event.id_bytes, name, ptag))
            elif event.kind == EventKind.DELETE and event.tags:
                # delete the referenced events
                for tag in event.tags:
                    name = tag[0]
                    if name == 'e':
                        event_id = tag[1]
                        await cursor.execute('DELETE FROM event WHERE id = ? AND pubkey = ?', (bytes.fromhex(event_id), event.pubkey))
        else:
            LOG.debug("skipped post-processing for %s", event)

    def read_subscriptions(self, client_id):
        for task, sub in self.clients[client_id].values():
            yield from sub.read()

    def subscribe(self, client_id, sub_id, filters):
        self.newevent_event.clear()
        LOG.debug('%s %s filters: %s', client_id, sub_id, filters)
        if sub_id in self.clients[client_id]:
            self.unsubscribe(client_id, sub_id)
        sub = Subscription(self.db, sub_id, filters, self.newevent_event)
        if sub.prepare():
            task = asyncio.create_task(sub.start())
            self.clients[client_id][sub_id] = (task, sub)
            LOG.info("%s +sub %s", client_id, sub_id)

    def unsubscribe(self, client_id, sub_id=None):
        if sub_id:
            try:
                task, sub = self.clients[client_id][sub_id]
                sub.cancel()
                task.cancel()
                del self.clients[client_id][sub_id]
                LOG.info("%s -sub %s", client_id, sub_id)
            except KeyError:
                pass
        else:
            for task, sub in self.clients[client_id].values():
                sub.cancel()
                task.cancel()
            del self.clients[client_id]

    async def num_subscriptions(self, byclient=False):
        subs = {}
        for client_id, client in self.clients.items():
            subs[client_id] = len(client)
        if byclient:
            return subs
        else:
            return {'total': sum(subs.values())}


class Subscription:
    def __init__(self, db, sub_id, filters:list, newevent_event):
        self.db  = db
        self.sub_id = sub_id
        self.filters = filters
        self.newevent_event = newevent_event
        self.queue = collections.deque()
        self.running = True
        self.interval = 60

    def prepare(self):
        try:
            self.query = self.build_query(self.filters)
        except Exception:
            LOG.exception("build_query")
            return False
        return True

    async def start(self):
        LOG.debug(f'Starting {self.sub_id}')
        seen_ids = set()
        runs = 0
        query = self.query
        LOG.debug(query)
        last_run = 0
        while self.running:
            runs += 1
            try:
                start = time.time()
                async with self.db.execute(query) as cursor:
                    async for row in cursor:
                        eid, event = row
                        if eid in seen_ids:
                            continue
                        seen_ids.add(eid)
                        self.queue.append(event)
                if runs == 1 and len(seen_ids) < 5000:
                    # send a sentinel to indicate we have no more events
                    self.queue.append(None)
                duration = int((time.time() - start) * 1000)

                LOG.info('queried %s runs:%s queue:%s duration:%dms', self.sub_id, runs, len(self.queue), duration)

                # every time an event is added, all subscribers are notified.
                # this could have a performance penalty since everyone will retry their queries
                # at the same time. but overall, this may be a worthwhile optimization to reduce
                # idle load

                await self.newevent_event.wait()
                if (time.time() - last_run) < 1.5:
                    await asyncio.sleep(self.interval)
            except Exception:
                LOG.exception("subscription")
                break
            last_run = time.time()
        LOG.debug(f'Stopped {self.sub_id}')

    def read(self):
        while self.queue:
            event = self.queue.popleft()
            yield self.sub_id, event

    def cancel(self):
        self.running = False

    def build_query(self, filters):
        select = '''
        SELECT event.id, event.event FROM event
        '''
        include_tags = False
        where = []
        limit = None
        for filter_obj in filters:
            subwhere = []
            if 'ids' in filter_obj:

                ids = filter_obj['ids']
                if not isinstance(ids, list):
                    ids = [ids]
                ids = set(ids)
                if ids:
                    idstr = ','.join("x'%s'" % validate_id(eid) for eid in ids)
                    subwhere.append(f'event.id in ({idstr})')
                # else:
                #     raise NotImplementedError()
                #     eq = ''
                #     while ids:
                #         eid = validate_id(ids.pop())
                #         if eid:
                #             eq += "event.hexid like '%s%%'" % eid
                #             if ids:
                #                 eq += ' OR '
                #         else:
                #             pass
                #     if eq:
                #         subwhere.append(f'({eq})')

            if 'authors' in filter_obj:
                astr = ','.join("'%s'" % validate_id(a) for a in set(filter_obj['authors']))
                if astr:
                    subwhere.append(f'pubkey in ({astr}) OR (tag.name = "delegation" and tag.value in ({astr}))')
                    include_tags = True

            if 'kinds' in filter_obj:
                subwhere.append('kind in ({})'.format(','.join(str(int(k)) for k in filter_obj['kinds'])))

            if 'since' in filter_obj:
                subwhere.append('created_at >= %d' % int(filter_obj['since']))

            if 'until' in filter_obj:
                subwhere.append('created_at < %d' % int(filter_obj['until']))

            if 'limit' in filter_obj:
                limit = max(min(int(filter_obj['limit']), 5000), 0)

            for k in filter_obj:
                if k[0] == '#' and len(k) == 2:
                    tagname = k[1]
                    tagval = filter_obj[k]
                    pstr = []
                    for val in set(tagval):
                        val = validate_id(val)
                        if val:
                            pstr.append(f"'{val}'")
                    if pstr:
                        pstr = ','.join(pstr)
                        subwhere.append(f'(tag.name = "{tagname}" and tag.value in ({pstr})) ')
                        include_tags = True

            if subwhere:
                subwhere = ' AND '.join(subwhere)
                where.append(subwhere)
        if where:
            if include_tags:
                select += 'LEFT JOIN tag ON tag.id = event.id\n'
            select += ' WHERE ('
            select += ') OR ('.join(where)
            select += ')'
        if limit is None:
            limit = 5000
        select += f'''
            ORDER BY created_at DESC LIMIT {limit}
        '''
        return select


async def migrate(db):
    import sqlite3

    async def migrate_to_1(db):
        """
        Create migration table
        """
        await db.execute("""
            CREATE TABLE migrations (
                version INT PRIMARY KEY,
                migration DATETIME
            )
        """)
        await db.commit()
        LOG.info("migration: created migration table")

    async def migrate_to_2(db):
        """
        Create new event table
        """
        await db.execute("""
            CREATE TABLE event (
                  id BLOB PRIMARY KEY,
                  created_at INT GENERATED ALWAYS AS (json_extract(event, "$.created_at")) STORED,
                  kind INT GENERATED ALWAYS AS (json_extract(event, "$.kind")) STORED,
                  pubkey TEXT GENERATED ALWAYS AS (json_extract(event, "$.pubkey")),
                  hexid TEXT GENERATED ALWAYS AS (lower(hex(id))),
                  event JSON
                )
            """)
        await db.execute("CREATE INDEX pkidx on event(pubkey);")
        await db.execute("CREATE INDEX kidx on event(kind);")
        await db.execute("CREATE INDEX cidx on event(created_at);")
        LOG.info("migration: created event table")
        await db.execute("""
            CREATE TABLE tag (
                id BLOB  REFERENCES event(id) ON DELETE CASCADE,
                name TEXT,
                value TEXT
        )
        """)
        await db.execute("CREATE UNIQUE INDEX IF NOT EXISTS tag_idx ON tag (id, name, value);")
        LOG.info("migration: created tag table")

    async def migrate_to_3(db):
        """
        Migrate old data to new event table
        """
        try:
            count = 0
            async with db.execute("select * from events") as cursor:
                async for row in cursor:
                    e = Event.from_tuple(row)
                    await db.execute("insert into event (id, event) VALUES (?, ?)", (e.id_bytes, str(e)))
                    for tag in e.tags:
                        name = tag[0]
                        value = tag[1]
                        if len(name) == 1 or name == 'delegation':
                            await db.execute("INSERT OR IGNORE INTO tag (id, name, value) VALUES (?, ?, ?)", (e.id_bytes, name, value))
                    count += 1
            LOG.info("migration: migrated %d events", count)
        except sqlite3.OperationalError:
            # events table doesn't exist
            LOG.info("migration: events table does not exist")

    async def migrate_to_4(db):
        """
        Create the verification table
        """
        #retrieve old entries
        data = []

        try:
            async with db.execute("select * from verification") as cursor:
                async for id, identifier, metadata_id, verified_at, failed_at in cursor:
                    data.append((id, identifier, bytes.fromhex(metadata_id), verified_at, failed_at))
            await db.execute("DROP TABLE verification")
        except sqlite3.OperationalError:
            pass

        await db.execute("""
            CREATE TABLE IF NOT EXISTS verification (
                id INTEGER PRIMARY KEY,
                identifier TEXT,
                metadata_id BLOB REFERENCES event(id) ON DELETE CASCADE,
                verified_at TIMESTAMP DEFAULT 0,
                failed_at TIMESTAMP DEFAULT 0
            );
        """)
        await db.execute("CREATE INDEX if not exists identifieridx on verification (identifier);")
        await db.execute("CREATE INDEX if not exists verifiedidx on verification (verified_at);")
        await db.execute("CREATE INDEX if not exists metadataidx on verification (metadata_id);")
        LOG.info("migration: created verification table")
        if data:
            await db.executemany("insert into verification (id, identifier, metadata_id, verified_at, failed_at) values (?, ?, ?, ?, ?)", data)
            LOG.info("migration: transferred %d verification records", len(data))



    version, lasttime = 0, None
    try:
        cursor = await db.execute("select * from migrations order by version desc limit 1;")
    except sqlite3.OperationalError:
        # table does not exist
        pass
    else:
        row = await cursor.fetchone()
        if row:
            version, lasttime = row

    migrated = False
    while 1:
        version = version + 1
        try:
            func = locals()[f'migrate_to_{version}']
        except KeyError:
            break
        LOG.info("migration: migrating to %s: %s", version, getattr(func, "__doc__", "").strip())
        await func(db)
        await db.execute("insert into migrations (version, migration) VALUES (?, datetime('now'))", (version, ))
        await db.commit()

