import os.path
import asyncio
import sqlite3
import time
import collections
import logging
import traceback
import threading
from time import perf_counter
from contextlib import contextmanager

import aiosqlite
import rapidjson
from .event import Event, EventKind
from .config import Config
from .verification import Verifier


LOG = logging.getLogger(__name__)

force_hex_translation = str.maketrans('abcdef0213456789','abcdef0213456789', 'ghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ')

def validate_id(obj_id):
    obj_id = str(obj_id or '').lower().strip()
    if obj_id.isalnum():
        obj_id = obj_id.translate(force_hex_translation)
        return obj_id
    return ''


@contextmanager
def catchtime() -> float:
    start = perf_counter()
    yield lambda: (perf_counter() - start) * 1000


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
        self.garbage_collector = None

    async def close(self):
        await self.verifier.stop()
        await self.optimize()
        await self.db.close()
        
    async def optimize(self):
         await self.db.executescript("""
             PRAGMA analysis_limit=400;
             PRAGMA optimize;
         """)

    async def setup_db(self):
        LOG.info(f"Database file {self.filename} {'exists' if os.path.exists(self.filename) else 'does not exist'}")
        async with aiosqlite.connect(self.filename) as db:
            await migrate(db)
        self.db = await aiosqlite.connect(self.filename)
        await self.db.executescript('''
            PRAGMA journal_mode=wal;
            PRAGMA synchronous = normal;
            PRAGMA temp_store = memory;
            PRAGMA mmap_size = 30000000000;
            PRAGMA foreign_keys = ON;
        ''')
        await self.verifier.start(self.db)
        self.garbage_collector = start_garbage_collector()

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
            count = 0
            with catchtime() as t:
                for client in self.clients.values():
                    for sub in client.values():
                        asyncio.create_task(sub.notify(event))
                        count += 1
            if count:
                LOG.debug("notify-all took %.2fms for %d subscriptions", t(), count)
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
        if (time.time() - event.created_at) > Config.oldest_event:
            raise StorageException(f"invalid: {event.created_at} is too old")
        elif (time.time() - event.created_at) < -3600:
            raise StorageException(f"invalid: {event.created_at} is in the future")

    async def pre_save(self, cursor, event):
        """
        Pre-process the event to check permissions, duplicates, etc.
        Return None to skip adding the event.
        """
        await cursor.execute(self.CHECK_QUERY, (event.id_bytes, ))
        row = await cursor.fetchone()
        if row:
            # duplicate
            return False
        # check NIP05 verification, if enabled
        await self.verifier.verify(cursor, event)

        if event.is_replaceable:
            # check for older event from same pubkey
            await cursor.execute('select id, created_at from event where pubkey = ? and kind = ? and created_at <= ?', (event.pubkey, event.kind, event.created_at))
            row = await cursor.fetchone()
            if row:
                old_id = row[0]
                old_ts = row[1]
                LOG.info("Replacing event %s from %s@%s with %s", old_id, event.pubkey, old_ts, event.id)
                await cursor.execute('delete from event where id = ?', (old_id, ))
        return True

    async def process_tags(self, cursor, event):
        if event.tags:
            # update mentions
            # single-letter tags can be searched
            # delegation tags are also searched
            # expiration tags are also added for the garbage collector
            tags = set((event.id_bytes, tag[0], tag[1]) for tag in event.tags if tag[0] in ('delegation', 'expiration') or len(tag[0]) == 1)
            if tags:
                await cursor.executemany('INSERT OR IGNORE INTO tag (id, name, value) VALUES (?, ?, ?)', tags)

            if event.kind == EventKind.DELETE:
                # delete the referenced events
                for tag in event.tags:
                    name = tag[0]
                    if name == 'e':
                        event_id = tag[1]
                        await cursor.execute('DELETE FROM event WHERE id = ? AND pubkey = ?', (bytes.fromhex(event_id), event.pubkey))

    async def post_save(self, cursor, event):
        """
        Post-process event
        (clear old metadata, update tag references)
        """

        if cursor.rowcount != -1:
            if event.kind in (EventKind.SET_METADATA, EventKind.CONTACTS):
                # older metadata events can be cleared
                query = 'DELETE FROM event WHERE pubkey = ? AND kind = ? AND created_at < ?'
                LOG.debug("q:%s kind:%s, key:%s", query, event.kind, event.pubkey)
                await cursor.execute(query, (event.pubkey, event.kind, event.created_at))
            await self.process_tags(cursor, event)
        else:
            LOG.debug("skipped post-processing for %s", event)

    async def subscribe(self, client_id, sub_id, filters, queue):
        LOG.debug('%s/%s filters: %s', client_id, sub_id, filters)
        if sub_id in self.clients[client_id]:
            await self.unsubscribe(client_id, sub_id)
        sub = Subscription(self.db, sub_id, filters, queue=queue, client_id=client_id)
        if sub.prepare():
            asyncio.create_task(sub.run_query())
            self.clients[client_id][sub_id] = sub
            LOG.info("%s/%s +", client_id, sub_id)

    async def unsubscribe(self, client_id, sub_id=None):
        if sub_id:
            try:
                self.clients[client_id][sub_id].cancel()
                del self.clients[client_id][sub_id]
                LOG.info("%s/%s -", client_id, sub_id)
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
            return {'total': sum(subs.values())}

    async def get_stats(self):
        stats = {'total': 0}
        async with self.db.execute('SELECT kind, COUNT(*) FROM event GROUP BY kind order by 2 DESC') as cursor:
            kinds = {}
            async for kind, count in cursor:
                kinds[kind] = count
                stats['total'] += count
            stats['kinds'] = kinds
        async with self.db.execute('SELECT COUNT(*) FROM verification') as cursor:
            row = await cursor.fetchone()
            stats['num_verified'] = row[0]
        try:
            async with self.db.execute('SELECT SUM("pgsize") FROM "dbstat" WHERE name="event"') as cursor:
                row = await cursor.fetchone()
                stats['db_size'] = row[0]
        except sqlite3.OperationalError:
            pass
        stats['active_subscriptions'] = (await self.num_subscriptions())['total']
        return stats

    async def get_identified_pubkey(self, identifier, domain=''):
        query = 'SELECT pubkey, identifier, relays FROM identity WHERE 1 '
        pars = []
        if domain:
            query += ' AND identifier LIKE ? '
            pars.append(f'%@{domain}')
        if identifier:
            query += ' AND identifier = ?'
            pars.append(identifier)
        data = {
            'names': {},
            'relays': {}
        }
        LOG.debug("Getting identity for ? ?", identifier, domain)
        async with self.db.execute(query, pars) as cursor:
            async for pubkey, identifier, relays in cursor:
                data['names'][identifier.split('@')[0]] = pubkey
                if relays:
                    data['relays'][pubkey] = rapidjson.loads(relays)

        return data

    async def set_identified_pubkey(self, identifier, pubkey, relays=None):
        async with self.db.cursor() as cursor:
            if not pubkey:
                pars = [identifier,]
                await cursor.execute("DELETE FROM identity WHERE identifier = ?", pars)
            elif not validate_id(pubkey):
                raise StorageException("invalid public key")
            else:
                pars = [identifier, pubkey, rapidjson.dumps(relays or [])]
                await cursor.execute("INSERT OR REPLACE INTO identity (identifier, pubkey, relays) VALUES (?, ?, ?)", pars)
            await self.db.commit()

    async def __aenter__(self):
        await self.setup_db()
        return self

    async def __aexit__(self, ex_type, ex, tb):
        await self.close()



class Subscription:
    def __init__(self, db, sub_id, filters:list, queue=None, client_id=None, default_limit=5000):
        self.db  = db
        self.sub_id = sub_id
        self.client_id = client_id
        self.filters = filters
        self.queue = queue
        self.query_task = None
        self.default_limit = default_limit

    def prepare(self):
        try:
            self.query, self.filters = self.build_query(self.filters)
        except Exception:
            LOG.exception("build_query")
            return False
        return True

    def cancel(self):
        if self.query_task:
            self.query_task.cancel()

    async def run_query(self):
        self.query_task = asyncio.current_task()

        query = self.query
        LOG.debug(query)

        try:
            count = 0
            with catchtime() as t:
                async with self.db.execute(query) as cursor:
                    async for row in cursor:
                        eid, event = row
                        await self.queue.put((self.sub_id, event))
                        count += 1
                await self.queue.put((self.sub_id, None))

            duration = t()
            LOG.info('%s/%s query – events:%s duration:%dms', self.client_id, self.sub_id, count, duration)
            if count > 2000:
                LOG.warning("%s/%s Long query: '%s' took %dms", self.client_id, self.sub_id, rapidjson.dumps(self.filters), duration)
        except Exception:
            LOG.exception("subscription")

    async def notify(self, event):
        # every time an event is added, all subscribers are notified.
        # this could have a performance penalty since everyone will retry their queries
        # at the same time. but overall, this may be a worthwhile optimization to reduce
        # idle load

        with catchtime() as t:
            matched = self.check_event(event, self.filters)
        LOG.debug('%s/%s notify match %s %s duration:%.2fms', self.client_id, self.sub_id, event.id, matched, t())
        if matched:
            await self.queue.put((self.sub_id, event))

    def check_event(self, event, filters):
        for filter_obj in filters:
            matched = set()
            for key, value in filter_obj.items():
                if key == 'ids':
                    matched.add(event.id in value)
                elif key == 'authors':
                    matched.add(event.pubkey in value)
                    for tag in event.tags:
                        if tag[0] == 'delegation' and tag[1] in value:
                            matched.add(True)
                elif key == 'kinds':
                    matched.add(event.kind in value)
                elif key == 'since':
                    matched.add(event.created_at >= value)
                elif key == 'until':
                    matched.add(event.created_at < value)
                elif key[0] == '#' and len(key) == 2:
                    for tag in event.tags:
                        if tag[0] == key[1]:
                            matched.add(tag[1] in value)
            if all(matched):
                return True
        return False

    def evaluate_filter(self, filter_obj, subwhere):
        for key, value in filter_obj.items():
            if key == 'ids':
                if not isinstance(value, list):
                    value = [value]
                ids = set(value)
                if ids:
                    exact = []
                    for eid in ids:
                        eid = validate_id(eid)
                        if eid:
                            if len(eid) == 64:
                                exact.append(f"x'{eid}'")
                            elif len(eid) > 2:
                                subwhere.append(f"event.hexid LIKE '{eid}%'")
                    if exact:
                        idstr = ','.join(exact)
                        subwhere.append(f'event.id IN ({idstr})')
                else:
                    # invalid query
                    raise ValueError("ids")
            elif key == 'authors' and isinstance(value, list):
                if value:
                    astr = ','.join("'%s'" % validate_id(a) for a in set(value))
                    if astr:
                        subwhere.append(f"(pubkey IN ({astr}) OR id IN (SELECT id FROM tag WHERE name = 'delegation' AND value IN ({astr})))")
                    else:
                        raise ValueError("authors")
                else:
                    # query with empty list should be invalid
                    raise ValueError("authors")
            elif key == 'kinds':
                if isinstance(value, list) and all(isinstance(k, int) for k in value):
                    subwhere.append('kind IN ({})'.format(','.join(str(int(k)) for k in value)))
                else:
                    raise ValueError("kinds")
            elif key == 'since':
                if value:
                    subwhere.append('created_at >= %d' % int(value))
                else:
                    raise ValueError("since")
            elif key == 'until':
                if value:
                    subwhere.append('created_at < %d' % int(value))
                else:
                    raise ValueError("until")
            elif key == 'limit' and value:
                filter_obj['limit'] = max(min(int(value or 0), self.default_limit), 0)
            elif key[0] == '#' and len(key) == 2 and value:
                pstr = []
                for val in set(value):
                    if val:
                        val = val.replace("'", "''")
                        pstr.append(f"'{val}'")
                if pstr:
                    pstr = ','.join(pstr)
                    subwhere.append(f"id IN (SELECT id FROM tag WHERE name = '{key[1]}' AND value IN ({pstr})) ")
        return filter_obj

    def build_query(self, filters):
        select = '''
        SELECT event.id, event.event FROM event
        '''
        where = set()
        limit = None
        new_filters = []
        for filter_obj in filters:
            subwhere = []
            try:
                filter_obj = self.evaluate_filter(filter_obj, subwhere)
            except ValueError:
                LOG.debug("bad query %s", filter_obj)
                filter_obj = {}
                subwhere = []
            if subwhere:
                subwhere = ' AND '.join(subwhere)
                where.add(subwhere)
            else:
                where.add('0')
            if 'limit' in filter_obj:
                limit = filter_obj['limit']
            new_filters.append(filter_obj)
        if where:
            select += ' WHERE (\n\t'
            select += '\n) OR (\n'.join(where)
            select += ')'
        if limit is None:
            limit = self.default_limit
        select += f'''
            LIMIT {limit}
        '''
        return select, new_filters


class BaseGarbageCollector(threading.Thread):
    def __init__(self, db_filename, **kwargs):
        super().__init__()
        self.log = logging.getLogger("nostr_relay.db:gc")
        self.db_filename = db_filename
        self.daemon = True
        self.running = True
        self.collect_interval = kwargs.get('collect_interval', 300)
        for k, v in kwargs.items():
            setattr(self, k, v)

    def collect(self, db):
        pass

    def run(self):
        import sqlite3
        self.log.info("Starting garbage collector %s. Interval %s", self.__class__.__name__, self.collect_interval)
        while self.running:
            db = sqlite3.connect(self.db_filename)
            db.execute("PRAGMA foreign_keys = ON;")
            try:
                collected = self.collect(db)
            except sqlite3.OperationalError as e:
                break
            if collected:
                self.log.info("Collected garbage (%d events)", collected)
                db.commit()
            db.close()
            time.sleep(self.collect_interval)

    def stop(self):
        self.running = False


class QueryGarbageCollector(BaseGarbageCollector):
    query = '''
        DELETE FROM event WHERE event.id IN
        (
            SELECT event.id FROM event
            LEFT JOIN tag on tag.id = event.id
            WHERE 
                (kind >= 20000 and kind < 30000)
            OR
                (tag.name = "expiration" AND tag.value < strftime("%s"))
        )
    '''

    def collect(self, db):
        cursor = db.cursor()
        cursor.execute(self.query)
        cursor.close()
        return max(0, cursor.rowcount)


def start_garbage_collector(options=None):
    options = options or Config.garbage_collector
    if options:
        gc_path = options.pop("class", "nostr_relay.db.QueryGarbageCollector")
        module_name, gc_class = gc_path.rsplit('.', 1)
        import importlib
        module = importlib.import_module(module_name)
        gc_obj = getattr(module, gc_class)(Config.db_filename, **options)
        gc_obj.start()
        return gc_obj


async def migrate(db):
    """
    Migrate the database
    """
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
                    tags = set((e.id_bytes, tag[0], tag[1]) for tag in e.tags if tag[0] == 'delegation' or len(tag[0]) == 1)
                    if tags:
                        await db.executemany("INSERT OR IGNORE INTO tag (id, name, value) VALUES (?, ?, ?)", tags)
                    count += 1
            LOG.info("migration: migrated %d events", count)
        except sqlite3.OperationalError:
            # events table doesn't exist
            LOG.debug("migration: events table does not exist")

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

    async def migrate_to_5(db):
        """
        Create the identity table
        """
        await db.execute("""
            CREATE TABLE IF NOT EXISTS identity (
                identifier TEXT PRIMARY KEY,
                pubkey TEXT,
                relays JSON
            );
        """)
        LOG.info("migration: created identity table")


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

