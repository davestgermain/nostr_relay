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

LOG = logging.getLogger(__name__)

def validate_id(obj_id):
    obj_id = obj_id.lower().strip()
    if obj_id.isalnum():
        return obj_id


class Storage:
    CREATE_TABLE = """
        CREATE TABLE if not exists events (
            id TEXT PRIMARY KEY,
            pubkey TEXT,
            created_at INT,
            kind INT,
            tags JSON,
            content TEXT,
            sig TEXT
        );
        CREATE INDEX if not exists authoridx on events (pubkey);
        CREATE INDEX if not exists createdidx on events (created_at);
        CREATE INDEX if not exists kindidx on events (kind);
        CREATE INDEX if not exists jsonidx on events (tags);
        CREATE TABLE if not exists tags (
            id TEXT,
            name TEXT,
            value TEXT
        );
        CREATE UNIQUE INDEX IF NOT EXISTS tag_composite_index ON tags (id, name, value);
        """.split(';')

    INSERT_EVENT = 'insert or ignore into events (id, pubkey, created_at, kind, tags, content, sig) values (?, ?, ?, ?, ?, ?, ?)'

    def __init__(self, filename='nostr.sqlite3'):
        self.filename = filename
        self.clients = collections.defaultdict(dict)
        self.db = None
        self.newevent_event = asyncio.Event()

    async def setup_db(self):
        LOG.info(f"Database file {self.filename} {'exists' if os.path.exists(self.filename) else 'does not exist'}")
        LOG.info("Creating db tables")
        async with aiosqlite.connect(self.filename) as db:
            for stmt in self.CREATE_TABLE:
                await db.execute(stmt)
            await db.commit()
        self.db = await aiosqlite.connect(self.filename)
        await self.db.execute('pragma journal_mode=wal')

    async def get_event(self, event_id):
        """
        Shortcut for retrieving an event by id
        """
        async with self.db.cursor() as cursor:
            await cursor.execute('select * from events where id = ?', (event_id, ))
            row = await cursor.fetchone()
            if row:
                return Event.from_tuple(row)

    async def add_event(self, event_json):
        """
        Add an event from json object
        Return (status, event)
        """
        event = Event(**event_json)

        if self.validate_event(event):
            changed = False
            async with self.db.cursor() as cursor:
                event = await self.pre_process(cursor, event)
                if event:
                    await cursor.execute(self.INSERT_EVENT, event.to_tuple())
                    changed = bool(cursor.rowcount)
                    await self.post_process(cursor, event)
                await self.db.commit()
            if changed:
                # notify all subscriptions
                self.newevent_event.set()
        else:
            LOG.warning('BAD EVENT %s', event)
            return False, event
        return True, event

    def validate_event(self, event):
        """
        Validate basic format and signature
        """
        return event.verify()

    async def check_whitelist(self, cursor, event):
        await cursor.execute('select expiration from whitelist where key = ? and type = "author"', (event.pubkey,))
        row = await cursor.fetchone()
        if row:
            expiration = row[0]
            if expiration is not None:
                if expiration < time.time():
                    return False
            return True
        return False

    async def pre_process(self, cursor, event):
        """
        Pre-process the event to check permissions, duplicates, etc.
        Return None to skip adding the event.
        """
        if event.is_ephemeral or event.kind > 30000:
            # don't save ephemeral or unspecified events
            return None
        elif event.is_replaceable:
            # check for older event from same pubkey
            await cursor.execute('select * from events where pubkey = ? and kind = ? and created_at < ?', (event.pubkey, event.kind, event.created_at))
            row = await cursor.fetchone()
            if row:
                old_id = row[0]
                old_ts = row[2]
                LOG.info("Replacing event %s from %s@%s with %s", old_id, event.pubkey, old_ts, event.id)
                await cursor.execute('delete from events where id = ?', (old_id, ))
        return event

    async def post_process(self, cursor, event):
        """
        Post-process event
        (clear old metadata, update tag references)
        """

        if cursor.rowcount:
            if event.kind in (EventKind.SET_METADATA, EventKind.CONTACTS):
                # older metadata events can be cleared
                query = 'DELETE FROM events WHERE pubkey = ? AND kind = ? AND created_at < ?'
                LOG.debug("q:{} kind:{}, key:{}", query, event.kind, event.pubkey)
                await cursor.execute(query, (event.pubkey, event.kind, event.created_at))
            elif event.kind in (EventKind.TEXT_NOTE, EventKind.ENCRYPTED_DIRECT_MESSAGE) and event.tags:
                # update mentions
                for tag in event.tags:
                    name = tag[0]
                    if len(name) == 1:
                        # single-letter tags can be searched
                        ptag = validate_id(tag[1])
                        if ptag:
                            await cursor.execute('INSERT OR IGNORE INTO tags (id, name, value) VALUES (?, ?, ?)', (event.id, name, ptag))
            elif event.kind == EventKind.DELETE and event.tags:
                # delete the referenced events
                for tag in event.tags:
                    name = tag[0]
                    if name == 'e':
                        event_id = tag[1]
                        await cursor.execute('DELETE FROM events WHERE id = ? AND pubkey = ?', (event_id, event.pubkey))
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
                        event = Event.from_tuple(row)
                        if event.id in seen_ids:
                            continue
                        seen_ids.add(event.id)
                        self.queue.append(event)
                if runs == 1 and len(seen_ids) < 1000:
                    # send a sentinel to indicate we have no more events
                    self.queue.append(None)
                duration = int((time.time() - start) * 1000)

                LOG.debug('waiting %s runs:%s queue:%s duration:%dms', self.sub_id, runs, len(self.queue), duration)

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
        SELECT events.* FROM events 
        LEFT JOIN tags ON tags.id = events.id
        '''
        where = []
        for filter_obj in filters:
            subwhere = []
            if 'ids' in filter_obj:

                ids = filter_obj['ids']
                if not isinstance(ids, list):
                    ids = [ids]
                ids = set(ids)
                eq = ''
                while ids:
                    eid = validate_id(ids.pop())
                    if eid:
                        eq += "events.id like '%s%%'" % eid
                        if ids:
                            eq += ' OR '
                    else:
                        pass
                if eq:
                    subwhere.append(f'({eq})')

            if 'authors' in filter_obj:
                astr = ','.join("'%s'" % validate_id(a) for a in set(filter_obj['authors']))
                if astr:
                    subwhere.append('pubkey in ({})'.format(astr))

            if 'kinds' in filter_obj:
                subwhere.append('kind in ({})'.format(','.join(str(int(k)) for k in filter_obj['kinds'])))

            if 'since' in filter_obj:
                subwhere.append('created_at >= %d' % int(filter_obj['since']))

            if 'until' in filter_obj:
                subwhere.append('created_at < %d' % int(filter_obj['until']))

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
                        subwhere.append(f'(tags.name = "{tagname}" and tags.value in ({pstr})) ')
            if subwhere:
                subwhere = ' AND '.join(subwhere)
                where.append(subwhere)
        if where:
            select += ' WHERE ('
            select += ') OR ('.join(where)
            select += ')'
        select += '''
            ORDER BY created_at DESC LIMIT 1000
        '''
        return select


