import asyncio
import sqlite3
import time
import collections
import traceback
import aiosqlite
import rapidjson
from .event import Event, EventKind


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

    async def add_event(self, event_json):
        event = Event(**event_json)

        if self.validate_event(event):
            print(f'Adding {event.id}')
            async with aiosqlite.connect(self.filename) as db:
                # for stmt in self.CREATE_TABLE:
                #     await db.execute(stmt)
                event = await self.pre_process(db, event)
                if event:
                    await db.execute(self.INSERT_EVENT, event.to_tuple())
                    await self.post_process(db, event)
                await db.commit()
        else:
            print(f'BAD EVENT {event}')
            return False, event
        return True, event

    def validate_event(self,event):
        """
        Validate basic format and signature
        """
        return event.verify()

    async def pre_process(self, db, event):
        """
        Pre-process the event to check permissions, duplicates, etc.
        Return None to skip adding the event.
        """
        return event

    async def post_process(self, db, event):
        if event.kind in (EventKind.SET_METADATA, EventKind.CONTACTS):
            # older metadata events can be cleared
            query = 'delete from events where pubkey = ? and kind = ? and created_at < ?'
            print(query, event.kind, event.pubkey)
            await db.execute(query, (event.pubkey, event.kind, event.created_at))
        elif event.kind in (EventKind.TEXT_NOTE, EventKind.ENCRYPTED_DIRECT_MESSAGE) and event.tags:
            # update mentions
            for tag in event.tags:
                name = tag[0]
                if len(name) == 1:
                    # single-letter tags can be searched
                    ptag = validate_id(tag[1])
                    if ptag:
                        await db.execute('insert or ignore into tags (id, name, value) values (?, ?, ?)', (event.id, name, ptag))
        elif event.kind == EventKind.DELETE and event.tags:
            # delete the referenced events
            for tag in event.tags:
                name = tag[0]
                if name == 'e':
                    event_id = tag[1]
                    await db.execute('delete from events where id = ? and pubkey = ?', (event_id, event.pubkey))

    def read_subscriptions(self, client_id):
        for task, sub in self.clients[client_id].values():
            yield from sub.read()

    def subscribe(self, client_id, sub_id, filters):
        print(f'Subscribing {client_id} {sub_id} to {filters}')
        if sub_id in self.clients[client_id]:
            self.unsubscribe(client_id, sub_id)
        sub = Subscription(self.filename, sub_id, filters)
        if sub.prepare():
            task = asyncio.create_task(sub.start())
            self.clients[client_id][sub_id] = (task, sub)

    def unsubscribe(self, client_id, sub_id=None):
        print(f'Unsubscribing {client_id} {sub_id}')
        if sub_id:
            try:
                task, sub = self.clients[client_id][sub_id]
                sub.cancel()
                task.cancel()
                del self.clients[client_id][sub_id]
            except KeyError:
                pass
        else:
            for task, sub in self.clients[client_id].values():
                sub.cancel()
                task.cancel()
            del self.clients[client_id]



class Subscription:
    def __init__(self, db, sub_id, filters:list):
        self.db  = db
        self.sub_id = sub_id
        self.filters = filters
        self.queue = collections.deque()
        self.running = True
        self.interval = 10
        self.queries = set()

    def prepare(self):
        for filter_obj in self.filters:
            ids = filter_obj.get('ids', []) or []
            authors = filter_obj.get('authors', []) or []
            kinds = filter_obj.get('kinds', []) or []
            assert isinstance(ids, list)
            assert isinstance(authors, list)
            assert isinstance(kinds, list)
            since = filter_obj.get('since', 0) or 0
            until = filter_obj.get('until', 0) or 0
            limit = filter_obj.get('limit', None)
            ptag = filter_obj.get('#p', None) or []
            etag = filter_obj.get('#e', None) or []
            try:
                query = build_query(
                    ids=ids,
                    authors=authors,
                    kinds=kinds,
                    since=since,
                    until=until,
                    limit=limit,
                    ptag=ptag,
                    etag=etag,
                )
                self.queries.add(query)
            except Exception:
                traceback.print_exc()
                print(f"Bad query {filter_obj}")
        return bool(self.queries)

    async def start(self):
        print(f'Starting {self.sub_id}')
        print(self.queries)
        seen_ids = set()
        runs = 0
        while self.running:
            runs += 1
            try:
                start = time.time()
                async with aiosqlite.connect(self.db) as db:
                    for query in self.queries:
                        async with db.execute(query) as cursor:
                            async for row in cursor:
                                event = Event.from_tuple(row)
                                if event.id in seen_ids:
                                    continue
                                seen_ids.add(event.id)
                                self.queue.append(event)
                duration = int((time.time() - start) * 1000)
                print(f'waiting {self.sub_id} runs:{runs} queue:{len(self.queue)} duration:{duration}ms')
                await asyncio.sleep(self.interval)
            except Exception:
                traceback.print_exc()
                break
        print(f'Stopped {self.sub_id}')

    def read(self):
        while self.queue:
            event = self.queue.popleft()
            yield self.sub_id, event

    def cancel(self):
        self.running = False


def build_query(ids=None, authors=None, kinds=None, etag=None, ptag=None, since=0, until=None, limit=None):
    select = 'select events.* from events '
    where = 'where 1 '
    if ids:
        ids = set(ids)
        eq = ''
        while ids:
            eid = validate_id(ids.pop())
            if eid:
                eq += "id like '%s%%'" % eid
                if ids:
                    eq += ' OR '
            else:
                pass
        if eq:
            where += f' AND ({eq})'

    if authors:
        astr = ','.join("'%s'" % validate_id(a) for a in set(authors))
        if astr:
            where += 'AND pubkey in ({})'.format(astr)
        # while authors:
        #     author = validate_id(authors.pop())
        #     if author:
        #         aq += "pubkey like '%s%%'" % author
        #         if authors:
        #             aq += ' OR '

    if ptag or etag:
        select += ' LEFT JOIN tags on tags.id = events.id '

        pstr = ','.join("'%s'" % validate_id(a) for a in set(ptag))
        if pstr:
            where += f' AND (tags.name = "p" and tags.value in ({pstr})) '
        estr = ','.join("'%s'" % validate_id(a) for a in set(etag))
        if estr:
            where += f' AND (tags.name = "e" and tags.value in ({estr})) '

    if kinds:
        where += ' AND kind in ({})'.format(','.join(str(int(k)) for k in kinds))

    if since:
        where += ' AND created_at >= %d' % int(since)

    if until:
        where += ' AND created_at < %d' % int(until)

    query = select + where
    query += ' ORDER BY created_at'
    limit = int(limit or 1000)
    if limit > 1000:
        limit = 1000
    query += ' LIMIT %d' % limit

    return query


