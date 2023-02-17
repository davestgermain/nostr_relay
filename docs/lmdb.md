# LMDB Storage Backend

nostr-relay can use an LMDB storage backend. To install: `pip install "nostr-relay[lmdb]"`

In your configuration file:
```yaml
storage:
    class: nostr_relay.storage.kv.LMDBStorage
    path: /path/to/db-environment
    map_size: 209715200
```

`map_size` is the maximum size of the database. The default is small, to prevent filling the disk. Writes will fail once the database fills, but you can simply increase the size and restart. The database is not pre-allocated.

See the [py-lmdb documentation](https://lmdb.readthedocs.io/en/release/#environment-class) for all of the available options.

Recommended options: `metasync: false` and `lock: false` (if you're only running one worker process). `writemap: true` is recommended on linux only.

# Limitations

As of version 1.10.5, authentication works if you set `service_privatekey` in your config file to a private key used for creating internal events.

NIP-05 verification does not yet work with the LMDB backend.

# Implementation Details

Event data is stored as key/value pairs, where the key is '\x00' + the event id (as bytes), and the value is a [msgpack](http://msgpack.org) tuple.

A variety of indexes are employed, to cover all of the possible nostr query patterns.

To run a query, we use a pipeline:

`executor(lmdb_environment, filters)` – runs the planner and executes each query using a threadpool, collecting the results in a `collections.deque`  

`planner(filters)` – takes the nostr query and creates a QueryPlan, sanitizing the query and choosing which indexes to use. If multiple indexes are necessary, a `MultiIndex` is instantiated, which chains the results from one index to the next. A crude algorithm chooses the order of the multi-index chain.

`index.scanner` – the chosen index returns an iterator which yields event ids (as bytes), based on the passed in matches and `created_at` constraints

`matcher(iterator)` – loops over the iterator, matching each event against the original nostr query and yielding `Event` objects. To speed the matching process, the original query is compiled into a function and cached in a process-wide LRU cache. The matcher acts on the msgpack tuples and only instantiates the data into `Event` objects if a match succeeds. 

# pypy

Using [pypy](https://www.pypy.org) can greatly increase performance. Be sure to install [pypy 3.9](https://www.pypy.org/download.html).  
