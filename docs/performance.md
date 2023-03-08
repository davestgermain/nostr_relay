# Performance

nostr-relay is designed to be performant and scalable "out of the box". The default configuration can support hundreds of connections, serving about 100 requests per second. But the default sqlite storage backend offers the worst performance (because the aiosqlite driver is not optimized for async access). With a few configuration changes, you can greatly increase throughput.

Here are some tips for squeezing the most performance out of nostr-relay:

## Use pypy

The easiest improvement is to install [pypy](https://www.pypy.org) version 3.9 or higher. The only limitation to using pypy is that the PostgreSQL driver (asyncpg) is not compatible. Sqlite and LMDB work well, however.

If you can't use pypy, be sure to use cPython 3.11 or higher.

## Use LMDB

Configure [LMDB](lmdb.md) as your storage backend. See the [python LMDB docs](https://lmdb.readthedocs.io/en/release/#environment-class) for more tuning tips.

## Use Purple

Version 1.12 introduces the "purple" server: a minimal websockets server that is intended to be the fastest method of serving nostr-relay. The implementation is subject to change, but currently it is limited to serving only the websockets (not `/e/<id>` or the other endpoints defined in web.py).

To use purple simply run it like so:

```console
nostr-relay serve --use-purple
```

## Turn off noisy logging

Disabling logging can improve performance. Add this to your config file:
```yaml
logging:
  version: 1
  handlers:
    empty:
      class: logging.NullHandler
      level: WARNING
    console:
      class: logging.StreamHandler
      level: ERROR
      stream: ext://sys.stdout
  root:
    level: WARNING
    handlers: [empty]
```

Logging is extremely configurable. See the [python logging docs](https://docs.python.org/3/library/logging.config.html) for more.

## Complete Configuration Example

Here's a complete high-performance configuration file, using LMDB:

```yaml
DEBUG: false

storage:
  class: nostr_relay.storage.kv.LMDBStorage
  path: ./data/nostr-fast.db
  map_size: 209715200
  metasync: false
  pool_size: 8

purple:
  host: 127.0.0.1
  port: 6969
  # compression is great for saving bandwidth, but it kills performance
  disable_compression: true
  workers: 4

relay_name: fast python relay
relay_description: a super fast implementation
sysop_pubkey: 
sysop_contact: 
nip05_verification: disabled


garbage_collector:
  collect_interval: 600


logging:
  version: 1
  handlers:
    empty:
      class: logging.NullHandler
      level: WARNING
    console:
      class: logging.StreamHandler
      level: ERROR
      stream: ext://sys.stdout
  root:
    level: WARNING
    handlers: [empty]
```

To use PostgreSQL, switch the storage section with this:

```yaml
storage:
  sqlalchemy.url: 'postgresql+asyncpg://user:pass@localhost/nostr'
  sqlalchemy.max_overflow: 80
  sqlalchemy.pool_size: 4
  num_concurrent_reqs: 10
  num_concurrent_adds: 10
```

Then, be sure to run with the purple server:

```console
nostr-relay -c config.yaml serve --use-purple
```


## Benchmarks

Use aionostr to benchmark your relay:

```console
aionostr bench -r ws://127.0.0.1:6969 -f req_per_second
```

This will execute a request for 50 events, in a tight loop. Use `-c` to increase the concurrency.

```console
aionostr bench -r ws://127.0.0.1:6969 -f events_per_second
```

This will execute a request for 500 events, measuring the number of events sent to the client.
