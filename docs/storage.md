# Storage

nostr-relay stores events using SQLAlchemy, with support for SQLite and PostgreSQL.  
The default configuration uses SQLite. To change the location of the database:

```yaml
storage:
    sqlalchemy.url: sqlite+aiosqlite:////full/path/to/nostr.sqlite3
```

To use PostgreSQL:

```yaml
storage:
    sqlalchemy.url: postgresql+asyncpg://username:password@dbhost/nostr
```

See the [SQLAlchemy docs](https://www.sqlalchemy.org) for more on connection URLs.

To use LMDB:

```yaml
storage:
    class: nostr_relay.storage.kv.LMDBStorage
    path: /path/to/db-environment
    map_size: 209715200
```

See [LMDB documentation](lmdb.md) for more options

## SQLAlchemy Options

Other SQLAlchemy options will be passed into the `create_engine` call:

```yaml
storage:
    sqlalchemy.url: postgresql+asyncpg://username:password@dbhost/nostr
    sqlalchemy.max_overflow: 80
    sqlalchemy.pool_size: 4
    sqlalchemy.pool_timeout: 60.0
```

This will create a pool of 4 DB connections, with a maximum temporary overflow of 84 connections.


## Concurrency

nostr-relay manages concurrent access to the databse, outside of the SQLAlchemy pool configuration.

To allow 10 concurrent read requests and 2 concurrent event adds:

```yaml
storage:
    sqlalchemy.url: sqlite+aiosqlite:///nostr.sqlite3
    num_concurrent_reqs: 10
    num_concurrent_adds: 2
```

These are the default settings, appropriate for small relays using SQLite, which cannot handle many concurrent adds.

Using SQLite, you can easily increase `num_concurrent_reqs`. Using PostgreSQL, the concurrency scales much better.  
A complete configuration might look like this:

```yaml
storage:
    sqlalchemy.url: postgresql+asyncpg://username:password@dbhost/nostr
    sqlalchemy.max_overflow: 80
    sqlalchemy.pool_size: 4
    num_concurrent_reqs: 60
    num_concurrent_adds: 20
```


## Validators

nostr-relay has a configurable event validator pipeline, to check events before saving to the database.  
The default configuration looks like this:

```yaml
storage:
  sqlalchemy.url: sqlite+aiosqlite:///nostr.sqlite3
  validators:
    - nostr_relay.validators.is_not_too_large
    - nostr_relay.validators.is_signed
    - nostr_relay.validators.is_recent
```

`is_not_too_large` checks configuration option `max_event_size`  
`is_recent` checks configuration option `oldest_event`

The defined functions will execute in order. You can add custom validators, as long as they are importable functions that have this interface:

```python
def my_validator(event, config)
```

`my_validator` will be called with the event and the configuration object. If the event is invalid, raise `nostr_relay.errors.StorageError`.  
It's best to keep your validator functions small, without side-effects.

To require proof of 20 bits of work:

```yaml
storage:
  sqlalchemy.url: sqlite+aiosqlite:///nostr.sqlite3
  validators:
    - nostr_relay.validators.is_not_too_large
    - nostr_relay.validators.is_signed
    - nostr_relay.validators.is_recent
    - nostr_relay.validators.is_pow

require_pow: 20
```

Other validators include `is_author_blacklisted`, `is_author_whitelisted`, and `is_certain_kind`

See the [code](https://code.pobblelabs.org/fossil/nostr_relay/file?name=nostr_relay/validators.py&ci=tip) for the complete list of installed validators.


## Customization

To use a different class for subscriptions:  

```yaml
storage:
    sqlalchemy.url: sqlite+aiosqlite:///nostr.sqlite3
    subscription_class: my_module.MySubscription
```

TODO: describe Subscription interface

To use a different storage class entirely:

```yaml
storage:
    sqlalchemy.url: sqlite+aiosqlite:///nostr.sqlite3
    class: my_module.MyStorage
```

