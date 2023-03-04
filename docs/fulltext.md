# Full-Text Searching

To enable full-text searching according to [NIP-50](https://github.com/nostr-protocol/nips/blob/master/50.md), use the [LMDB](lmdb.md) storage backend, and enable it in your config file:

```yaml
storage:
    class: nostr_relay.storage.kv.LMDBStorage
    path: /path/to/db-environment
    map_size: 209715200

fts_enabled: true
```

Then, reindex everything:

```console
nostr_relay -c /path/to/config.yaml fts reindex
```

This process can take quite a while. If you have a running server, run the reindex process before restarting the server, so that search queries will not start until the index completes.

The first line of the output will say `Starting reindex at 1677949599`. After the process completes, you can re-run indexing to catch up with more recent events:

```console
nostr_relay -c /path/to/config.yaml fts reindex --since 1677949599
```

# Implementation

Full-text indexing uses the pure python [whoosh](https://whoosh.readthedocs.io) library. In a multi-thread/multi-process environment, there can be only one writer, and readers do not affect writers. 

By default, the whoosh index files are stored in the `fts` subdirectory of your LMDB environment. To locate them elsewhere, configure:

```yaml
fts_index_path: /path/to/index/
```

# Querying

See NIP-50 for details on querying. To test it locally, run:

```console
nostr-relay -c /path/to/config.yaml query -q '{"search": "bitcoin"}'
```
