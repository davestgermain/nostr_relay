# Dynamic Allow/Deny Lists

nostr-relay can use nostr queries to compile lists of pubkeys that are allowed or denied access to the relay.  
Here is an example configuration:

```yaml

storage:
  validators:
    - nostr_relay.dynamic_lists.is_pubkey_allowed

dynamic_lists:
  check_interval: 1200
  allow_list_queries:
    - {"kinds": [3], "authors": ["c7da62153485ecfb1b65792c79ce3fe6fce6ed7d8ef536cb121d7a0c732e92df"]}
    - {"kinds": [31494], "#t": ["auth", "verification", "foaf"]}

  deny_list_queries:
    - {"kinds": [1984], "authors": ["c7da62153485ecfb1b65792c79ce3fe6fce6ed7d8ef536cb121d7a0c732e92df"]}
```


In this configuration, every 20 minutes, the relay will query itself for kind=3 events by `c7da62153485ecfb1b65792c79ce3fe6fce6ed7d8ef536cb121d7a0c732e92df` and add the tagged pubkeys to the allow list. It will also query for kind=31494 events, which are internal events used by the relay for bookkeeping. In this query, all events that are NIP-05 verified (see below) and in the [FOAF network](foaf.md) will be allowed.

Any pubkeys tagged in [kind=1984](https://github.com/nostr-protocol/nips/blob/master/56.md) events by certain authors will be added to a blocklist. You can use this as a blunt moderation tool.

On an empty relay, you may want to seed the allow list by adding a few pubkeys to the static whitelist:

```yaml
pubkey_whitelist:
    - 3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d
    - c7da62153485ecfb1b65792c79ce3fe6fce6ed7d8ef536cb121d7a0c732e92df
```

You could integrate [NIP-58](https://github.com/nostr-protocol/nips/blob/master/58.md) badges with dynamic lists, allowing badge recipients access to the relay. For instance, create a kind=30009 badge of type "supporter", and then specify a query to match recipients:

```yaml
 dynamic_lists:
  allow_list_queries:
    - {"kinds": [8], "authors": ["c7da62153485ecfb1b65792c79ce3fe6fce6ed7d8ef536cb121d7a0c732e92df"], "#a": ["30009:c7da62153485ecfb1b65792c79ce3fe6fce6ed7d8ef536cb121d7a0c732e92df:supporter"]}
```

## NIP-05 Verification

To enable [NIP-05 verification](https://github.com/nostr-protocol/nips/blob/master/5.md), install nostr-bot:

`pip install nostr-bot`

Then configure:

```yaml
verification:
  # options are disabled, passive, enabled
  # passive will verify identifiers but not
  nip05_verification: disabled
  expiration: 86400 * 30
  update_frequency: 3600
  #blacklist:
  # - badhost.biz
  #whitelist:
  # - goodhost.com

storage:
  validators:
    - nostr_relay.verification.is_nip05_verified
    - nostr_relay.dynamic_lists.is_pubkey_allowed

```

It's important that `nostr_relay.verification.is_nip05_verified` comes before `nostr_relay.dynamic_lists.is_pubkey_allowed`, because this will temporarily add the pubkey to the allow list

Then after restarting the server, run:

`nostr-relay -c config.yaml nip05 bot`

The bot will connect to the running relay and listen for kind=0 events, then verify the NIP-05 identifier and save the result back to the relay, as a kind=31494 event. This event will later be queried by the dynamic list task, and the pubkey will be added to the allow list.

To reverify all kind=0 events in the database:

`nostr-relay -c config.yaml nip05 reverify`


