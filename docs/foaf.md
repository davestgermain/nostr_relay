# Social Network

nostr-relay can restrict access to publishing if the pubkey is not in your social network.

To configure, add `nostr_relay.foaf.is_in_foaf` to your list of validators (see [Storage](storage.md) for more) and configure which pubkey(s) to search for:

```yaml
storage:
    validators:
        - nostr_relay.foaf.is_in_foaf

foaf:
    network_pubkeys: 
        - <your pubkey here>
```

Here are the complete configuration options:

```yaml
foaf:
    # list of starting pubkeys to search
    network_pubkeys:
        - c7da62153485ecfb1b65792c79ce3fe6fce6ed7d8ef536cb121d7a0c732e92df
        - 3bf0c63fcb93463407af97a5e5ee64fa883d107ef9e558472c4eb9aaaefa459d

    # levels to search. Default is 1
    levels: 2

    # file to save the network to, to avoid querying on relay start
    save_to: /tmp/nostr-foaf.json

    # relays to query. You can use your own relay address here...
    check_relays:
        - wss://nos.lol
        - wss://relay.snort.social

    # how often to refresh the network
    check_interval: 7200
```
