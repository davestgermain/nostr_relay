This is a python3 implementation of a [nostr](https://github.com/nostr-protocol/nostr) relay.

To intall:

`pip install nostr-relay`

To run:

`nostr-relay serve`

to change the location of the sqlite database and other settings, create a yaml config file that looks [like this](https://code.pobblelabs.org/fossil/nostr_relay/file?name=config/config.yaml):

and run with `nostr-relay -c /path/to/config.yaml serve`


Then add `ws://127.0.0.1:6969` to your relay list.

(obviously, in production you should use a TLS certificate)
