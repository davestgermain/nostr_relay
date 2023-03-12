This is a Python 3.9+ implementation of a [nostr](https://github.com/nostr-protocol/nostr) relay.

## Features

* Stores data in LMDB, SQLite or Postgresql. See [Storage](https://code.pobblelabs.org/fossil/nostr_relay/doc/tip/docs/storage.md)
* [Scalable](https://code.pobblelabs.org/fossil/nostr_relay/doc/tip/docs/performance.md) to thousands of concurrent clients
* [Dynamic allow/deny lists](https://code.pobblelabs.org/fossil/nostr_relay/doc/tip/docs/dynamic_lists.md)
* Configurable garbage collector
* Configurable [event validators](https://code.pobblelabs.org/fossil/nostr_relay/doc/tip/docs/storage.md)
* [Full-text indexing](https://code.pobblelabs.org/fossil/nostr_relay/doc/tip/docs/fulltext.md)
* Can serve as a NIP-05 [identity provider](https://code.pobblelabs.org/fossil/nostr_relay/wiki?name=idp)
* Support for NIP-42 [authentication](https://code.pobblelabs.org/fossil/nostr_relay/doc/tip/docs/authentication.md)
* Support for [rate-limiting](https://code.pobblelabs.org/fossil/nostr_relay/doc/tip/docs/rate_limits.md)
* Supports more NIPs than any other relay implementation:
    * [NIP-01](https://github.com/nostr-protocol/nips/blob/master/01.md) – basic protocol
    * [NIP-02](https://github.com/nostr-protocol/nips/blob/master/02.md) – contact lists
    * [NIP-05](https://github.com/nostr-protocol/nips/blob/master/05.md) – verifying identity
    * [NIP-09](https://github.com/nostr-protocol/nips/blob/master/09.md) – deletion events
    * [NIP-11](https://github.com/nostr-protocol/nips/blob/master/11.md) – relay metadata
    * [NIP-12](https://github.com/nostr-protocol/nips/blob/master/12.md) – generic tags
    * [NIP-15](https://github.com/nostr-protocol/nips/blob/master/15.md) – EOSE
    * [NIP-20](https://github.com/nostr-protocol/nips/blob/master/20.md) – command results
    * [NIP-26](https://github.com/nostr-protocol/nips/blob/master/26.md) – delegated events
    * [NIP-33](https://github.com/nostr-protocol/nips/blob/master/33.md) – parameterized replaceable events
    * [NIP-40](https://github.com/nostr-protocol/nips/blob/master/40.md) – expiration events
    * [NIP-42](https://github.com/nostr-protocol/nips/blob/master/42.md) – [authentication](https://code.pobblelabs.org/fossil/nostr_relay/doc/tip/docs/authentication.md)
    * [NIP-50](https://github.com/nostr-protocol/nips/blob/master/50.md) – [full-text search](https://code.pobblelabs.org/fossil/nostr_relay/doc/tip/docs/fulltext.md)
    * [NIP-65](https://github.com/nostr-protocol/nips/blob/master/65.md) – Relay lists
* Pluggable features, allowing you to use nostr_relay as a library for your own custom implementation


## Installation

`pip install nostr-relay`

To run:

`nostr-relay serve`

to change the location of the database and other settings, create a yaml config file that looks [like this](https://code.pobblelabs.org/fossil/nostr_relay/file?name=nostr_relay/config.yaml):

and run with `nostr-relay -c /path/to/config.yaml serve`


Then add `ws://127.0.0.1:6969` to your relay list.

(obviously, in production you should use a TLS certificate)

Visit [the nostr-relay fossil repository](https://code.pobblelabs.org/fossil/nostr_relay) for more information.

