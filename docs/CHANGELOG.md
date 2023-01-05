# CHANGES

## 1.2.6

* bugfix: enable foreign keys on the garbage collector

## 1.2.5

* Using subselects for tag queries greatly increases performance

## 1.2.4

* bugfix: foreign keys were not enabled on the sqlite connection
* optimize db upon close

## 1.2.3

* Replace replaceable events if the created time is the same as the replaced event

## 1.2.2

* Reject events that are > 1 hour in the future
* Process tags for all events
* Added cli to reprocess event tags
* Added convenience functions to run the server programatically
   `nostr_relay.web.run_with_gunicorn()`
   `nostr_relay.web.run_with_uvicorn()`

## 1.2.1

* config file wasn't include in wheel
 
## 1.2

* Added rate limiter

## 1.1.8

* Support for NIP-40 -- event expiration tag
 