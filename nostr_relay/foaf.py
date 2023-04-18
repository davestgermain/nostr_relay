"""
Validate that the pubkey is in your network

To enable, add this to your configuration file:

foaf:
    network_pubkeys: 
        - <your pubkey here>

See https://code.pobblelabs.org/fossil/nostr_relay/doc/tip/docs/foaf.md for all of the configuration options
"""


import asyncio
import logging
import time

from itertools import islice
from aionostr import Manager
from aionostr.event import Event
from nostr_relay.dynamic_lists import ALLOWED_PUBKEYS
from nostr_relay.util import Periodic
from nostr_relay.config import Config
from nostr_relay.storage import get_storage


def is_in_foaf(event, config):
    """
    Check that the pubkey is in the configured social network
    """
    import warnings

    warnings.warn(
        "Deprecated. See https://code.pobblelabs.org/fossil/nostr_relay/doc/tip/docs/foaf.md"
    )


class FOAFBuilder(Periodic):
    """
    Periodically build the social network
    """

    def __init__(self):
        self.private_key = Config.service_privatekey
        self.public_key = Config.service_pubkey

        self.relay_urls = Config.foaf.get("check_relays", ["wss://nos.lol"])
        self.log = logging.getLogger("nostr_relay.foaf")
        self.network_levels = Config.foaf.get("levels", 1)
        self.seed_authors = Config.foaf.get(
            "network_pubkeys",
            ["c7da62153485ecfb1b65792c79ce3fe6fce6ed7d8ef536cb121d7a0c732e92df"],
        )
        self.batch_size = Config.foaf.get("batch_size", 100)
        self._first_run = True
        Periodic.__init__(
            self,
            Config.foaf.get("check_interval", 7200),
            swallow_exceptions=True,
            run_at_start=False,
            timeout=10,
        )

    async def save(self, network):
        if not network:
            return

        ALLOWED_PUBKEYS.update(bytes.fromhex(pubkey) for pubkey in network)
        if not self.private_key:
            self.log.error("Config.service_privatekey is not set")
            return
        self.log.info("Saving network to storage")
        storage = get_storage()
        # first, clear out old events
        del_count = 0
        async for event in storage.run_single_query(
            [{"kinds": [31494], "#t": ["foaf"], "authors": [self.public_key]}]
        ):
            await storage.delete_event(event.id)
            del_count += 1
        if del_count:
            self.log.info("Deleted %d existing foaf events", del_count)

        expiration = str(int(time.time() + (2 * self.interval)))
        # then, batch up the network into service events
        i = 1
        for batch in batched(list(network), self.batch_size):
            tags = [["t", "foaf"], ["expiration", expiration], ["d", f"batch-{i}"]]
            for author in batch:
                if not author.startswith("npub"):
                    tags.append(["p", author])
            event = Event(pubkey=self.public_key, kind=31494, tags=tags)
            event.sign(self.private_key)
            await storage.add_event(event.to_json_object())
            i += 1
            self.log.info("Saved batch of %d pubkeys in event %s", len(batch), event.id)

    async def wait_function(self):
        interval = self.interval
        if self._first_run:
            # if there are already records, wait for the normal amount of time
            async for event in get_storage().run_single_query(
                [{"kinds": [31494], "#t": ["foaf"]}]
            ):
                interval = max(time.time() - event.created_at, interval)
                break
            else:
                # otherwise, query right away
                interval = 1.0
            self._first_run = False
        await asyncio.sleep(interval)

    async def run_once(self):
        find_query = {
            "kinds": [3],
            "authors": self.seed_authors,
        }
        network = set(self.seed_authors)
        async with Manager(self.relay_urls, private_key=self.private_key) as manager:
            self.log.info(
                "Getting following for %s from %s", self.seed_authors, self.relay_urls
            )
            async for event in manager.get_events(find_query):
                for tag in event.tags:
                    if tag[0] == "p" and not tag[1].startswith("npub"):
                        network.add(tag[1])
            found = 1
            while found < self.network_levels:
                self.log.info("Getting extended network. Level %d", found)

                for batch in batched(list(network), 100):
                    find_query["authors"] = batch
                    async for event in manager.get_events(find_query):
                        for tag in event.tags:
                            if tag[0] == "p" and not tag[1].startswith("npub"):
                                network.add(tag[1])
                    self.log.info("Got batch of 100...")
                found += 1

        self.log.info("Found network of %d pubkeys", len(network))
        await self.save(network)


def batched(iterable, n):
    it = iter(iterable)
    while True:
        batch = list(islice(it, n))
        if not batch:
            return
        yield batch
