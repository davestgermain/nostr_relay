import logging

from nostr_relay.config import Config
from nostr_relay.errors import StorageError
from nostr_relay.storage import get_storage
from nostr_relay.util import Periodic


ALLOWED_PUBKEYS = set()

DENIED_PUBKEYS = set()


def is_pubkey_allowed(event, config):
    """
    Ensure that the pubkey is in the dynamic ALLOWED_PUBKEYS list
    and not in DENIED_PUBKEYS
    """
    if ALLOWED_PUBKEYS and bytes.fromhex(event.pubkey) not in ALLOWED_PUBKEYS:
        raise StorageError(f"{event.pubkey} is not allowed")
    if DENIED_PUBKEYS and bytes.fromhex(event.pubkey) in DENIED_PUBKEYS:
        raise StorageError(f"{event.pubkey} is not allowed")


class ListBuilder(Periodic):
    def __init__(self):
        self.log = logging.getLogger("nostr_relay.lists")
        self.options = Config.get("dynamic_lists", {})
        self.initial = []

        if Config.service_pubkey:
            self.initial.append(Config.service_pubkey)
        if Config.pubkey_whitelist:
            self.initial.extend(Config.pubkey_whitelist)
        Periodic.__init__(
            self,
            self.options.get("check_interval", 7200),
            swallow_exceptions=True,
            run_at_start=True,
        )

    async def run_once(self):
        self.log.info("Refreshing global lists")
        for list_kind, global_set in (
            ("allow", ALLOWED_PUBKEYS),
            ("deny", DENIED_PUBKEYS),
        ):
            queries = self.options.get(f"{list_kind}_list_queries", [])
            if queries:
                local_set = set()
                event_count = 0
                pubkey_count = 0
                async for event in get_storage().run_single_query(queries):
                    for tag in event.tags:
                        if len(tag) >= 2 and tag[0] == "p":
                            pubkey = tag[1].lower()
                            if len(pubkey) == 64 and not any(
                                c not in "abcdef0123456789" for c in pubkey
                            ):
                                local_set.add(bytes.fromhex(pubkey))
                                pubkey_count += 1
                    event_count += 1
                global_set.clear()
                global_set.update(local_set)
                self.log.info(
                    "Loaded %s list with %d pubkeys from %d events",
                    list_kind,
                    pubkey_count,
                    event_count,
                )
        if ALLOWED_PUBKEYS and self.initial:
            # add the preconfigured keys
            ALLOWED_PUBKEYS.update(bytes.fromhex(p) for p in self.initial)

    async def start(self):
        if self.options:
            self.log.info(
                "Starting list builder task. Interval %s",
                self.interval,
            )
            await super().start()
