"""
This recipe enables a private inbox/outbox style relay, using LMDB storage.
* Anyone in the pubkey_whitelist can post and read all events (if authenticated)
* Anyone else can post replies to somebody on the whitelist, but can't read anything other than whitelisted pubkeys
* Anyone can post a kind=10002 list of relays
* If forward_events=true, then saved events will automatically be forwarded to the preferred relays of those tagged in the event


Configuration yaml file:

pubkey_whitelist: 
    - c7da62153485ecfb1b65792c79ce3fe6fce6ed7d8ef536cb121d7a0c732e92df

output_validator: nostr_relay.recipe.homeserver.whitelist_output_validator

storage:
    class: nostr_relay.recipe.homeserver.PrivateLMDBStorage
    path: /path/to/db/
    validators:
        - nostr_relay.validators.is_signed
        - nostr_relay.recipe.homeserver.is_whitelisted_or_tagged

authentication:
    enabled: true
    relay_urls:
        - ws://127.0.0.1:6969

forward_events: true
"""

import asyncio
from aionostr import Relay
from nostr_relay.config import Config
from nostr_relay.errors import StorageError
from nostr_relay.storage.kv import LMDBStorage
from nostr_relay.storage.db import DBStorage
from nostr_relay.util import timeout


def is_whitelisted_or_tagged(event, config):
    """
    check that event is tagged with a configurable list of users
    """
    if event.kind == 10002:
        # allow NIP-65 lists
        return
    if event.pubkey not in config.pubkey_whitelist:
        found, match = event.has_tag("p", config.pubkey_whitelist)
        if not (found and match):
            raise StorageError(f"rejected: {event.pubkey} not allowed")


def whitelist_output_validator(event, context):
    """
    output only events that are in the Config.pubkey_whitelist
    authenticated users in the whitelist can see everything
    (output validators should return booleans rather than raise exceptions)
    """
    whitelist = context["config"].pubkey_whitelist
    auth_token = context["auth_token"]
    return (
        (event.pubkey in whitelist)
        or (auth_token.get("pubkey") in whitelist)
        or (event.kind == 10002)
    )


class PostSaveForward:
    async def _bounce_to_relay(self, relay_url, event):
        async with timeout(10):
            try:
                async with Relay(relay_url) as relay:
                    return await relay.add_event(event, check_response=True)
            except Exception as e:
                self.log.warning("bouncing %s to %s: %s", event.id, relay_url, str(e))
                return

    async def post_save(self, event, **kwargs):
        await super().post_save(event, **kwargs)
        if event.pubkey in Config.pubkey_whitelist and Config.forward_events:
            # to prevent loops, add our own relay to the list of already-delivered relays
            sent_to = set(Config.authentication.get("relay_urls", []))
            bounce_to = set([event.pubkey])
            for tag in event.tags:
                if tag[0] == "p":
                    bounce_to.add(tag[1])
            async for relay_list in self.run_single_query(
                [{"kinds": [10002], "authors": list(bounce_to)}]
            ):
                relays = set([r[1] for r in relay_list.tags if r[0] == "r"]) - sent_to
                if relays:
                    self.log.info("Sending %s to %s", event.id, relays)
                    tasks = [
                        asyncio.create_task(self._bounce_to_relay(r, event))
                        for r in relays
                    ]
                    done, _ = await asyncio.wait(tasks)
                    sent_to.update(relays)


class PrivateLMDBStorage(PostSaveForward, LMDBStorage):
    pass


class PrivateDBStorage(PostSaveForward, DBStorage):
    pass


PrivateStorage = PrivateLMDBStorage
