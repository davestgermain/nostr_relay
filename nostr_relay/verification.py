"""
This handles pubkey verification according to NIP-05

if Config.verification.nip05_verification is set to 'enabled', NIP-05 identifiers will be validated

When a kind=0 (metadata) event is saved, it will be considered a candidate
for verification if it contains a nip05 tag.

Every Config.verification.update_frequency, the verifications will be reprocessed.

See https://code.pobblelabs.org/fossil/nostr_relay/doc/tip/docs/dynamic_lists.md for all of the configuration options
"""
import asyncio
import re
import time

from .errors import VerificationError
from .util import json_loads, json_dumps
from .dynamic_lists import ALLOWED_PUBKEYS
from .config import Config

from nostr_bot import CommunicatorBot


def get_aiohttp_session():
    import aiohttp

    return aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(total=60.0), json_serialize=json_dumps
    )


class NIP05CheckerBot(CommunicatorBot):
    """
    A bot that listens for kind=0 events and verifies NIP-05 identifiers
    """

    LISTEN_KIND = 0
    LISTEN_PUBKEY = None
    PRIVATE_KEY = Config.service_privatekey

    def __init__(self, options):
        super().__init__()
        self.options = options
        self.options.setdefault("update_frequency", 3600)
        self.options.setdefault("expiration", 86400)
        self._last_run = options.get("last_run", 0)

    async def reverify(self):
        candidates = []
        try:
            if (time.time() - self._last_run) > self.options["update_frequency"]:
                self.log.debug("running batch query %s", self._last_run)
                query = {
                    "kinds": [31494],
                    "#t": ["verification"],
                    "authors": [self.PUBLIC_KEY],
                    "until": int(time.time()) - (self.options["expiration"] - 300),
                }
                async for verify_record in self.manager.get_events(query):
                    pubkey = [t[1] for t in verify_record.tags if t[0] == "p"][0]
                    candidates.append(
                        [verify_record.content, verify_record.created_at, pubkey]
                    )

        except Exception:
            self.log.exception("batch_query")
            return

        if candidates:
            await self._process_candidates(candidates)
        self._last_run = time.time()

    async def verify_all_metadata(self):
        query = {"kinds": [0]}

        candidates = []
        async for event in self.manager.get_events(query):
            identifier = self.get_nip05_identifier(event)
            if identifier:
                candidates.append((identifier, 0, event.pubkey))
        if candidates:
            await self._process_candidates(candidates)

    async def _process_candidates(self, candidates):
        tasks = []
        self.log.info("Processing %d candidates", len(candidates))
        candidates.sort(key=lambda item: item[0].rsplit("@", 1)[1])
        async with get_aiohttp_session() as session:
            for identifier, verified_at, pubkey in candidates:
                self.log.info(
                    "Checking verification for %s. Last verified %s",
                    identifier,
                    verified_at,
                )
                tasks.append(
                    asyncio.create_task(self.verify_nip05(session, identifier, pubkey))
                )
            if tasks:
                self.log.info("Waiting for %d tasks", len(tasks))
                await asyncio.wait(tasks)

    async def verify_nip05(self, session, identifier, pubkey):
        uname, domain = identifier.split("@", 1)
        if not self.check_allowed_domains(domain):
            # how did this record get here?
            self.log.warning(
                "skipping verification for disallowed domain %s", identifier
            )
            return

        # request well-known url
        url = f"https://{domain}/.well-known/nostr.json?name={uname}"
        self.log.info("Requesting %s", url)
        success = False

        try:
            async with session.get(url) as response:
                # content_type=None will not check for the correct content-type
                # lots of nostr.json files seem to be served with wrong types
                data = await response.json(loads=json_loads, content_type=None)
            names = data["names"]
            assert isinstance(names, dict)
        except Exception as e:
            # self.log.exception("verify")
            # breakpoint()
            self.log.error("Failure verifying %s from %s %s", identifier, url, str(e))
        else:
            if names.get(uname, "") != pubkey:
                self.log.warning(
                    "Could not verify %s=%s from %s", identifier, pubkey, url
                )
            else:
                self.log.info("Verified %s=%s from %s", identifier, pubkey, url)
                success = True
        await self._mark_done(identifier, pubkey, success=success)

    async def _mark_done(self, identifier, pubkey, success=False):
        expiration = str(int(time.time()) + self.options["expiration"])
        tags = [
            ["t", "verification"],
            ["d", f"verify:{pubkey}"],
            ["n", identifier],
            ["expiration", expiration],
        ]
        if success:
            tags.append(["p", pubkey])
        else:
            tags.append(["failed", str(int(time.time()))])
            try:
                ALLOWED_PUBKEYS.remove(bytes.fromhex(pubkey))
            except KeyError:
                pass
        verify_record = self.make_event(kind=31494, content=identifier, tags=tags)
        await self.reply(verify_record)
        self.log.info(
            "Saved verification for %s=%s success=%s", identifier, pubkey, success
        )

    def check_allowed_domains(self, domain):
        if "/" in domain:
            return False
        if self.options.get("whitelist"):
            return domain in self.options["whitelist"]
        elif self.options.get("blacklist"):
            return domain not in self.options["blacklist"]
        return True

    def get_nip05_identifier(self, event):
        if event.kind != 0:
            return
        try:
            identifier = json_loads(event.content)["nip05"].lower().strip()
        except Exception:
            self.log.exception("bad metadata")
            return
        self.log.debug("Found identifier %s in event %s", identifier, event)
        if "@" in identifier:
            # queue this identifier as a candidate
            uname, domain = identifier.split("@", 1)
            if self.check_allowed_domains(domain) and re.fullmatch(
                "[a-z0-9\\._-]+", uname
            ):
                return identifier
            else:
                self.log.error("Illegal identifier %s", identifier)

    async def handle_event(self, event):
        identifier = self.get_nip05_identifier(event)
        if identifier:
            async with get_aiohttp_session() as session:
                await self.verify_nip05(session, identifier, event.pubkey)

            await self.reverify()
            return True


def is_nip05_verified(event, config):
    status = config.get("verification", {}).get("nip05_verification")
    if status not in ("enabled", "passive"):
        return True
    elif event.kind == 10002:
        # NIP-65 kinds are ok
        return True

    if event.kind == 0:
        if "nip05" not in event.content:
            if status == "enabled":
                raise VerificationError("rejected: metadata must have nip05 tag")
        else:
            if ALLOWED_PUBKEYS:
                ALLOWED_PUBKEYS.add(bytes.fromhex(event.pubkey))
            return True
