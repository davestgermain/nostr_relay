"""
This handles pubkey verification according to NIP-05

if Config.nip05_verification is set to 'enabled',
the verification table will be consulted for every event addition.
If set to 'passive', the table will be consulted, but failures will only be logged.

When a kind=0 (metadata) event is saved, it will be considered a candidate
for verification if it contains a nip05 tag.

Every Config.verification_update_frequency, the verifications will be reprocessed.
"""
import asyncio
import logging
import re
import time

from datetime import datetime, timedelta

from .errors import VerificationError
from .util import Periodic, timeout, json


class Verifier(Periodic):
    def __init__(self, storage, options: dict = None):
        self.storage = storage
        self.options = options or {}
        self.options.setdefault("update_frequency", 3600)
        self.options.setdefault("expiration", 86400)
        Periodic.__init__(self, self.options["update_frequency"])
        self._candidates = []
        self._last_run = 0
        self.queue = asyncio.Queue()
        # nip05_verification can be "enabled", "disabled", or "passive"
        self.is_enabled = options.get("nip05_verification", "") == "enabled"
        self.should_verify = options.get("nip05_verification", "") in (
            "enabled",
            "passive",
        )
        if self.should_verify:
            self.log = logging.getLogger(__name__)

    async def update_metadata(self, event):
        # metadata events are evaluated as candidates
        try:
            meta = json.loads(event.content)
        except Exception:
            self.log.exception("bad metadata")
        else:
            identifier = meta.get("nip05", "").lower().strip()
            self.log.debug("Found identifier %s in event %s", identifier, event)
            if "@" in identifier:
                # queue this identifier as a candidate
                uname, domain = identifier.split("@", 1)
                if self.check_allowed_domains(domain) and re.fullmatch(
                    "[a-z0-9\\._-]+", uname
                ):
                    await self.queue.put([identifier, 0, event.pubkey])
                    return True
                else:
                    self.log.error("Illegal identifier %s", identifier)
        return False

    def check_allowed_domains(self, domain):
        if "/" in domain:
            return False
        if self.options.get("whitelist"):
            return domain in self.options["whitelist"]
        elif self.options.get("blacklist"):
            return domain not in self.options["blacklist"]
        return True

    async def verify(self, event):
        """
        Check an event against the NIP-05
        verification table
        """
        if not self.should_verify:
            return True
        elif event.pubkey == self.storage.service_pubkey:
            return True

        if event.kind == 0:
            is_candidate = await self.update_metadata(event)
            if not is_candidate:
                if self.is_enabled:
                    raise VerificationError("rejected: metadata must have nip05 tag")
                else:
                    self.log.warning(
                        "Attempt to save metadata event %s from %s without nip05 tag",
                        event.id,
                        event.pubkey,
                    )
            else:
                return True

        query = {
            "kinds": [self.storage.service_kind],
            "#d": [f"verify:{event.pubkey}"],
            "authors": [self.storage.service_pubkey],
        }
        verify_record = await self.storage.get_event_from_query(query)

        if not verify_record:
            if self.is_enabled:
                raise VerificationError(
                    f"rejected: pubkey {event.pubkey} must be verified"
                )
            else:
                self.log.warning("pubkey %s is not verified.", event.pubkey)
        elif "failed" not in [tag[0] for tag in verify_record.tags]:
            identifier = verify_record.content
            self.log.debug(
                "Checking verification for %s verified:%s",
                identifier,
                verify_record.created_at,
            )

            uname, domain = identifier.split("@", 1)
            domain = domain.lower()
            if not self.check_allowed_domains(domain):
                if self.is_enabled:
                    raise VerificationError(f"rejected: {domain} not allowed")
                else:
                    self.log.warning("verification for %s not allowed", identifier)
        self.log.debug("Verified %s", event.pubkey)
        return True

    async def run_once(self):
        candidates = []
        try:
            if (time.time() - self._last_run) > self.options["update_frequency"]:
                self.log.debug("running batch query %s", self._last_run)
                query = {
                    "kinds": [self.storage.service_kind],
                    "#t": ["verification"],
                    "authors": [self.storage.service_pubkey],
                    "until": int(time.time()) - (self.options["expiration"] - 300),
                }
                async for verify_record in self.storage.run_single_query([query]):
                    pubkey = [t[1] for t in verify_record.tags if t[0] == "p"][0]
                    candidates.append(
                        [verify_record.content, verify_record.created_at, pubkey]
                    )

        except Exception:
            self.log.exception("batch_query")
            return
        if self._candidates:
            candidates.extend(self._candidates)

        try:
            await self.process_verifications(candidates)
        except Exception:
            self.log.exception("process_verifications")
        self._last_run = time.time()

    async def _mark_done(self, identifier, pubkey, success=False):
        expiration = str(int(time.time()) + self.options["expiration"])
        tags = {
            "t": "verification",
            "d": f"verify:{pubkey}",
            "expiration": expiration,
        }
        if not success:
            tags["failed"] = str(int(time.time()))
        verify_record = await self.storage.add_service_event(
            content=identifier, tags=tags
        )
        self.log.info(
            "Saved verification for %s=%s success=%s", identifier, pubkey, success
        )

    def get_aiohttp_session(self):
        import aiohttp

        return aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=10.0), json_serialize=json.dumps
        )

    async def process_verifications(self, candidates):
        if not candidates:
            return
        tasks = []
        async with self.get_aiohttp_session() as session:
            for identifier, verified_at, pubkey in candidates:
                self.log.info(
                    "Checking verification for %s. Last verified %s",
                    identifier,
                    verified_at,
                )
                tasks.append(
                    asyncio.create_task(
                        self._fetch_one_verification(session, identifier, pubkey)
                    )
                )
            if tasks:
                self.log.info("Waiting for %d tasks", len(tasks))
                await asyncio.wait(tasks)

    async def _fetch_one_verification(self, session, identifier, pubkey):
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
                data = await response.json(loads=json.loads, content_type=None)
            names = data["names"]
            assert isinstance(names, dict)
        except Exception as e:
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

    async def wait_function(self):
        self._candidates = []
        flag = True
        try:
            async with timeout(self.options["update_frequency"]):
                while flag or self.queue.qsize():
                    candidate = await self.queue.get()
                    if candidate is None:
                        self.running = False
                        return
                    else:
                        self._candidates.append(candidate)
                        flag = False
            self.log.debug("Got candidates %s", self._candidates)
        except asyncio.TimeoutError:
            self.log.debug("timed out waiting for queue")

    async def start(self, db=None):
        if self.should_verify:
            self.log.info(
                "Starting verification task. Interval %s",
                self.options["update_frequency"],
            )
            await super().start()

    def is_processing(self):
        return not (self.queue.empty() and self._candidates)
