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
import time
import rapidjson
from .config import Config


class VerificationError(Exception):
    pass


class Verifier:
    VERIFICATION_QUERY = """
        SELECT verification.id, identifier, verified_at, failed_at, created_at FROM verification
        LEFT JOIN event ON verification.metadata_id = event.id 
        WHERE event.pubkey = ? ORDER BY event.created_at DESC
    """
    FAILURE_QUERY = "UPDATE verification SET failed_at = strftime('%s', 'now') WHERE id = ?"
    SUCCESS_QUERY = "UPDATE verification SET verified_at = strftime('%s', 'now') WHERE id = ?"


    def __init__(self):
        self.running = True
        self.queue = asyncio.Queue()
        # nip05_verification can be "enabled", "disabled", or "passive"
        self.is_enabled = Config.nip05_verification == 'enabled'
        self.should_verify = Config.nip05_verification in ('enabled', 'passive')
        if self.should_verify:
            self.log = logging.getLogger(__name__)

    async def update_metadata(self, cursor, event):
        # metadata events are evaluated as candidates
        try:
            meta = rapidjson.loads(event.content)
        except Exception:
            self.log.exception("bad metadata")
        else:
            identifier = meta.get('nip05', '')
            self.log.debug("Found identifier %s in event %s", identifier, event)
            if '@' in identifier:
                # queue this identifier as a candidate
                domain = identifier.split('@', 1)[1].lower()
                if self.check_allowed_domains(domain):
                    await self.queue.put([None, identifier, 0, event.pubkey, event.id_bytes])
                    return True
                else:
                    self.log.error("Illegal domain in identifier %s", identifier)
        return False

    def check_allowed_domains(self, domain):
        if Config.verification_whitelist:
            return domain in Config.verification_whitelist
        elif Config.verification_blacklist:
            return domain not in Config.verification_blacklist
        elif '/' in domain:
            return False
        return True

    async def verify(self, cursor, event):
        """
        Check an event against the NIP-05
        verification table
        """
        if Config.nip05_verification == 'disabled':
            return True

        
        if event.kind == 0:
            is_candidate = await self.update_metadata(cursor, event)
            if not is_candidate:
                if self.is_enabled:
                    raise VerificationError("rejected: metadata must have nip05 tag")
                else:
                    self.log.warning("Attempt to save metadata event %s from %s without nip05 tag", event.id, event.pubkey)
            else:
                return True

        await cursor.execute(self.VERIFICATION_QUERY, (event.pubkey, ))
        row = await cursor.fetchone()
        if not row:
            if self.is_enabled:
                raise VerificationError(f"rejected: pubkey {event.pubkey} must be verified")
            else:
                self.log.warning('pubkey %s is not verified.', event.pubkey)
        else:
            vid, identifier, verified_at, failed_at, created_at = row
            self.log.debug("Checking verification for %s verified:%s created:%s", identifier, verified_at, created_at)
            now = time.time()
            if ((verified_at or 0) + Config.verification_expiration) < now:
                # verification has expired
                if self.is_enabled:
                    raise VerificationError(f"rejected: verification expired for {identifier}")
                else:
                    self.log.warning("verification expired for %s on %s", identifier, verified_at)

            uname, domain = identifier.split('@', 1)
            domain = domain.lower()
            if not self.check_allowed_domains(domain):
                if self.is_enabled:
                    raise VerificationError(f"rejected: {domain} not allowed")
                else:
                    self.log.warning("verification for %s not allowed", identifier)
        self.log.debug("Verified %s", event.pubkey)
        return True

    async def verification_task(self, db):
        self.log.info("Starting verification task. Interval %s", Config.verification_update_frequency)
        last_run = 0
        query = f"""
            SELECT v.id, identifier, verified_at, pubkey, metadata_id FROM verification as v
            LEFT JOIN event ON v.metadata_id = event.id
            WHERE pubkey IS NOT NULL AND
            (? - verified_at > {Config.verification_expiration})
            ORDER BY verified_at DESC
        """
        while self.running:
            candidate = await self.queue.get()
            if candidate is None:
                break
            self.log.debug("Got candidate %s", candidate)
            candidates = []
            try:
                if (time.time() - last_run) > Config.verification_update_frequency:
                    self.log.debug("running batch query")
                    async with db.cursor() as cursor:
                        try:
                            await cursor.execute(query, (int(time.time()), ))
                        except Exception:
                            self.log.exception('batch query')
                            continue
                        async for row in cursor:
                            candidates.append(row)
                            # vid, identifier, verified_at, pubkey = row
            except Exception:
                self.log.exception("batch_query")
                continue
            candidates.append(candidate)

            try:
                success, failure = await self.process_verifications(candidates)
            except Exception:
                self.log.exception("process_verifications")
            else:
                if success or failure:
                    async with db.cursor() as cursor:
                        for vid, identifier, metadata_id in success:
                            if vid is None:
                                # first time verifying
                                await cursor.execute("INSERT INTO verification (identifier, metadata_id, verified_at) VALUES (?, ?, strftime('%s', 'now'))", (identifier, metadata_id, ))
                            else:
                                await cursor.execute(self.SUCCESS_QUERY, (vid, ))
                        for vid, identifier, metadata_id in failure:
                            if vid is None:
                                # don't persist first time candidates
                                continue
                            else:
                                await cursor.execute(self.FAILURE_QUERY, (vid, ))
                        await db.commit()
                    self.log.info("Saved success:%d failure:%d", len(success), len(failure))
            last_run = time.time()

        self.log.info("Stopped verification task")

    async def process_verifications(self, candidates):
        success = []
        failure = []
        import aiohttp
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=10.0), json_serialize=rapidjson.dumps) as session:

            for vid, identifier, verified_at, pubkey, metadata_id in candidates:
                self.log.info("Checking verification for %s. Last verified %d", identifier, verified_at)
                uname, domain = identifier.split('@', 1)
                domain = domain.lower()
                if not self.check_allowed_domains(domain):
                    # how did this record get here?
                    self.log.warning("skipping verification for disallowed domain %s", identifier)
                    continue
                # request well-known url
                url = f'https://{domain}/.well-known/nostr.json?name={uname}'
                self.log.info("Requesting %s", url)

                try:
                    async with session.get(url) as response:
                        data = await response.json(loads=rapidjson.loads)
                    names = data['names']
                except Exception:
                    self.log.exception("Failure verifying %s from %s", identifier, url)
                    failure.append([vid, identifier, metadata_id])
                else:
                    if names.get(uname, '') != pubkey:
                        self.log.warning("Could not verify %s=%s from %s", identifier, pubkey, url)
                        failure.append([vid,  identifier, metadata_id])
                    else:
                        self.log.info("Verified %s=%s from %s", identifier, pubkey, url)
                        success.append([vid, identifier, metadata_id])

        return success, failure

    async def start(self, db):
        if self.should_verify:
            asyncio.create_task(self.verification_task(db))

    async def stop(self):
        if self.should_verify:
            self.running = False
            await self.queue.put(None)


