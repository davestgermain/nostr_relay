"""
Validator functions to check events before adding them to the database.

To configure and use your own, see https://code.pobblelabs.org/fossil/nostr_relay/doc/tip/docs/storage.md
"""
import asyncio
from time import time

from nostr_relay.errors import StorageError
from nostr_relay.util import object_from_path


def is_not_too_large(event, config):
    """
    Ensure the event is not larger than Config.max_event_size
    """
    if len(event.content) > config.max_event_size:
        raise StorageError("invalid: 280 characters should be enough for anybody")


def is_signed(event, config):
    """
    Ensure the event is correctly formatted and signed
    """
    if not event.verify():
        raise StorageError("invalid: Bad signature")


def is_recent(event, config):
    """
    Ensure the event is not too old, based on Config.oldest_event
    or in the future
    """
    if (time() - event.created_at) > config.oldest_event:
        raise StorageError(f"invalid: {event.created_at} is too old")
    elif (time() - event.created_at) < -3600:
        raise StorageError(f"invalid: {event.created_at} is in the future")


def is_certain_kind(event, config):
    """
    Ensure the event kind is in Config.valid_kinds
    """
    if event.kind not in config.valid_kinds:
        raise StorageError(f"invalid: kind={event.kind} not allowed")


def is_author_whitelisted(event, config):
    """
    Ensure the event pubkey is in Config.pubkey_whitelist
    """
    if event.pubkey not in config.pubkey_whitelist:
        raise StorageError(f"rejected: {event.pubkey} not allowed")


def is_author_blacklisted(event, config):
    """
    Ensure the event pubkey is not in Config.pubkey_blacklist
    """
    if event.pubkey in config.pubkey_blacklist:
        raise StorageError(f"rejected: {event.pubkey} not allowed")


def is_pow(event, config):
    """
    Ensure the event has a certain number of leading zero bits, set by Config.require_pow
    """
    found_bits = 256 - int.from_bytes(event.id_bytes, "big").bit_length()
    if found_bits < config.require_pow:
        raise StorageError(
            f"rejected: {config.require_pow} PoW required. Found: {found_bits}"
        )


def get_validator(function_names):
    """
    Given a list of function names, return an async validator function to run them all
    """
    validators = [object_from_path(funcname) for funcname in function_names]

    async def validate(event, config):
        def _inner_validator(event, config):
            for func in validators:
                func(event, config)

        await asyncio.get_running_loop().run_in_executor(
            None, _inner_validator, event, config
        )

    return validate
