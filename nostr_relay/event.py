"""
forked from https://github.com/jeffthibault/python-nostr.git
"""
import time
from enum import IntEnum
from secp256k1 import PrivateKey, PublicKey
from hashlib import sha256

try:
    import rapidjson as json
except ImportError:
    import json


class EventKind(IntEnum):
    SET_METADATA = 0
    TEXT_NOTE = 1
    RECOMMEND_RELAY = 2
    CONTACTS = 3
    ENCRYPTED_DIRECT_MESSAGE = 4
    DELETE = 5

class Event:
    def __init__(
            self,
            pubkey: str='', 
            content: str='', 
            created_at: int=int(time.time()), 
            kind: int=EventKind.TEXT_NOTE, 
            tags: "list[list[str]]"=[], 
            id: str=None,
            sig: str=None) -> None:
        if not isinstance(content, str):
            raise TypeError("Argument 'content' must be of type str")
        
        if not id:
            id = Event.compute_id(pubkey, created_at, kind, tags, content)
        self.id = id
        self.pubkey = pubkey
        self.content = content
        self.created_at = created_at
        self.kind = kind
        self.tags = tags
        self.sig = sig

    @property
    def is_ephemeral(self):
        return self.kind >= 20000 and self.kind < 30000

    @property
    def is_replaceable(self):
        return self.kind >= 10000 and self.kind < 20000

    @staticmethod
    def serialize(public_key: str, created_at: int, kind: int, tags: "list[list[str]]", content: str) -> bytes:
        data = [0, public_key, created_at, kind, tags, content]
        data_str = json.dumps(data, ensure_ascii=False)
        return data_str.encode()

    @staticmethod
    def compute_id(public_key: str, created_at: int, kind: int, tags: "list[list[str]]", content: str) -> str:
        return sha256(Event.serialize(public_key, created_at, kind, tags, content)).hexdigest()

    @staticmethod
    def from_tuple(row):
        return Event(
            pubkey=row[1],
            content=row[5],
            created_at=row[2],
            kind=row[3],
            tags=json.loads(row[4]),
            sig=row[6]
        )

    def sign(self, private_key_hex: str) -> None:
        sk = PrivateKey(bytes.fromhex(private_key_hex))
        sig = sk.schnorr_sign(bytes.fromhex(self.id), None, raw=True)
        self.signature = sig.hex()

    def verify(self) -> bool:
        pub_key = PublicKey(bytes.fromhex("02" + self.pubkey), True) # add 02 for schnorr (bip340)
        event_id = Event.compute_id(self.pubkey, self.created_at, self.kind, self.tags, self.content)
        return pub_key.schnorr_verify(bytes.fromhex(event_id), bytes.fromhex(self.sig), None, raw=True)

    def to_tuple(self):
        return (
            self.id,
            self.pubkey,
            self.created_at,
            self.kind,
            json.dumps(self.tags),
            self.content,
            self.sig
        )

    def __str__(self):
        return json.dumps(self.to_json_object())

    def to_json_object(self) -> dict:
        return {
            "id": self.id,
            "pubkey": self.pubkey,
            "created_at": self.created_at,
            "kind": self.kind,
            "tags": self.tags,
            "content": self.content,
            "sig": self.sig
        }
