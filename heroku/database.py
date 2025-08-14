import redis
import asyncio
import collections
import json
import logging
import os
import re
import time
import typing
from urllib.parse import urlparse

from herokutl.errors.rpcerrorlist import ChannelsTooMuchError
from herokutl.tl.types import Message, User

from . import main, utils
from .pointers import (
    BaseSerializingMiddlewareDict,
    BaseSerializingMiddlewareList,
    NamedTupleMiddlewareDict,
    NamedTupleMiddlewareList,
    PointerDict,
    PointerList,
)
from .tl_cache import CustomTelegramClient
from .types import JSONSerializable

__all__ = [
    "Database",
    "PointerList",
    "PointerDict",
    "NamedTupleMiddlewareDict",
    "NamedTupleMiddlewareList",
    "BaseSerializingMiddlewareDict",
    "BaseSerializingMiddlewareList",
]

logger = logging.getLogger(__name__)


class NoAssetsChannel(Exception):
    """Raised when trying to read/store asset with no asset channel present"""


class Database(dict):
    def __init__(self, client: CustomTelegramClient):
        super().__init__()
        self._client: CustomTelegramClient = client
        self._next_revision_call: int = 0
        self._revisions: typing.List[dict] = []
        self._assets: int = None
        self._me: User = None
        self._redis: redis.Redis = None
        self._saving_task: asyncio.Future = None

    def __repr__(self):
        return object.__repr__(self)

    def _redis_save_sync(self):
        with self._redis.pipeline() as pipe:
            pipe.set(
                str(self._client.tg_id),
                json.dumps(self, ensure_ascii=True),
            )
            pipe.execute()

    async def remote_force_save(self) -> bool:
        if not self._redis:
            return False
        await utils.run_sync(self._redis_save_sync)
        logger.debug("Published db to Redis")
        return True

    async def _redis_save(self) -> bool:
        if not self._redis:
            return False
        await asyncio.sleep(5)
        await utils.run_sync(self._redis_save_sync)
        logger.debug("Published db to Redis")
        self._saving_task = None
        return True

    async def redis_init(self) -> bool:
        REDIS_URI = os.environ.get("REDIS_URL") or main.get_config_key("redis_uri")
        if not REDIS_URI:
            return False

        url = urlparse(REDIS_URI)

        # Убираем "default:" если это просто Railway placeholder
        if url.username == "default":
            REDIS_URI = REDIS_URI.replace("default:", "", 1)

        if url.scheme == "rediss":
            self._redis = redis.Redis.from_url(
                REDIS_URI,
                ssl=True,
                ssl_cert_reqs=None
            )
        else:
            self._redis = redis.Redis.from_url(REDIS_URI)

        try:
            self._redis.ping()
            logger.debug("Successfully connected to Redis")
        except redis.exceptions.AuthenticationError as e:
            logger.error(f"Redis auth failed: {e}")
            return False
        except redis.exceptions.ConnectionError as e:
            logger.error(f"Redis connection failed: {e}")
            return False

        return True

    async def init(self):
        if os.environ.get("REDIS_URL") or main.get_config_key("redis_uri"):
            await self.redis_init()

        self._db_file = main.BASE_PATH / f"config-{self._client.tg_id}.json"
        self.read()

        try:
            self._assets, _ = await utils.asset_channel(
                self._client,
                "heroku-assets",
                "🌆 Your Heroku assets will be stored here",
                archive=True,
                avatar="https://raw.githubusercontent.com/coddrago/assets/refs/heads/main/heroku/heroku_assets.png"
            )
        except ChannelsTooMuchError:
            self._assets = None
            logger.error(
                "Can't find and/or create assets folder\n"
                "This may cause several consequences, such as:\n"
                "- Non working assets feature (e.g. notes)\n"
                "- This error will occur every restart\n\n"
                "You can solve this by leaving some channels/groups"
            )

    def read(self):
        if self._redis:
            try:
                val = self._redis.get(str(self._client.tg_id))
                if val:
                    self.update(json.loads(val.decode()))
            except Exception:
                logger.exception("Error reading redis database")
            return

        try:
            db = self._db_file.read_text()
            if re.search(r'"(hikka\.)(\S+\":)', db):
                logging.warning("Converting db after update")
                db = re.sub(r'(hikka\.)(\S+\":)', lambda m: 'heroku.' + m.group(2), db)
            self.update(**json.loads(db))
        except json.decoder.JSONDecodeError:
            logger.warning("Database read failed! Creating new one...")
        except FileNotFoundError:
            logger.debug("Database file not found, creating new one...")

    def process_db_autofix(self, db: dict) -> bool:
        if not utils.is_serializable(db):
            return False

        for key, value in db.copy().items():
            if not isinstance(key, (str, int)):
                logger.warning("DbAutoFix: Dropped key %s, because type is %s", key, type(key))
                continue

            if not isinstance(value, dict):
                del db[key]
                logger.warning("DbAutoFix: Dropped key %s, because type is %s", key, type(value))
                continue

            for subkey in list(value):
                if not isinstance(subkey, (str, int)):
                    del db[key][subkey]
                    logger.warning("DbAutoFix: Dropped subkey %s of db key %s because type is %s", subkey, key, type(subkey))
        return True

    def save(self) -> bool:
        if not self.process_db_autofix(self):
            try:
                rev = self._revisions.pop()
                while not self.process_db_autofix(rev):
                    rev = self._revisions.pop()
            except IndexError:
                raise RuntimeError("Can't find revision to restore broken database from; db broken.")

            self.clear()
            self.update(**rev)

            raise RuntimeError("Rewriting database to the last known good revision.")

        if time.time() > self._next_revision_call:
            self._revisions.append(dict(self))
            self._next_revision_call = time.time() + 3

        while len(self._revisions) > 15:
            self._revisions.pop(0)

        if self._redis:
            if not self._saving_task:
                self._saving_task = asyncio.ensure_future(self._redis_save())
            return True

        try:
            self._db_file.write_text(json.dumps(self, indent=4))
        except Exception:
            logger.exception("Database save failed!")
            return False

        return True

    async def store_asset(self, message: Message) -> int:
        if not self._assets:
            raise NoAssetsChannel("Tried to save asset to non-existing asset channel")
        if isinstance(message, Message):
            return (await self._client.send_message(self._assets, message)).id
        return (await self._client.send_message(self._assets, file=message, force_document=True)).id

    async def fetch_asset(self, asset_id: int) -> typing.Optional[Message]:
        if not self._assets:
            raise NoAssetsChannel("Tried to fetch asset from non-existing asset channel")
        asset = await self._client.get_messages(self._assets, ids=[asset_id])
        return asset[0] if asset else None

    def get(self, owner: str, key: str, default: typing.Optional[JSONSerializable] = None) -> JSONSerializable:
        return self.get(owner, key, default)

    def set(self, owner: str, key: str, value: JSONSerializable) -> bool:
        if not utils.is_serializable(owner):
            raise RuntimeError(f"Non-serializable owner: {owner}")
        if not utils.is_serializable(key):
            raise RuntimeError(f"Non-serializable key: {key}")
        if not utils.is_serializable(value):
            raise RuntimeError(f"Non-serializable value: {value}")

        super().setdefault(owner, {})[key] = value
        return self.save()

    def pointer(
        self,
        owner: str,
        key: str,
        default: typing.Optional[JSONSerializable] = None,
        item_type: typing.Optional[typing.Any] = None,
    ) -> typing.Union[JSONSerializable, PointerList, PointerDict]:
        value = self.get(owner, key, default)
        mapping = {
            list: PointerList,
            dict: PointerDict,
            collections.abc.Hashable: lambda v: v,
        }

        pointer_constructor = next(
            (ptr for t, ptr in mapping.items() if isinstance(value, t)),
            None,
        )

        if (current_value := self.get(owner, key, None)) and type(current_value) is not type(default):
            raise ValueError("Pointer type mismatch in database")

        if pointer_constructor is None:
            raise ValueError(f"Pointer for type {type(value)} not implemented")

        if item_type is not None and isinstance(value, list):
            return NamedTupleMiddlewareList(pointer_constructor(self, owner, key, default), item_type)
        if item_type is not None and isinstance(value, dict):
            return NamedTupleMiddlewareDict(pointer_constructor(self, owner, key, default), item_type)

        return pointer_constructor(self, owner, key, default)