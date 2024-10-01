import logging
from datetime import datetime

from redis import Redis, exceptions

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RedisStorage:

    def __init__(self, redis_client: Redis, state_key: str):
        self.redis_client = redis_client
        self.state_key = state_key

    def save_state(self, modified: datetime):
        self.redis_client.set(self.state_key, modified.isoformat())

    def retrieve_state(self) -> datetime:
        try:
            last_modified = self.redis_client.get(self.state_key)
            if last_modified:
                return datetime.fromisoformat(last_modified.decode("utf-8"))  # noqa
            return datetime.fromisoformat("1970-01-01T00:00:00+00:00")
        except exceptions.ConnectionError as ex:
            logging.exception("Check connection to Redis")
            raise ex
