from datetime import datetime, timezone

from redis import Redis, exceptions

from etl.postgres_to_es.config.logger import logger


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
                return datetime.fromisoformat(
                    last_modified.decode("utf-8")  # noqa
                )
            return datetime.min.replace(tzinfo=timezone.utc)
        except exceptions.ConnectionError as ex:
            logger.exception("Check connection to Redis")
            raise ex
