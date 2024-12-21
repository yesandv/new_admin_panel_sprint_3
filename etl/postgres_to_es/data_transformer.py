from pydantic import ValidationError

from etl.postgres_to_es.config.logger import logger
from etl.postgres_to_es.config.settings import etl_settings


class DataTransformer:

    @classmethod
    def transform(cls, data: list[dict], index: str) -> list[dict]:
        transformed_data = []
        for record in data:
            try:
                dto_class = etl_settings.dto_map[index]
                dto_instance = dto_class(**record)
                es_record = {
                    "_index": index,
                    "_id": dto_instance.id,
                    "_source": dto_instance.model_dump(),
                }
                transformed_data.append(es_record)
            except ValidationError:
                logger.exception(
                    "Validation error for an index '%s' record with the ID '%s'",
                    index,
                    record.get("id"),
                )
        return transformed_data
