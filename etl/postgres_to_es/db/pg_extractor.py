from datetime import datetime

from psycopg.rows import Row

from etl.postgres_to_es.db.base import DataExtractor, BaseConnector
from etl.postgres_to_es.queries.sql_queries import (
    GET_MODIFIED_RECORDS,
    GET_RELATED_TO_FILM_WORK_IDS,
    GET_FILM_WORK_DATA,
    GET_GENRE_DATA,
    GET_PERSON_DATA,
)


def initialize_extractors(
        db_connector: BaseConnector, schema: str
) -> tuple:
    return (
        FilmWorkExtractor(db_connector, schema),
        GenreExtractor(db_connector, schema),
        PersonExtractor(db_connector, schema),
    )


class FilmWorkExtractor(DataExtractor):
    def __init__(self, db_connector: BaseConnector, schema: str):
        self.db_connector = db_connector
        self.schema = schema

    def get_modified_records(self, last_modified: datetime) -> list[str]:
        query = GET_MODIFIED_RECORDS.format(table=f"{self.schema}.film_work")
        records = self.db_connector.fetch_data(query, (last_modified,))
        if records:
            return [str(record["id"]) for record in records]
        return []

    def get_related_to_film_work_ids(
            self,
            table: str,
            modify_record_ids: list[str],
    ) -> list[str]:
        alias = f"{'g' if table == 'genre' else 'p'}fw"
        query = GET_RELATED_TO_FILM_WORK_IDS.format(
            alias=alias, schema=self.schema, table=table
        )
        records = self.db_connector.fetch_data(query, (modify_record_ids,))
        if records:
            return [str(record["film_work_id"]) for record in records]
        return []

    def get_data(self, record_ids: list[str]) -> list[Row]:
        return self.db_connector.fetch_data(GET_FILM_WORK_DATA, (record_ids,))


class GenreExtractor(DataExtractor):
    def __init__(self, db_connector: BaseConnector, schema: str):
        self.db_connector = db_connector
        self.schema = schema

    def get_modified_records(self, last_modified: datetime) -> list[str]:
        query = GET_MODIFIED_RECORDS.format(table=f"{self.schema}.genre")
        records = self.db_connector.fetch_data(query, (last_modified,))
        if records:
            return [str(record["id"]) for record in records]
        return []

    def get_data(self, record_ids: list[str]) -> list[Row]:
        return self.db_connector.fetch_data(GET_GENRE_DATA, (record_ids,))


class PersonExtractor(DataExtractor):
    def __init__(self, db_connector: BaseConnector, schema: str):
        self.db_connector = db_connector
        self.schema = schema

    def get_modified_records(self, last_modified: datetime) -> list[str]:
        query = GET_MODIFIED_RECORDS.format(table=f"{self.schema}.person")
        records = self.db_connector.fetch_data(query, (last_modified,))
        if records:
            return [str(record["id"]) for record in records]
        return []

    def get_data(self, record_ids: list[str]) -> list[Row]:
        return self.db_connector.fetch_data(GET_PERSON_DATA, (record_ids,))
