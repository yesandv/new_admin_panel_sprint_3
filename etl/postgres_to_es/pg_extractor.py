import logging
from datetime import datetime

import backoff
import psycopg
from psycopg import InterfaceError
from psycopg.rows import Row
from pydantic import ValidationError

from etl.postgres_to_es.config.settings import (
    PG_SCHEMA,
    RELATED_PG_TABLES,
    ES_INDEX,
)
from etl.postgres_to_es.models.models import FilmWorkDto


class PostgresExtractor:

    def __init__(self, dsl: dict):
        self.config = dsl
        self.connection = self.connect()

    @backoff.on_exception(
        backoff.expo,
        psycopg.OperationalError,
        max_tries=5,
        jitter=backoff.full_jitter,
    )
    def connect(self):
        return psycopg.connect(**self.config)

    def _fetch_data(self, query: str, params: tuple) -> list[Row]:
        cursor = self.connection.cursor()
        try:
            cursor.execute(query, params)
            return cursor.fetchall()
        finally:
            cursor.close()

    def get_modified_records(
            self, table: str, last_modified: datetime
    ) -> tuple[list[str], datetime]:
        query = f"""
            SELECT id, modified
            FROM {table}
            WHERE modified > %s
        """
        records = self._fetch_data(query, (last_modified,))
        if records:
            return (
                [str(record[0]) for record in records],
                max(record[1] for record in records),
            )
        return [], last_modified

    def get_related_to_film_work_ids(
            self,
            table: str,
            modify_record_ids: list[str],
    ) -> list[str]:
        alias = f"{'g' if table == 'genre' else 'p'}fw"
        query = f"""
            SELECT DISTINCT {alias}.film_work_id
            FROM {PG_SCHEMA}.{table}_film_work {alias}
            WHERE {alias}.{table}_id = ANY(%s)
        """
        film_ids = self._fetch_data(query, (modify_record_ids,))
        return [str(film_id[0]) for film_id in film_ids]

    def get_film_work_data(self, film_ids: list[str]) -> list[dict]:
        connection = self.connect()
        cursor = connection.cursor()

        query = """
                SELECT 
                    fw.id,
                    fw.rating,
                    fw.title,
                    fw.description,
                    fw.modified,

                    COALESCE(array_agg(DISTINCT g.name), ARRAY[]::text[]) AS genres,

                    COALESCE(jsonb_agg(DISTINCT jsonb_build_object('id', p.id, 'full_name', p.full_name))
                             FILTER (WHERE pfw.role = 'actor'), '[]'::jsonb) AS actors,
                    COALESCE(jsonb_agg(DISTINCT jsonb_build_object('id', p.id, 'full_name', p.full_name))
                             FILTER (WHERE pfw.role = 'director'), '[]'::jsonb) AS directors,
                    COALESCE(jsonb_agg(DISTINCT jsonb_build_object('id', p.id, 'full_name', p.full_name))
                             FILTER (WHERE pfw.role = 'writer'), '[]'::jsonb) AS writers,

                    COALESCE(array_agg(DISTINCT p.full_name)
                             FILTER (WHERE pfw.role = 'actor'), ARRAY[]::text[]) AS actors_names,
                    COALESCE(array_agg(DISTINCT p.full_name)
                             FILTER (WHERE pfw.role = 'director'), ARRAY[]::text[]) AS directors_names,
                    COALESCE(array_agg(DISTINCT p.full_name)
                             FILTER (WHERE pfw.role = 'writer'), ARRAY[]::text[]) AS writers_names

                FROM content.film_work fw

                LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
                LEFT JOIN content.genre g ON g.id = gfw.genre_id
                LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
                LEFT JOIN content.person p ON p.id = pfw.person_id

                WHERE fw.id = ANY(%s)

                GROUP BY fw.id;
                """

        cursor.execute(query, (film_ids,))
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        film_data = [dict(zip(columns, row)) for row in rows]
        cursor.close()
        connection.close()
        return film_data

    def extract_data(
            self, last_modified: datetime
    ) -> tuple[list[dict], datetime]:
        latest_modified_timestamps = []
        film_work_ids = set()
        try:
            modified_record_ids, _latest_modified = self.get_modified_records(
                f"{PG_SCHEMA}.film_work", last_modified
            )
            film_work_ids.update(modified_record_ids)
            latest_modified_timestamps.append(_latest_modified)
            for table in RELATED_PG_TABLES:
                modified_record_ids, _latest_modified = (
                    self.get_modified_records(
                        f"{PG_SCHEMA}.{table}", last_modified
                    )
                )
                film_work_ids.update(
                    self.get_related_to_film_work_ids(
                        table, modified_record_ids
                    )
                )
                latest_modified_timestamps.append(_latest_modified)
            data = self.get_film_work_data(list(film_work_ids))
            return data, max(latest_modified_timestamps)
        except InterfaceError:
            logging.exception(
                "Error occurred while extracting data from the PG DB"
            )

    @staticmethod
    def transform_data(data: list[dict]) -> list[dict]:
        transformed_data = []
        for record in data:
            try:
                film_work = FilmWorkDto(**record)
                es_record = {
                    "_index": ES_INDEX,
                    "_id": film_work.id,
                    "_source": film_work.model_dump(),
                }
                transformed_data.append(es_record)
            except ValidationError:
                logging.exception(
                    "Validation error for a record with the ID '%s'",
                    record.get("id"),
                )
        return transformed_data
