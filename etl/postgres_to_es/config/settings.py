from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

from etl.postgres_to_es.config import es_schema
from etl.postgres_to_es.models.models import (
    FilmWorkDto,
    GenreDto,
    PersonDto,
)

ROOT_DIR = Path(__file__).resolve().parent.parent.parent.parent


class Settings(BaseSettings):
    pg_db: str
    pg_user: str
    pg_password: str
    pg_host: str
    pg_port: int
    pg_schema: str

    es_url: str
    es_film_index: str
    es_genre_index: str
    es_person_index: str

    redis_host: str
    redis_port: int
    redis_key: str

    chunk_size: int = 100

    model_config = SettingsConfigDict(
        env_file=ROOT_DIR / ".env", extra="ignore"
    )

    @property
    def pg_dsl(self) -> dict:
        return {
            "dbname": self.pg_db,
            "user": self.pg_user,
            "password": self.pg_password,
            "host": self.pg_host,
            "port": self.pg_port,
        }

    @property
    def related_pg_tables(self) -> tuple:
        return "genre", "person"

    @property
    def es_indexes(self) -> list[str]:
        return [self.es_film_index, self.es_genre_index, self.es_person_index]

    @property
    def es_schema_map(self) -> dict:
        return {
            self.es_film_index: es_schema.filmworks,
            self.es_genre_index: es_schema.genres,
            self.es_person_index: es_schema.persons,
        }

    @property
    def dto_map(self) -> dict:
        return {
            self.es_film_index: FilmWorkDto,
            self.es_genre_index: GenreDto,
            self.es_person_index: PersonDto,
        }


etl_settings = Settings()
