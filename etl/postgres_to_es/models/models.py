from uuid import UUID

from pydantic import BaseModel, Field, field_validator


class PersonDto(BaseModel):
    id: str
    name: str = Field(None, alias="full_name")


class FilmWorkDto(BaseModel):
    id: str
    imdb_rating: float | None = Field(0.0, alias="rating")
    genres: list[str] = []
    title: str | None = None
    description: str | None = None
    directors_names: list[str] | None = []
    actors_names: list[str] | None = []
    writers_names: list[str] | None = []
    directors: list[PersonDto] = []
    actors: list[PersonDto] = []
    writers: list[PersonDto] = []

    @field_validator("id", mode="before")  # noqa
    @classmethod
    def turn_uuid_into_str(cls, value):
        if isinstance(value, UUID):
            return str(value)
        return value
