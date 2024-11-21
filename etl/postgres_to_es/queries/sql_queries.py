GET_MODIFIED_RECORDS = """
    SELECT id
    FROM {table}
    WHERE modified > %s
"""

GET_RELATED_TO_FILM_WORK_IDS = """
    SELECT DISTINCT {alias}.film_work_id
    FROM {schema}.{table}_film_work {alias}
    WHERE {alias}.{table}_id = ANY(%s)
"""

GET_FILM_WORK_DATA = """
    SELECT
        fw.id,
        fw.rating,
        fw.title,
        fw.description,

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

    GROUP BY fw.id
"""

GET_GENRE_DATA = """
    SELECT
        id, name
    FROM content.genre
    WHERE id = ANY(%s)
"""


GET_PERSON_DATA = """
    SELECT
        id, full_name
    FROM content.person
    WHERE id = ANY(%s)
"""
