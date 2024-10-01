FROM python:3.12

WORKDIR /app

ENV POETRY_VIRTUALENVS_CREATE=false
ENV PYTHONPATH=/app

COPY requirements.txt requirements.txt
COPY pyproject.toml pyproject.toml

RUN pip install --upgrade pip \
    && pip install -r requirements.txt --no-cache-dir \
    && poetry install

COPY etl ./etl
