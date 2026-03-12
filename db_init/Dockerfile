FROM python:3.14
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY shared db_init pyproject.toml ./

RUN uv sync

CMD ["./.venv/bin/python", "main.py"]