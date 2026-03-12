FROM python:3.14
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY shared api pyproject.toml ./

RUN uv sync --group api

CMD ["./.venv/bin/python", "main.py"]