FROM python:3.14-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY shared/ ./shared/
COPY db_init pyproject.toml ./

RUN uv sync --no-dev

CMD ["./.venv/bin/python", "main.py"]