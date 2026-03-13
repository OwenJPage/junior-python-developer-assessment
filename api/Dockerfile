FROM python:3.14-slim
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY shared/ ./shared/
COPY api/ pyproject.toml ./

RUN uv sync --group api --no-dev

CMD ["./.venv/bin/fastapi", "run", "main.py"]