FROM ghcr.io/astral-sh/uv:bookworm-slim AS base

FROM base AS app-base
WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen
ENV PATH="/app/.venv/bin:${PATH}"
COPY . .

FROM app-base AS production
ENTRYPOINT ["uv", "run", "csv-example.py"]

FROM app-base AS development

RUN apt-get update --allow-insecure-repositories && apt-get install -y \
    bash \
    && rm -rf /var/lib/apt/lists/*
