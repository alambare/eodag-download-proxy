FROM python:3.14-slim

WORKDIR /app

# Install uv for fast dependency resolution
COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

# Copy and install the package
COPY eodag-control-plane/ ./eodag-control-plane/
RUN uv pip install --system --no-cache ./eodag-control-plane

RUN useradd --no-create-home --shell /bin/false eodag \
    && mkdir -p /app/.config/eodag \
    && chown eodag /app/.config/eodag

ENV EODAG_CFG_DIR=/app/.config/eodag

USER eodag

EXPOSE 8000

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
