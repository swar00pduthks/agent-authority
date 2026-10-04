# syntax=docker/dockerfile:1
# --- web UI -------------------------------------------------------------------
FROM node:22-alpine AS ui
WORKDIR /src/ui
COPY ui/package.json ui/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY ui/ ./
RUN npm run build            # writes to /src/backend/agenthub/static

# --- python ---------------------------------------------------------------------
FROM python:3.12-slim
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY backend/ ./backend/
COPY --from=ui /src/backend/agenthub/static ./backend/agenthub/static
RUN pip install --no-cache-dir "./backend[prod]" && useradd -r -u 10001 agenthub
USER agenthub
EXPOSE 8000
HEALTHCHECK --interval=15s --timeout=3s CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8000/api/hub/health')"
CMD ["agenthub", "serve"]
