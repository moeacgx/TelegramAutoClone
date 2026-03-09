FROM python:3.11-slim

WORKDIR /app

ARG BUILD_VERSION=dev

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    SELF_UPDATE_WORK_DIR=/app/data/self_update \
    SELF_UPDATE_EXECUTABLE_NAME=telegram-auto-clone

RUN apt-get update \
    && apt-get install -y --no-install-recommends gcc libc6-dev libssl-dev ffmpeg \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
RUN printf '%s\n' "$BUILD_VERSION" > /app/VERSION \
    && chmod +x /app/docker/entrypoint.sh

EXPOSE 8000

ENTRYPOINT ["/app/docker/entrypoint.sh"]