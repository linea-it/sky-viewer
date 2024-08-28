# FROM mcr.microsoft.com/devcontainers/python:0-3.12
FROM mcr.microsoft.com/devcontainers/python:3.12-bookworm

ENV PYTHONUNBUFFERED 1

ARG BUILD_ENVIRONMENT=local

# [Optional] Uncomment this section to install additional OS packages.
RUN apt-get update && export DEBIAN_FRONTEND=noninteractive \
    && apt-get -y install --no-install-recommends \
    # dependencies for building Python packages
    build-essential \
    # psycopg dependencies
    libpq-dev \
    # python dev
    python3-dev \
    # Cleaning up
    && apt-get clean \
    && apt-get autoclean \
    && apt-get autoremove --purge  -y \
    && rm -rf /var/lib/apt/lists/*

# [Optional] If your requirements rarely change, uncomment this section to add them to the image.
COPY ./backend/requirements /tmp/pip-tmp/
RUN pip install --upgrade pip wheel setuptools \
    && pip --disable-pip-version-check --no-cache-dir install -r /tmp/pip-tmp/${BUILD_ENVIRONMENT}.txt \
    && rm -rf /tmp/pip-tmp

ENV DJANGO_SETTINGS_MODULE=config.settings.production
