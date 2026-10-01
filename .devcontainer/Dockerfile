FROM mcr.microsoft.com/devcontainers/java:1-17-bookworm

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client \
    && rm -rf /var/lib/apt/lists/*

USER vscode
