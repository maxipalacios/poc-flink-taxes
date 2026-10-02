FROM mcr.microsoft.com/devcontainers/java:1-17-bookworm

USER root

# The base image carries a yarn apt repository whose signing key was rotated
# upstream; it is unrelated to this project and breaks apt-get update.
RUN rm -f /etc/apt/sources.list.d/yarn.list \
    && apt-get update \
    && apt-get install -y --no-install-recommends postgresql-client unzip \
    && rm -rf /var/lib/apt/lists/*

# Install a pinned Gradle so plain docker-compose workflows can build the job.
ARG GRADLE_VERSION=8.14.3
RUN curl -fsSL "https://services.gradle.org/distributions/gradle-${GRADLE_VERSION}-bin.zip" -o /tmp/gradle.zip \
    && unzip -q /tmp/gradle.zip -d /opt \
    && ln -s "/opt/gradle-${GRADLE_VERSION}/bin/gradle" /usr/local/bin/gradle \
    && rm /tmp/gradle.zip

# Pre-create the Gradle home so the `gradle-cache` volume in docker-compose.yml
# mounts with vscode ownership instead of root, keeping dependency downloads
# cached across one-off `docker compose run` containers.
RUN mkdir -p /home/vscode/.gradle && chown vscode:vscode /home/vscode/.gradle

USER vscode
