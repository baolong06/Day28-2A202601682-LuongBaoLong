FROM mcr.microsoft.com/devcontainers/python:1-3.11-bookworm

# Remove broken Yarn apt repo (expired GPG key) so feature installs that run
# `apt-get update` (e.g. docker-in-docker) do not fail on this image.
RUN rm -f /etc/apt/sources.list.d/yarn.list \
    && apt-get clean

RUN python -m pip install --no-cache-dir uv==0.12.9 \
 && uv --version
