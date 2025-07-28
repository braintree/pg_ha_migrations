ARG PGVERSION
ARG PARTMAN_VERSION

FROM postgres:$PGVERSION-bookworm AS base

RUN apt-get update && apt-get install -y curl ca-certificates gnupg lsb-release

RUN curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor | tee /etc/apt/trusted.gpg.d/apt.postgresql.org.gpg >/dev/null

RUN echo "deb https://apt-archive.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg-archive main" > /etc/apt/sources.list.d/pgdg.list

FROM base AS partman-4-branch
ENV PARTMAN_VERSION=4.7.4-2.pgdg120+1

FROM base AS partman-5-branch
ENV PARTMAN_VERSION=5.2.4-1.pgdg120+1

FROM partman-$PARTMAN_VERSION-branch AS final
RUN apt update && apt-get install -y postgresql-$PG_MAJOR-partman=$PARTMAN_VERSION
