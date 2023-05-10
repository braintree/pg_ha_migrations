ARG PGVERSION

FROM postgres:$PGVERSION-bullseye

RUN apt-get update && apt-get install -y curl ca-certificates gnupg lsb-release

RUN curl https://www.postgresql.org/media/keys/ACCC4CF8.asc | gpg --dearmor | tee /etc/apt/trusted.gpg.d/apt.postgresql.org.gpg >/dev/null

RUN echo "deb http://apt.postgresql.org/pub/repos/apt $(lsb_release -cs)-pgdg main" > /etc/apt/sources.list.d/pgdg.list

RUN apt update && apt-get install -y postgresql-$PG_MAJOR-partman
