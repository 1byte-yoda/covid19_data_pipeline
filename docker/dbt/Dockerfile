FROM python:3.11-slim

RUN apt-get update

RUN apt-get install -y --no-install-recommends curl
RUN apt-get update
RUN apt-get install -y git g++ cmake ninja-build libssl-dev

RUN git clone https://github.com/duckdb/duckdb
WORKDIR /duckdb
RUN GEN=ninja make

ENV PATH "$PATH:/duckdb/build/release/"
RUN duckdb --version

RUN pip install uv
RUN uv pip install --system dbt-core==1.9.4 dbt-duckdb==1.9.3 fsspec s3fs

WORKDIR /transformer

ADD ./docker/dbt/entrypoint.sh /dbt/entrypoint.sh
RUN chmod +x /dbt/entrypoint.sh

CMD ["/dbt/entrypoint.sh"]