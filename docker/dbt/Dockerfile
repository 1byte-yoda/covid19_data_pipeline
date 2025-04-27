FROM python:3.11-slim

RUN apt-get update

RUN apt-get install -y --no-install-recommends curl

RUN pip install uv

RUN uv pip install --system dbt-core==1.9.4 dbt-duckdb==1.9.3 fsspec s3fs

WORKDIR /covid19_dbt

ADD ./docker/dbt/entrypoint.sh /dbt/entrypoint.sh
RUN chmod +x /dbt/entrypoint.sh

CMD ["/dbt/entrypoint.sh"]