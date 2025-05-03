FROM python:3.11-slim

RUN apt-get update

RUN apt-get install -y --no-install-recommends curl

RUN pip install uv

ADD ./requirements.txt .

RUN uv pip install --system -r requirements.txt

WORKDIR /dags

ADD ./docker/dagster/entrypoint.sh /docker/dagster/entrypoint.sh
RUN chmod +x /docker/dagster/entrypoint.sh

ENTRYPOINT ["/docker/dagster/entrypoint.sh"]
