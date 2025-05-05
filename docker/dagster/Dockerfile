FROM python:3.11-slim

RUN apt-get update

RUN apt-get install -y --no-install-recommends curl \
    git \
    bash \
    unzip \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

RUN curl -O https://dl.min.io/client/mc/release/linux-amd64/mc && \
    chmod +x mc && mv mc /usr/local/bin/mc

COPY ./docker/dagster/init_assets.sh /init_assets.sh
COPY ./docker/dagster/create_deltalake.py /create_deltalake.py
COPY ./data/country_mapping.csv /data/country_mapping.csv

RUN chmod +x /init_assets.sh

RUN pip install uv

ADD ./requirements.txt .

RUN uv pip install --system -r requirements.txt

WORKDIR /dags

ADD ./docker/dagster/entrypoint.sh /docker/dagster/entrypoint.sh
RUN chmod +x /docker/dagster/entrypoint.sh

ENTRYPOINT ["/docker/dagster/entrypoint.sh"]
