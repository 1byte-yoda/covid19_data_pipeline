FROM apache/superset:4.1.1

ARG SUPERSET_ADMIN
ARG SUPERSET_PASSWORD
ARG POSTGRES_USER
ARG POSTGRES_PASSWORD
ARG SUPERSET_SECRET_KEY
# Switching to root to install the required packages
USER root

RUN pip install uv

#COPY --chown=superset:superset ./superset/assets .
RUN uv pip install --system duckdb-engine duckdb
USER superset
RUN superset fab create-admin \
              --username ${SUPERSET_ADMIN} \
              --firstname Superset \
              --lastname Admin \
              --email admin@example.com \
              --password ${SUPERSET_PASSWORD}
RUN superset db upgrade
RUN superset init
RUN superset set_database_uri -d DW  -u duckdb:///superset_home/db/github_dw.duckdb

CMD ["gunicorn", "--bind", "0.0.0.0:8088", "--access-logfile", "-", "--error-logfile", "-", "--workers", "1", "--worker-class", "gthread", "--threads", "20", "--timeout", "60", "--limit-request-line", "0", "--limit-request-field_size", "0", "superset.app:create_app()"]
