{% macro create_minio_secret(secret_name) %}
    INSTALL httpfs;
    LOAD httpfs;
    INSTALL delta;
    LOAD delta;
    CREATE OR REPLACE PERSISTENT SECRET {{ secret_name }} (
           TYPE s3,
           PROVIDER config,
           KEY_ID '{{ env_var("MINIO_ACCESS_KEY") }}',
           SECRET '{{ env_var("MINIO_SECRET_KEY") }}',
           URL_STYLE 'path',
           USE_SSL false,
           ENDPOINT '{{ env_var("ENDPOINT_URL") }}'
    );
{% endmacro %}