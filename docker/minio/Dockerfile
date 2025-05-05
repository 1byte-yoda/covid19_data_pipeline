FROM alpine:3.19

RUN apk add --no-cache bash curl grep \
 && curl -O https://dl.min.io/server/minio/release/linux-amd64/minio \
 && chmod +x minio \
 && mv minio /usr/bin/minio \
 && curl -O https://dl.min.io/client/mc/release/linux-amd64/mc \
 && chmod +x mc \
 && mv mc /usr/bin/mc

ADD ./docker/minio/entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

WORKDIR /minio

ENTRYPOINT ["/entrypoint.sh"]
