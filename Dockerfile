FROM alpine

RUN apk add --update \
      ca-certificates \
      bash \
      curl && \
      rm -rf /var/cache/apk/*

ADD target/x86_64-unknown-linux-musl/release/dracula-covid19 /usr/bin/dracula-covid19