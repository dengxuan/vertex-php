# Self-contained dev image for vertex-php: PHP 8.3 + Swoole + composer + protoc.
# Nothing here depends on the other Vertex repos — this SDK builds and tests
# itself in isolation.
FROM php:8.3-cli

# Build deps for pecl/swoole + protoc, plus git/unzip for composer.
RUN apt-get update && apt-get install -y --no-install-recommends \
        git unzip libssl-dev zlib1g-dev libbrotli-dev protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

# Swoole with the optional features the transport may use (OpenSSL for TLS,
# HTTP/2 client). enable-openssl is what lets the coroutine HTTP/2 client speak
# to TLS servers; the compat default is plaintext but production won't be.
RUN pecl install swoole \
    && docker-php-ext-enable swoole \
    && php -m | grep -qi swoole

# Composer (copy the official binary from its image — simplest reproducible path).
COPY --from=composer:2 /usr/bin/composer /usr/local/bin/composer

WORKDIR /workspaces/vertex-php
