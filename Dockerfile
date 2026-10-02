FROM rust:1-slim-trixie AS build
WORKDIR /src

COPY Cargo.toml Cargo.lock ./
RUN mkdir src && echo 'fn main() {}' > src/main.rs && touch src/lib.rs \
    && cargo build --release --locked \
    && rm -rf src

COPY src ./src
RUN touch src/main.rs src/lib.rs && cargo build --release --locked

FROM chromedp/headless-shell:latest AS runtime
WORKDIR /app

COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=build /src/target/release/prospectar-engine /usr/local/bin/prospectar-engine

ENV PORT=3001 \
    CHROME_PATH=/headless-shell/headless-shell \
    SSL_CERT_FILE=/etc/ssl/certs/ca-certificates.crt

EXPOSE 3001

ENTRYPOINT ["prospectar-engine"]
