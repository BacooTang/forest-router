FROM rust:1.98-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY static ./static
RUN cargo build --release --locked
FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/forest-router /usr/local/bin/forest-router
ENV FOREST_ROUTER_HOME=/data FOREST_LISTEN=0.0.0.0:8119
VOLUME ["/data"]
EXPOSE 8119
ENTRYPOINT ["forest-router"]
