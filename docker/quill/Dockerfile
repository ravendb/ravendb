# syntax=docker/dockerfile:1.7

ARG SDK_IMAGE=mcr.microsoft.com/dotnet/sdk:10.0.400
ARG RUNTIME_IMAGE=mcr.microsoft.com/dotnet/aspnet:10.0.11-noble
ARG NODE_IMAGE=node:24-bookworm-slim
ARG RAVENDB_IMAGE=ravendb/ravendb-nightly:7.2-latest
ARG PNPM_VERSION=11.2.2
ARG S6_OVERLAY_VERSION=3.2.1.0

# Stage 0: alias the published RavenDB image
FROM ${RAVENDB_IMAGE} AS ravendb


# Stage 1: build the backend just enough to emit the OpenAPI doc (consumed by Stage 2).
FROM ${SDK_IMAGE} AS quill-openapi-build
WORKDIR /src
COPY . .

RUN --mount=type=cache,target=/root/.nuget/packages,sharing=locked \
    dotnet build src/Raven.Quill/Raven.Quill.csproj \
        -c Release \
        --nologo \
        --tl:off


# Stage 2: build the React/Vite frontend (consumes the OpenAPI doc from Stage 1).
FROM ${NODE_IMAGE} AS quill-frontend-build
ARG PNPM_VERSION
WORKDIR /workspace

RUN corepack enable && corepack prepare pnpm@${PNPM_VERSION} --activate

COPY src/Raven.Quill.Web/package.json \
     src/Raven.Quill.Web/pnpm-lock.yaml \
     src/Raven.Quill.Web/pnpm-workspace.yaml \
    ./src/Raven.Quill.Web/
# The embeddable widget is its own workspace package, so its manifest belongs in the cached install layer too.
COPY src/Raven.Quill.Web/packages/widget/package.json ./src/Raven.Quill.Web/packages/widget/

WORKDIR /workspace/src/Raven.Quill.Web

RUN --mount=type=cache,target=/root/.local/share/pnpm/store,sharing=locked \
    pnpm install --frozen-lockfile

WORKDIR /workspace

COPY --from=quill-openapi-build /src/src/Raven.Quill/obj/openapi/quill.json ./src/Raven.Quill/obj/openapi/quill.json
COPY src/Raven.Quill.Web/ ./src/Raven.Quill.Web/

WORKDIR /workspace/src/Raven.Quill.Web
ENV RAVEN_QUILL_OPENAPI_SKIP_SPEC_BUILD=1
RUN pnpm build


# Stage 3: publish the backend and overlay the Vite output into wwwroot.
FROM ${SDK_IMAGE} AS web-build
ARG TARGETARCH
WORKDIR /src
COPY . .

RUN --mount=type=cache,target=/root/.nuget/packages,sharing=locked \
    . docker/quill/scripts/dotnet-rid.sh \
    && dotnet publish src/Raven.Quill/Raven.Quill.csproj \
        -c Release \
        -r "$DOTNET_RID" \
        --self-contained false \
        -o /publish/web \
        --nologo \
    && rm -rf /publish/web/wwwroot

COPY --from=quill-frontend-build /workspace/src/Raven.Quill.Web/dist /publish/web/wwwroot


# Stage 4: runtime - aspnet base + s6-overlay supervising RavenDB, the web app, and nginx.
FROM ${RUNTIME_IMAGE} AS runtime
ARG S6_OVERLAY_VERSION
ARG TARGETARCH

RUN apt-get update \
    && apt-get install -y --no-install-recommends xz-utils curl ca-certificates nginx-full openssl jq \
    && rm -rf /var/lib/apt/lists/*

RUN <<EOF
set -eu
cd /tmp

case "$TARGETARCH" in
  amd64) S6_ARCH=x86_64 ;;
  arm64) S6_ARCH=aarch64 ;;
  arm)   S6_ARCH=arm ;;
  *) echo "Unsupported TARGETARCH: $TARGETARCH" >&2; exit 1 ;;
esac

S6_URL="https://github.com/just-containers/s6-overlay/releases/download/v${S6_OVERLAY_VERSION}"

for part in noarch "$S6_ARCH"; do
  curl -fsSLO "${S6_URL}/s6-overlay-${part}.tar.xz"
  curl -fsSLO "${S6_URL}/s6-overlay-${part}.tar.xz.sha256"
done

sha256sum -c s6-overlay-*.sha256
tar -C / -Jxpf s6-overlay-noarch.tar.xz
tar -C / -Jxpf "s6-overlay-${S6_ARCH}.tar.xz"
rm s6-overlay-*
EOF

COPY --from=ravendb   /usr/lib/ravendb/server /app/ravendb
COPY --from=web-build /publish/web            /app/web
COPY docker/quill/nginx.conf /etc/nginx/nginx.conf

COPY docker/quill/s6-rc.d/ /etc/s6-overlay/s6-rc.d/
COPY docker/quill/scripts/proxy-certs.sh /usr/local/bin/proxy-certs
COPY docker/quill/scripts/cert-chain.sh /usr/local/bin/cert-chain
COPY docker/quill/scripts/svc-log.sh /usr/local/bin/svc-log
RUN chmod +x /etc/s6-overlay/s6-rc.d/*/run \
             /usr/local/bin/proxy-certs \
             /usr/local/bin/cert-chain \
             /usr/local/bin/svc-log

EXPOSE 443

VOLUME /var/lib/quill

# --start-period must match RAVEN_QUILL_STARTUP_GRACE_SECONDS below: both are the startup budget,
# one for Docker, one for 05-console deciding when a slow start counts as stuck.
HEALTHCHECK --interval=30s --timeout=5s --start-period=180s \
    CMD curl -fsk https://127.0.0.1/healthz && curl -fs http://127.0.0.1:5000/healthz

# S6_VERBOSITY=0 keeps the supervisor's lifecycle chatter off stdout
ENV S6_VERBOSITY=0 \
    RAVEN_QUILL_RAVEN_URL=http://127.0.0.1:8080 \
    RAVEN_QUILL_WEB_LISTEN_URL=http://0.0.0.0:5000 \
    RAVEN_QUILL_CONFIG_DB=quill-config \
    RAVEN_QUILL_SETUP_PACKAGE_PATH=/var/lib/quill/setup \
    RAVEN_QUILL_RAVENDB_S6_SERVICE=/run/service/01-ravendb \
    RAVEN_QUILL_RAVENDB_INTERNAL_PORT=8443 \
    RAVEN_QUILL_RAVENDB_PROXY_PORT=8447 \
    RAVEN_DataDir=/var/lib/quill/ravendb \
    RAVEN_Logs_Path=/var/lib/quill/logs/ravendb-server \
    RAVEN_QUILL_STARTUP_GRACE_SECONDS=180

ENTRYPOINT ["/init"]
