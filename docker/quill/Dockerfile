# syntax=docker/dockerfile:1.7

# Pin base images to specific patch versions matching the repo's pinned
# toolchain: global.json declares SDK 10.0.301. The runtime image hosts both
# apps, so it must satisfy the highest RuntimeFrameworkVersion among them:
# Raven.Server.csproj pins 10.0.9 and Raven.Quill.csproj pins 10.0.8, so
# the ASP.NET runtime must be >= 10.0.9 (the appliance app rolls forward
# 10.0.8 -> 10.0.9). Floating `:10.0` tags would let CI and local rebuilds pick
# up patch-level changes that diverge from those pins. Bump these together with
# the corresponding global.json / RuntimeFrameworkVersion changes.
ARG SDK_IMAGE=mcr.microsoft.com/dotnet/sdk:10.0.301
ARG RUNTIME_IMAGE=mcr.microsoft.com/dotnet/aspnet:10.0.9
ARG NODE_IMAGE=node:24-bookworm-slim
ARG RAVENDB_STUDIO_IMAGE=ravendb/ravendb:7.2.4
ARG PNPM_VERSION=11.2.2
ARG S6_OVERLAY_VERSION=3.2.1.0

# Stage 0: alias the external RavenDB image so we can COPY --from it by name.
FROM --platform=$BUILDPLATFORM ${RAVENDB_STUDIO_IMAGE} AS ravendb-studio

# Stage 1: build Raven.Server from source.
FROM ${SDK_IMAGE} AS ravendb-build
ARG TARGETARCH
WORKDIR /src
COPY . .

RUN --mount=type=cache,target=/root/.nuget/packages,sharing=locked \
    . docker/quill/scripts/dotnet-rid.sh \
    && dotnet publish src/Raven.Server/Raven.Server.csproj \
        -c Release \
        -r "$DOTNET_RID" \
        --self-contained false \
        -o /publish/raven \
        --nologo


# Stage 2: build the backend just enough to emit the OpenAPI doc (consumed by Stage 3).
FROM ${SDK_IMAGE} AS quill-openapi-build
WORKDIR /src
COPY . .

RUN --mount=type=cache,target=/root/.nuget/packages,sharing=locked \
    dotnet build src/Raven.Quill/Raven.Quill.csproj \
        -c Release \
        --nologo \
        --tl:off


# Stage 3: build the React/Vite frontend (consumes the OpenAPI doc from Stage 2).
FROM ${NODE_IMAGE} AS quill-frontend-build
ARG PNPM_VERSION
WORKDIR /workspace

RUN corepack enable && corepack prepare pnpm@${PNPM_VERSION} --activate

COPY src/Raven.Quill.Web/package.json \
     src/Raven.Quill.Web/pnpm-lock.yaml \
     src/Raven.Quill.Web/pnpm-workspace.yaml \
    ./src/Raven.Quill.Web/

WORKDIR /workspace/src/Raven.Quill.Web

RUN --mount=type=cache,target=/root/.local/share/pnpm/store,sharing=locked \
    pnpm install --frozen-lockfile

WORKDIR /workspace

COPY --from=quill-openapi-build /src/src/Raven.Quill/obj/openapi/quill.json ./src/Raven.Quill/obj/openapi/quill.json
COPY src/Raven.Quill.Web/ ./src/Raven.Quill.Web/

WORKDIR /workspace/src/Raven.Quill.Web
ENV RAVEN_QUILL_OPENAPI_SKIP_SPEC_BUILD=1
RUN pnpm build


# Stage 4: publish the backend and overlay the Vite output into wwwroot.
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


# Stage 5: runtime - aspnet base + s6-overlay supervising RavenDB, the web app, and nginx.
FROM ${RUNTIME_IMAGE} AS runtime
ARG S6_OVERLAY_VERSION
ARG TARGETARCH

RUN apt-get update \
    && apt-get install -y --no-install-recommends xz-utils curl ca-certificates nginx-full openssl \
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

COPY --from=ravendb-build /publish/raven /app/ravendb
COPY --from=web-build     /publish/web   /app/web
COPY docker/quill/ravendb-settings.json /app/ravendb/settings.json
COPY --from=ravendb-studio /usr/lib/ravendb/server/Raven.Studio.zip /app/ravendb/
COPY docker/quill/nginx.conf /etc/nginx/nginx.conf

COPY docker/quill/s6-rc.d/ /etc/s6-overlay/s6-rc.d/
RUN chmod +x /etc/s6-overlay/s6-rc.d/01-ravendb/run \
             /etc/s6-overlay/s6-rc.d/02-web/run \
             /etc/s6-overlay/s6-rc.d/03-proxy/run

EXPOSE 443

VOLUME /var/lib/quill

HEALTHCHECK --interval=30s --timeout=5s --start-period=180s \
    CMD curl -fsk https://127.0.0.1/healthz && curl -fs http://127.0.0.1:5000/healthz

ENV RAVEN_QUILL_RAVEN_URL=http://127.0.0.1:8080 \
    RAVEN_QUILL_WEB_LISTEN_URL=http://0.0.0.0:5000 \
    RAVEN_QUILL_CONFIG_DB=quill-config \
    RAVEN_QUILL_SETUP_PACKAGE_PATH=/var/lib/quill/setup \
    RAVEN_QUILL_RAVENDB_S6_SERVICE=/run/service/01-ravendb \
    RAVEN_QUILL_RAVENDB_INTERNAL_PORT=8443 \
    RAVEN_DataDir=/var/lib/quill/ravendb

ENTRYPOINT ["/init"]
