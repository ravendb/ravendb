# syntax=docker/dockerfile:1.7
#
# RavenDB Quill — single-image build for the 8-week internal demo.
#
# Build context is the RavenDB repo root. Invoke via scripts/ai-appliance/up.ps1
# or directly:
#
#   cd <ravendb repo root>
#   docker build \
#     -f docker/ai-appliance/Dockerfile \
#     -t ravendb/ai-appliance:demo \
#     .
#
# Five stages:
#   1. ravendb-build          — dotnet publish of Raven.Server.
#   2. appliance-openapi-build — dotnet build of Raven.AiAppliance to emit openapi.json.
#   3. appliance-frontend-build — pnpm build of Raven.AiAppliance.Web.
#   4. web-build              — dotnet publish of Raven.AiAppliance.
#   5. runtime                — aspnet base image, s6-overlay v3, both artifacts copied in.
#
# Security posture: the nginx :443 TLS/SNI front and operator API-key auth are built in (Phase 1).
# On first run, before activation, RavenDB starts unsecured on loopback and the web app is reachable
# over plain HTTP; once the setup package is applied the appliance restarts into secure mode (mTLS
# passthrough for db.*; terminated dashboard./api./public.*). External exposure is governed by which
# host ports are published, not by the servers' bind addresses.

# Pin base images to specific patch versions matching the repo's pinned
# toolchain: global.json declares SDK 10.0.301. The runtime image hosts both
# apps, so it must satisfy the highest RuntimeFrameworkVersion among them:
# Raven.Server.csproj pins 10.0.9 and Raven.AiAppliance.csproj pins 10.0.8, so
# the ASP.NET runtime must be >= 10.0.9 (the appliance app rolls forward
# 10.0.8 -> 10.0.9). Floating `:10.0` tags would let CI and local rebuilds pick
# up patch-level changes that diverge from those pins. Bump these together with
# the corresponding global.json / RuntimeFrameworkVersion changes.
ARG SDK_IMAGE=mcr.microsoft.com/dotnet/sdk:10.0.301
ARG RUNTIME_IMAGE=mcr.microsoft.com/dotnet/aspnet:10.0.9
ARG NODE_IMAGE=node:24-bookworm-slim
ARG PNPM_VERSION=11.2.2
ARG S6_OVERLAY_VERSION=3.2.1.0

# -----------------------------------------------------------------------------
# Stage 1: build Raven.Server.
# -----------------------------------------------------------------------------
FROM ${SDK_IMAGE} AS ravendb-build
WORKDIR /src
COPY . .

RUN dotnet publish src/Raven.Server/Raven.Server.csproj \
        -c Release \
        -o /publish/raven \
        --nologo

# -----------------------------------------------------------------------------
# Stage 2: build-time OpenAPI document for Raven.AiAppliance.
# -----------------------------------------------------------------------------
FROM ${SDK_IMAGE} AS appliance-openapi-build
WORKDIR /src
COPY . .

RUN dotnet build src/Raven.AiAppliance/Raven.AiAppliance.csproj \
       -c Release \
       --nologo \
       --tl:off

# -----------------------------------------------------------------------------
# Stage 3: build Raven.AiAppliance.Web.
# -----------------------------------------------------------------------------
FROM ${NODE_IMAGE} AS appliance-frontend-build
ARG PNPM_VERSION
WORKDIR /workspace

RUN corepack enable \
    && corepack prepare pnpm@${PNPM_VERSION} --activate

COPY src/Raven.AiAppliance.Web/package.json \
     src/Raven.AiAppliance.Web/pnpm-lock.yaml \
     src/Raven.AiAppliance.Web/pnpm-workspace.yaml \
    ./src/Raven.AiAppliance.Web/

WORKDIR /workspace/src/Raven.AiAppliance.Web

RUN pnpm install --frozen-lockfile

WORKDIR /workspace
COPY --from=appliance-openapi-build /src/src/Raven.AiAppliance/obj/openapi/ai-appliance.json ./src/Raven.AiAppliance/obj/openapi/ai-appliance.json
COPY src/Raven.AiAppliance.Web/ ./src/Raven.AiAppliance.Web/

WORKDIR /workspace/src/Raven.AiAppliance.Web
ENV RAVEN_QUILL_OPENAPI_SKIP_SPEC_BUILD=1
RUN pnpm build

# -----------------------------------------------------------------------------
# Stage 4: build Raven.AiAppliance.
# -----------------------------------------------------------------------------
FROM ${SDK_IMAGE} AS web-build
WORKDIR /src
COPY . .

RUN dotnet publish src/Raven.AiAppliance/Raven.AiAppliance.csproj \
        -c Release \
        -o /publish/web \
        --nologo

# Replace the server's checked-in placeholder wwwroot with the Vite build.
RUN rm -rf /publish/web/wwwroot
COPY --from=appliance-frontend-build /workspace/src/Raven.AiAppliance.Web/dist /publish/web/wwwroot

# -----------------------------------------------------------------------------
# Stage 5: runtime. ASP.NET base + s6-overlay supervising RavenDB and the web app.
# -----------------------------------------------------------------------------
FROM ${RUNTIME_IMAGE} AS runtime
ARG S6_OVERLAY_VERSION
ENV S6_OVERLAY_VERSION=${S6_OVERLAY_VERSION}

# s6-overlay needs xz + curl to bootstrap; nginx is the :443 SNI front (Phase 1) and openssl
# converts the package's wildcard PFX to the PEM nginx serves.
RUN apt-get update \
    && apt-get install -y --no-install-recommends xz-utils curl ca-certificates nginx-full openssl \
    && rm -rf /var/lib/apt/lists/*

# Install s6-overlay v3 (noarch + x86_64). Two-tarball layout per
# https://github.com/just-containers/s6-overlay#usage.
#
# Download both tarballs alongside their upstream-published .sha256 files
# (keeping the original filenames via `curl -O` so `sha256sum -c` matches the
# entries in the checksum files), verify, then extract. Catches corrupted
# downloads and any MITM that doesn't also own the GitHub release's TLS chain.
RUN cd /tmp \
    && curl -fsSLO "https://github.com/just-containers/s6-overlay/releases/download/v${S6_OVERLAY_VERSION}/s6-overlay-noarch.tar.xz" \
    && curl -fsSLO "https://github.com/just-containers/s6-overlay/releases/download/v${S6_OVERLAY_VERSION}/s6-overlay-noarch.tar.xz.sha256" \
    && curl -fsSLO "https://github.com/just-containers/s6-overlay/releases/download/v${S6_OVERLAY_VERSION}/s6-overlay-x86_64.tar.xz" \
    && curl -fsSLO "https://github.com/just-containers/s6-overlay/releases/download/v${S6_OVERLAY_VERSION}/s6-overlay-x86_64.tar.xz.sha256" \
    && sha256sum -c s6-overlay-noarch.tar.xz.sha256 s6-overlay-x86_64.tar.xz.sha256 \
    && tar -C / -Jxpf s6-overlay-noarch.tar.xz \
    && tar -C / -Jxpf s6-overlay-x86_64.tar.xz \
    && rm s6-overlay-noarch.tar.xz s6-overlay-noarch.tar.xz.sha256 s6-overlay-x86_64.tar.xz s6-overlay-x86_64.tar.xz.sha256

COPY --from=ravendb-build /publish/raven /app/ravendb
COPY --from=web-build     /publish/web   /app/web

# Override the settings.json baked into `dotnet publish src/Raven.Server`.
# The default file hard-codes ServerUrl=127.0.0.1:8080 (and overrides the
# RAVEN_ServerUrl env var, because settings.json wins in RavenDB's config
# precedence). Ours binds 0.0.0.0 so Docker port-publish can forward.
COPY docker/ai-appliance/ravendb-settings.json /app/ravendb/settings.json

# Apply the license. RavenDB's License.Path defaults to "license.json" resolved
# against its working dir (/app/ravendb/, where the 01-ravendb run script cd's
# before launch), so dropping the file here activates the server with no
# settings change. The file is customer-secret and gitignored (see
# docker/.gitignore); supply your own at docker/ai-appliance/license.json.
COPY docker/ai-appliance/license.json /app/ravendb/license.json

# Studio bundle. `dotnet publish src/Raven.Server` doesn't include Studio.zip
# (the official image's CI does a separate zip step). Pulling it from the
# published `ravendb/ravendb:latest` image keeps our build self-contained.
COPY --from=ravendb/ravendb:latest /usr/lib/ravendb/server/Raven.Studio.zip /app/ravendb/

# nginx :443 SNI front (Phase 1): db.*/a.* TLS-passthrough to RavenDB (mTLS preserved); the web
# subdomains terminate with the package's wildcard cert and forward to the web app on :5000.
COPY docker/ai-appliance/nginx.conf /etc/nginx/nginx.conf

# s6-rc.d service definitions ship under docker/ai-appliance/s6-rc.d/.
COPY docker/ai-appliance/s6-rc.d/ /etc/s6-overlay/s6-rc.d/
RUN find /etc/s6-overlay/s6-rc.d -type f -exec sed -i 's/\r$//' {} + \
    && chmod +x /etc/s6-overlay/s6-rc.d/01-ravendb/run \
             /etc/s6-overlay/s6-rc.d/02-web/run \
             /etc/s6-overlay/s6-rc.d/03-proxy/run

# Ports: nginx fronts :443 (the SNI router). :5000 stays exposed for first-run /
# pre-activation (before the wildcard cert exists, nginx isn't up yet). RavenDB
# is reached through the proxy (db.* passthrough), not published directly.
EXPOSE 5000 443

VOLUME /var/lib/ai-appliance

ENV RAVEN_QUILL_RAVEN_URL=http://127.0.0.1:8080 \
    RAVEN_QUILL_WEB_LISTEN_URL=http://0.0.0.0:5000 \
    RAVEN_QUILL_CONFIG_DB=quill-config \
    RAVEN_QUILL_SETUP_PACKAGE_PATH=/var/lib/ai-appliance/setup \
    RAVEN_QUILL_RAVENDB_S6_SERVICE=/run/service/01-ravendb \
    RAVEN_QUILL_RAVENDB_INTERNAL_PORT=8443 \
    RAVEN_DataDir=/var/lib/ai-appliance/ravendb

ENTRYPOINT ["/init"]
