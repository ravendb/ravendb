# syntax=docker/dockerfile:1.7
#
# RavenDB AI Appliance — single-image build for the 8-week internal demo.
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
# Four stages:
#   1. ravendb-build — dotnet publish of Raven.Server.
#   2. appliance-frontend-build — pnpm build of Raven.AiAppliance.Web.
#   3. web-build     — dotnet publish of Raven.AiAppliance.
#   4. runtime       — aspnet base image, s6-overlay v3, both artifacts copied in.
#
# Security posture is intentionally minimal: RavenDB starts with
# Setup.Mode=Unsecured, Studio binds to 0.0.0.0:8080 inside the container,
# and the web app listens on plain HTTP. The full POC layers TLS + API-key
# auth on top. External exposure is controlled by whether the host port is
# published, not by the server's bind address.

# Pin base images to specific patch versions matching the repo's pinned
# toolchain: global.json declares SDK 10.0.300, and Raven.AiAppliance.csproj
# pins RuntimeFrameworkVersion to 10.0.8. Floating `:10.0` tags would let CI
# and local rebuilds pick up patch-level changes that diverge from those
# pins. Bump these together with the corresponding global.json /
# RuntimeFrameworkVersion changes.
ARG SDK_IMAGE=mcr.microsoft.com/dotnet/sdk:10.0.300
ARG RUNTIME_IMAGE=mcr.microsoft.com/dotnet/aspnet:10.0.8
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
# Stage 2: build Raven.AiAppliance.Web.
# -----------------------------------------------------------------------------
FROM ${NODE_IMAGE} AS appliance-frontend-build
ARG PNPM_VERSION
WORKDIR /src/Raven.AiAppliance.Web

RUN corepack enable \
    && corepack prepare pnpm@${PNPM_VERSION} --activate

COPY src/Raven.AiAppliance.Web/package.json \
     src/Raven.AiAppliance.Web/pnpm-lock.yaml \
     src/Raven.AiAppliance.Web/pnpm-workspace.yaml \
     ./

RUN pnpm install --frozen-lockfile

COPY src/Raven.AiAppliance.Web/ ./
RUN pnpm build

# -----------------------------------------------------------------------------
# Stage 3: build Raven.AiAppliance.
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
COPY --from=appliance-frontend-build /src/Raven.AiAppliance.Web/dist /publish/web/wwwroot

# -----------------------------------------------------------------------------
# Stage 4: runtime. ASP.NET base + s6-overlay supervising RavenDB and the web app.
# -----------------------------------------------------------------------------
FROM ${RUNTIME_IMAGE} AS runtime
ARG S6_OVERLAY_VERSION
ENV S6_OVERLAY_VERSION=${S6_OVERLAY_VERSION}

# s6-overlay needs xz + curl to bootstrap.
RUN apt-get update \
    && apt-get install -y --no-install-recommends xz-utils curl ca-certificates \
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

# Studio bundle. `dotnet publish src/Raven.Server` doesn't include Studio.zip
# (the official image's CI does a separate zip step). Pulling it from the
# published `ravendb/ravendb:latest` image keeps our build self-contained.
COPY --from=ravendb/ravendb:latest /usr/lib/ravendb/server/Raven.Studio.zip /app/ravendb/

# s6-rc.d service definitions ship under docker/ai-appliance/s6-rc.d/.
COPY docker/ai-appliance/s6-rc.d/ /etc/s6-overlay/s6-rc.d/
RUN find /etc/s6-overlay/s6-rc.d -type f -exec sed -i 's/\r$//' {} + \
    && chmod +x /etc/s6-overlay/s6-rc.d/01-ravendb/run \
             /etc/s6-overlay/s6-rc.d/02-web/run

# Ports: only the web app's listener is published by default. RavenDB stays
# inside the container; use `docker run -p 8080:8080` to expose Studio
# (scripts/ai-appliance/up.ps1 has a -WithStudio switch for this).
EXPOSE 5000

VOLUME /var/lib/ai-appliance

ENV RAVEN_AI_RAVEN_URL=http://127.0.0.1:8080 \
    RAVEN_AI_WEB_LISTEN_URL=http://0.0.0.0:5000 \
    RAVEN_AI_CONFIG_DB=ai-appliance-config \
    RAVEN_AI_SETUP_PACKAGE_PATH=/var/lib/ai-appliance/setup \
    RAVEN_AI_SETUP_PACKAGE_ZIP=/var/lib/ai-appliance/setup-source.zip \
    RAVEN_AI_RAVENDB_S6_SERVICE=/run/service/01-ravendb \
    RAVEN_DataDir=/var/lib/ai-appliance/ravendb

ENTRYPOINT ["/init"]
