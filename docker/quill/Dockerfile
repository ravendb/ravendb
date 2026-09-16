# syntax=docker/dockerfile:1.7

ARG RUNTIME_IMAGE=mcr.microsoft.com/dotnet/aspnet:10.0.11
ARG S6_OVERLAY_VERSION=3.2.1.0

FROM ${RUNTIME_IMAGE} AS payload
ARG QUILL_PAYLOAD
RUN apt-get update \
    && apt-get install -y --no-install-recommends bzip2 \
    && rm -rf /var/lib/apt/lists/*
COPY ${QUILL_PAYLOAD} /tmp/quill.tar.bz2
RUN mkdir -p /payload \
    && tar -C /payload -xjf /tmp/quill.tar.bz2 \
    && rm /tmp/quill.tar.bz2


FROM ${RUNTIME_IMAGE} AS runtime
ARG S6_OVERLAY_VERSION
ARG TARGETARCH

ARG QUILL_USER_ID=999
ARG QUILL_GROUP_ID=999

RUN apt-get update \
    && apt-get install -y --no-install-recommends xz-utils curl ca-certificates nginx-full openssl jq libcap2-bin \
    && rm -rf /var/lib/apt/lists/*

# no top-level USER by design: s6/pid 1 needs root for setup, 
# so each exposed daemon drops to this quill user via s6-setuidgid in its run script 
RUN groupadd -r -g ${QUILL_GROUP_ID} quill
RUN useradd -r -u ${QUILL_USER_ID} -g quill -d /var/lib/quill -s /usr/sbin/nologin quill

RUN chown -R quill:quill /var/lib/nginx \
    && setcap 'cap_net_bind_service=+ep' /usr/sbin/nginx

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

# ravendb (server + tools + Studio.zip) and the Quill web app, from the pre-built payload.
COPY --from=payload /payload/Quill/ravendb /app/ravendb
COPY --from=payload /payload/Quill/web     /app/web

# Only /app/ravendb needs to be quill-writable (RavenDB unpacks its studio/nuget Packages there at
# runtime); /app/web stays root-owned so a compromised quill can't swap the parent root writes into.
RUN chown -R quill:quill /app/ravendb
COPY docker/quill/nginx.conf /etc/nginx/nginx.conf

COPY docker/quill/s6-rc.d/ /etc/s6-overlay/s6-rc.d/
COPY docker/quill/scripts/update-dns.sh /usr/local/bin/update-dns
COPY docker/quill/scripts/proxy-certs.sh /usr/local/bin/proxy-certs
COPY docker/quill/scripts/cert-chain.sh /usr/local/bin/cert-chain
COPY docker/quill/scripts/svc-log.sh /usr/local/bin/svc-log
COPY docker/quill/scripts/mask-tokens.sh /usr/local/bin/mask-tokens
COPY docker/quill/scripts/prepare-volume.sh /usr/local/bin/prepare-volume
RUN chmod +x /etc/s6-overlay/s6-rc.d/*/run \
             /usr/local/bin/update-dns \
             /usr/local/bin/proxy-certs \
             /usr/local/bin/cert-chain \
             /usr/local/bin/svc-log \
             /usr/local/bin/mask-tokens \
             /usr/local/bin/prepare-volume

EXPOSE 443

VOLUME /var/lib/quill

# --start-period must match RAVEN_QUILL_STARTUP_GRACE_SECONDS below: both are the startup budget,
# one for Docker, one for 05-console deciding when a slow start counts as stuck.
HEALTHCHECK --interval=30s --timeout=5s --start-period=180s \
    CMD curl -fsk https://127.0.0.1/healthz && curl -fs http://127.0.0.1:5000/healthz

# S6_VERBOSITY=0 keeps the supervisor's lifecycle chatter off stdout
ENV S6_VERBOSITY=0 \
    RAVEN_QUILL_RAVEN_URL=http://127.0.0.1:8080 \
    RAVEN_QUILL_WEB_LISTEN_URL=http://127.0.0.1:5000 \
    RAVEN_QUILL_CONFIG_DB=quill-config \
    RAVEN_QUILL_SETUP_PACKAGE_PATH=/var/lib/quill/setup \
    RAVEN_QUILL_RAVENDB_S6_SERVICE=/run/service/01-ravendb \
    RAVEN_QUILL_RAVENDB_INTERNAL_PORT=8443 \
    RAVEN_QUILL_RAVENDB_PROXY_PORT=8447 \
    RAVEN_DataDir=/var/lib/quill/ravendb \
    RAVEN_Logs_Path=/var/lib/quill/logs/ravendb-server \
    RAVEN_QUILL_STARTUP_GRACE_SECONDS=180

ENTRYPOINT ["/init"]
