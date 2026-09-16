#!/bin/sh
set -e

# The pfx RavenDB writes keeps only the leaf certificate, so nginx has no path to a trusted root to
# serve. Collect the issuers by following each certificate's Authority Information Access extension
# and write them to $2 for proxy-certs to concatenate.

PFX="$1"
OUT="$2"

if [ ! -f "$PFX" ]; then
    echo "cert-chain: no pfx at '$PFX'; nothing to do." >&2
    exit 0
fi

umask 077

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

CA_BUNDLE=/etc/ssl/certs/ca-certificates.crt

trusted() {
    if [ -s "$WORK/chain.pem" ]; then
        openssl verify -CAfile "$CA_BUNDLE" -untrusted "$WORK/chain.pem" "$WORK/leaf.pem" >/dev/null 2>&1
    else
        openssl verify -CAfile "$CA_BUNDLE" "$WORK/leaf.pem" >/dev/null 2>&1
    fi
}

extract_certs() {
    openssl x509 -inform DER -in "$1" -out "$2" 2>/dev/null \
        || openssl x509 -in "$1" -out "$2" 2>/dev/null \
        || openssl pkcs7 -inform DER -in "$1" -print_certs -out "$2" 2>/dev/null \
        || openssl pkcs7 -in "$1" -print_certs -out "$2" 2>/dev/null \
        || return 1

    grep -q "BEGIN CERTIFICATE" "$2"
}

issuer_urls() {
    openssl x509 -in "$1" -noout -ext authorityInfoAccess 2>/dev/null \
        | sed -n 's#^ *CA Issuers - URI:##p' \
        | sed -e 's#[[:space:]]*$##' -e '/^$/d'
}

openssl pkcs12 -in "$PFX" -nokeys -clcerts -passin pass: -out "$WORK/leaf.pem"
openssl pkcs12 -in "$PFX" -nokeys -cacerts -passin pass: -out "$WORK/chain.pem" 2>/dev/null || true
touch "$WORK/chain.pem"

last="$WORK/leaf.pem"
hops=0
added=0

now() {
    cut -d. -f1 /proc/uptime
}

deadline=$(( $(now) + 30 ))

# Stopping on `trusted` rather than on a known issuer name keeps this working when Let's Encrypt
# rotates intermediates, and drops the extra hop once their new root ships in trust stores.
while [ "$hops" -lt 4 ] && [ "$(now)" -lt "$deadline" ] && ! trusted; do
    hops=$((hops + 1))
    next="$WORK/issuer-$hops.pem"

    # Try every caIssuers URI rather than only the first: a mirror can be down. caIssuers is usually
    # DER, but PEM and PKCS#7 are also served in the wild.
    for url in $(issuer_urls "$last"); do
        [ "$(now)" -lt "$deadline" ] || break

        curl -fsSL --proto '=http,https' --proto-redir '=http,https' \
            --connect-timeout 3 --max-time 10 --max-filesize 1M \
            "$url" -o "$WORK/fetched" || continue

        if extract_certs "$WORK/fetched" "$next" \
            && openssl verify -partial_chain -trusted "$next" "$last" >/dev/null 2>&1; then
            break
        fi

        # Discard it, or a rejected certificate would still be sitting there after the sweep.
        rm -f "$next"
    done

    [ -f "$next" ] || break

    cat "$next" >> "$WORK/chain.pem"
    last="$next"
    added=$((added + 1))
done

cp "$WORK/chain.pem" "$OUT"

[ "$added" -eq 0 ] || echo "cert-chain: added $added issuer(s) to the chain nginx serves."
if ! trusted && [ -n "$(issuer_urls "$last")" ]; then
    echo "cert-chain: chain for $(basename "$PFX") is incomplete; $(basename "$last") still advertises an issuer." >&2
fi
