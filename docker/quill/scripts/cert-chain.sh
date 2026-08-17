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

# openssl treats an empty -untrusted file as an error rather than as "no intermediates", so only
# pass it once we have something to put in it.
trusted() {
    if [ -s "$WORK/chain.pem" ]; then
        openssl verify -untrusted "$WORK/chain.pem" "$WORK/leaf.pem" >/dev/null 2>&1
    else
        openssl verify "$WORK/leaf.pem" >/dev/null 2>&1
    fi
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

deadline=$(( $(date +%s) + 30 ))

# Stopping on `trusted` rather than on a known issuer name keeps this working when Let's Encrypt
# rotates intermediates, and drops the extra hop once their new root ships in trust stores.
while [ "$hops" -lt 4 ] && [ "$(date +%s)" -lt "$deadline" ] && ! trusted; do
    hops=$((hops + 1))
    next="$WORK/issuer-$hops.pem"

    # Try every caIssuers URI rather than only the first: a mirror can be down. caIssuers is usually
    # DER, but PEM and PKCS#7 are also served in the wild.
    for url in $(issuer_urls "$last"); do
        curl -fsSL --proto '=http,https' --proto-redir '=http,https' \
            --retry 1 --max-time 10 "$url" -o "$WORK/fetched" || continue

        if openssl x509 -inform DER -in "$WORK/fetched" -out "$next" 2>/dev/null \
            || openssl x509 -in "$WORK/fetched" -out "$next" 2>/dev/null \
            || openssl pkcs7 -inform DER -in "$WORK/fetched" -print_certs -out "$next" 2>/dev/null; then
            break
        fi
    done

    [ -s "$next" ] || break

    cat "$next" >> "$WORK/chain.pem"
    last="$next"
    added=$((added + 1))
done

# Written either way: whatever we have is at least what the pfx already carried.
cp "$WORK/chain.pem" "$OUT"

if trusted; then
    [ "$added" -eq 0 ] || echo "cert-chain: added $added issuer(s) to the chain nginx serves."
else
    echo "cert-chain: could not build a trusted chain for $(basename "$PFX")." >&2
fi
