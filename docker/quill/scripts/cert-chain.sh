#!/bin/sh
set -e

# Let's Encrypt sends the issuers along with the leaf, but the PFX RavenDB writes keeps only the
# leaf, so clients get no path to a trusted root. Rebuild the PFX with the issuers fetched from
# each certificate's Authority Information Access extension.

PFX="$1"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

certs() {
    openssl pkcs12 -in "$PFX" -nokeys -passin pass: 2>/dev/null | grep -c 'BEGIN CERTIFICATE'
}

trusted() {
    openssl verify -untrusted "$WORK/chain.pem" "$WORK/leaf.pem" >/dev/null 2>&1
}

issuer_url() {
    openssl x509 -in "$1" -noout -text | sed -n 's#.*CA Issuers - URI:##p' | head -n1
}

if [ "$(certs)" -gt 1 ]; then
    exit 0
fi

openssl pkcs12 -in "$PFX" -nokeys -clcerts -passin pass: -out "$WORK/leaf.pem"
openssl pkcs12 -in "$PFX" -nocerts -nodes  -passin pass: -out "$WORK/key.pem"

touch "$WORK/chain.pem"
last="$WORK/leaf.pem"
hops=0

# Stopping on `trusted` rather than on a known issuer name keeps this working when Let's Encrypt
# rotates intermediates, and drops the extra hop once their new root ships in trust stores.
while [ "$hops" -lt 4 ] && ! trusted; do
    url="$(issuer_url "$last")"
    [ -n "$url" ] || break

    hops=$((hops + 1))
    curl -fsS --max-time 15 "$url" -o "$WORK/issuer.der" || break
    openssl x509 -inform DER -in "$WORK/issuer.der" -out "$WORK/issuer-$hops.pem"

    cat "$WORK/issuer-$hops.pem" >> "$WORK/chain.pem"
    last="$WORK/issuer-$hops.pem"
done

if ! trusted; then
    echo "cert-chain: could not build a trusted chain; leaving $(basename "$PFX") as it is." >&2
    exit 0
fi

openssl pkcs12 -export -passout pass: \
    -inkey "$WORK/key.pem" -in "$WORK/leaf.pem" -certfile "$WORK/chain.pem" \
    -out "$WORK/new.pfx"

cp "$WORK/new.pfx" "$PFX.tmp"
chmod 600 "$PFX.tmp"
mv "$PFX.tmp" "$PFX"

echo "cert-chain: added $hops issuer(s) to $(basename "$PFX")."
