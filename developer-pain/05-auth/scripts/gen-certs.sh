#!/usr/bin/env bash
# Creates a private CA and a server certificate for the Iggy container.
# The server certificate names "iggy" (the compose service), "localhost" and 127.0.0.1,
# so clients inside and outside Docker can verify it.
set -euo pipefail
cd "$(dirname "$0")/../certs"

openssl req -x509 -newkey rsa:4096 -sha256 -days 825 -nodes \
  -keyout ca.key -out ca.pem -subj "/CN=iggy-secure-starter CA"

openssl req -newkey rsa:2048 -sha256 -nodes \
  -keyout server.key -out server.csr -subj "/CN=iggy"

cat > server.ext <<EXT
subjectAltName = DNS:iggy, DNS:localhost, IP:127.0.0.1
extendedKeyUsage = serverAuth
EXT

openssl x509 -req -in server.csr -CA ca.pem -CAkey ca.key -CAcreateserial \
  -out server.pem -days 397 -sha256 -extfile server.ext

rm -f server.csr server.ext ca.srl
chmod 600 ca.key server.key
echo "Wrote certs/ca.pem (give to clients) and certs/server.pem + server.key (server only)."
