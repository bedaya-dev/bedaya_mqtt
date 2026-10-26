#!/usr/bin/env bash
# Starts or stops a local Mosquitto broker in Docker for integration tests.
#
#   tool/mosquitto.sh start   # then: BEDAYA_MOSQUITTO=1 dart test test/integration
#   tool/mosquitto.sh stop
#
# Ports:
#   18830 TCP             18831 TCP + login (bedaya/secret)
#   18080 WebSocket       18081 secure WebSocket
#   18883 TLS             18884 TLS + client certificate
#
# TLS uses a private CA made here (tool/mosquitto/certs/, git-ignored).
set -euo pipefail

NAME=bedaya-mosquitto
DIR="$(cd "$(dirname "$0")/mosquitto" && pwd)"
CERTS="$DIR/certs"

make_certs() {
  [ -f "$CERTS/client.key" ] && return
  mkdir -p "$CERTS"
  cd "$CERTS"
  # Our own CA.
  openssl req -x509 -newkey rsa:2048 -nodes -days 3650 \
    -keyout ca.key -out ca.crt -subj "/CN=bedaya-test-ca" 2>/dev/null
  # Server certificate for localhost, signed by the CA.
  openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr \
    -subj "/CN=localhost" 2>/dev/null
  printf 'subjectAltName=DNS:localhost,IP:127.0.0.1\n' > server.ext
  openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -days 3650 -out server.crt -extfile server.ext 2>/dev/null
  # Client certificate for mutual TLS, signed by the same CA.
  openssl req -newkey rsa:2048 -nodes -keyout client.key -out client.csr \
    -subj "/CN=bedaya-test-client" 2>/dev/null
  openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
    -days 3650 -out client.crt 2>/dev/null
  rm -f ./*.csr server.ext
  # The broker runs as another user inside the container.
  chmod 644 ./*
  cd - >/dev/null
}

case "${1:-}" in
  start)
    make_certs
    docker rm -f "$NAME" >/dev/null 2>&1 || true
    rm -f "$DIR/passwd"
    docker run --rm -v "$DIR:/work" eclipse-mosquitto:2 \
      sh -c 'mosquitto_passwd -b -c /work/passwd bedaya secret && chmod 644 /work/passwd'
    docker run -d --name "$NAME" \
      -p 18830:1883 -p 18831:1884 -p 18080:8080 -p 18081:8081 \
      -p 18883:8883 -p 18884:8884 \
      -v "$DIR:/mosquitto/config" eclipse-mosquitto:2 >/dev/null
    echo "Mosquitto running as '$NAME'."
    ;;
  stop)
    docker rm -f "$NAME" >/dev/null
    echo "Stopped."
    ;;
  *)
    echo "usage: $0 start|stop" >&2
    exit 1
    ;;
esac
