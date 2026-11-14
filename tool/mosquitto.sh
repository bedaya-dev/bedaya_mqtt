#!/usr/bin/env bash
# Starts or stops a local Mosquitto broker in Docker for integration tests,
# plus a Toxiproxy in front of it to simulate a bad network.
#
#   tool/mosquitto.sh start   # then: BEDAYA_MOSQUITTO=1 dart test test/integration
#   tool/mosquitto.sh stop
#
# Ports (on 127.0.0.1 only; BEDAYA_BIND=0.0.0.0 opens them to the LAN):
#   18830 TCP             18831 TCP + login (bedaya/secret)
#   18080 WebSocket       18081 secure WebSocket
#   18883 TLS             18884 TLS + client certificate
#   18890 TCP through Toxiproxy (the tests create this proxy)
#    8474 Toxiproxy API
#
# TLS uses a private CA made here (tool/mosquitto/certs/, git-ignored).
set -euo pipefail

NAME=bedaya-mosquitto
PROXY=bedaya-toxiproxy
NETWORK=bedaya-test
TOXIPROXY_IMAGE=ghcr.io/shopify/toxiproxy:2.9.0
DIR="$(cd "$(dirname "$0")/mosquitto" && pwd)"
CERTS="$DIR/certs"
BIND="${BEDAYA_BIND:-127.0.0.1}"

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

stop_all() {
  docker rm -f "$NAME" "$PROXY" >/dev/null 2>&1 || true
  docker network rm "$NETWORK" >/dev/null 2>&1 || true
}

case "${1:-}" in
  start)
    make_certs
    stop_all
    # A network of our own, so Toxiproxy can reach the broker by name.
    docker network create "$NETWORK" >/dev/null
    rm -f "$DIR/passwd"
    docker run --rm -v "$DIR:/work" eclipse-mosquitto:2 \
      sh -c 'mosquitto_passwd -b -c /work/passwd bedaya secret && chmod 644 /work/passwd'
    docker run -d --name "$NAME" --network "$NETWORK" \
      -p "$BIND:18830:1883" -p "$BIND:18831:1884" \
      -p "$BIND:18080:8080" -p "$BIND:18081:8081" \
      -p "$BIND:18883:8883" -p "$BIND:18884:8884" \
      -v "$DIR:/mosquitto/config" eclipse-mosquitto:2 >/dev/null
    # Sits between the client and the broker. The tests tell it to drop
    # traffic without closing the connection, like a dead WiFi link.
    docker run -d --name "$PROXY" --network "$NETWORK" \
      -p "$BIND:8474:8474" -p "$BIND:18890:18890" \
      "$TOXIPROXY_IMAGE" -host=0.0.0.0 >/dev/null
    echo "Mosquitto running as '$NAME', Toxiproxy as '$PROXY'."
    ;;
  stop)
    stop_all
    echo "Stopped."
    ;;
  *)
    echo "usage: $0 start|stop" >&2
    exit 1
    ;;
esac
