## 0.1.0

First release.

- `BedayaClient`: connect over TCP, TLS or WebSocket (native), or
  WebSocket in the browser, on top of `mqtt_client`.
- Typed topics: `Topic<T>` with `{param}` patterns, `MessageCodec<T>` and
  `Topic.json(...)`. A configurable topic prefix (`ClientConfig.topicPrefix`).
- Two roles: controller (`client.device(id)`: listen, `send` a command and
  wait for its ack) and device (`client.asDevice(id)`: publish, receive
  commands, `ack`), with online/offline status and a Last Will.
- `client.devices`: every device's online status and last seen time.
- Automatic reconnect with exponential backoff and jitter
  (`ReconnectPolicy`), resubscribing everything after a reconnect.
  Optional retries for the first connect (`retryFirstConnect`).
- Offline queue: messages published while offline are sent after
  reconnecting (`MemoryQueue`, bounded, optional TTL, pluggable
  `MessageQueue`).
- TLS with a private CA (`caCertificate`) and client certificates
  (`clientCertificate`, `clientKey`).
- Bad payloads are skipped and reported on `client.errors` instead of
  breaking streams.
- `package:bedaya_mqtt/testing.dart`: `MemoryBroker` and `MemoryTransport`
  for fast tests without a network.
- Tracing: `ClientConfig.logger` receives a `BedayaLogRecord` for every
  step (connect, subscribe, each message, queue, reconnect).
- Every public API is documented, and `doc/ARCHITECTURE.md` explains how
  the parts work together.
