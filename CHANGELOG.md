## Unreleased

- `Device` (the controller role, from `client.device(id)`) is renamed to
  `DeviceRemote`, so it is not confused with `DeviceEndpoint`. `Device`
  still works as a deprecated alias and will be removed in 0.3.0.
- **Fix: a command that timed out could still be sent later.**
  `send()` used to put the command in the offline queue while offline;
  after `CommandTimeoutException` it stayed there and was sent on the next
  connect (a relay switching long after the app said "failed"). Commands
  are now never queued: `send()` waits for the connection only until its
  `timeout`, and a command that timed out is never sent again. `timeout`
  now covers the whole round trip, including the broker's confirmation,
  so `send()` no longer outlives it on a dead link.
- **Fix: `disconnect()` while `connect()` was in progress was ignored.** The
  connect finished and the client ended up `Connected`. Now the connect is
  cancelled: it throws `ConnectionException`, the connection is closed,
  and a device does not go online. `disconnect()` also waits for a
  reconnect attempt already on the wire, so nothing reconnects after it
  returns.
- **Fix: a device could stay "online" after `disconnect()`.** If it was
  called while a reconnect was still restoring, the reconnect's "online"
  could go out after the "offline". `disconnect()` now lets it go first.
- **Changed: telemetry and status are no longer queued while offline.**
  New `Topic.offline` (`OfflinePolicy.queue`, `drop`, `fail`).
  `BedayaTopics.telemetry` and `status` use `drop`, `command` uses `fail`,
  `commandAck` and your own topics keep `queue` (the default). If you relied
  on queued telemetry, define your own topic with `OfflinePolicy.queue`.
  Only `queue` topics wait behind older queued messages.
- Calling `connect()` again while it is in progress returns the same
  future, instead of completing at once before the client is connected.
- New `publish(..., deadline:)` and `PublishDeadlineException` (a
  `ConnectionException`): never queued, never sent after the deadline.
- Docs: new "Delivery guarantees" section.

## 0.1.2

- pubspec description now starts with "Flutter & Dart", so the package
  shows up when people search pub.dev for Flutter MQTT.
- No changes to the library.

## 0.1.1

- README: the quick start warns that `broker.hivemq.com` is shared by
  everyone and uses a personal `topicPrefix`, so people trying it at the
  same time don't see each other's devices.
- `example/simulated_device.dart` and `example/controller.dart` default to
  the prefix `bedaya-<your user name>` instead of `bedaya`.
- README: "About Bedaya" now says Bedaya is a Flutter app, not hardware.
- No changes to the library.

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
