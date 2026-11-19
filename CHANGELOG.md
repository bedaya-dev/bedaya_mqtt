## 0.2.2

- **Clearing a retained topic is no longer an error.** An empty payload on
  a retained topic (the MQTT way to clear it, for example the `status` of
  a removed device) used to be reported as a `DecodeException`. It is now
  skipped quietly. On other topics an empty payload is still reported.
- README: a diagram of how a reading travels; `homepage` set to
  bedaya.dev; `doc/` is no longer shipped in the package.
- **Refused subscriptions are reported.** If the broker refuses a
  subscription (answers with SUBACK 0x80), a new `SubscriptionException`
  goes to `client.errors`, with the filter, the reason, and the `context`:
  first connect, reconnect, or a new listener. Before, the stream silently
  got nothing. The stream itself never gets an error event. The client asks
  again after every reconnect, so fixing the ACL is enough. Mosquitto
  (checked with 2.1.2) accepts a subscription its ACL refuses and drops the
  messages instead, so there is nothing to report there.
- **Subscriptions wait for the broker's answer (SUBACK).** After a
  reconnect, commands wait until every restored subscription is confirmed
  or refused, not just requested. The restore sends all requests at once
  and then waits, instead of one round trip per subscription.
- **`send()` waits for its ack subscription.** Fixed a race: a command
  could go out before the broker had confirmed the subscription to its
  ack topic, so a fast ack could be missed. Now `send()` waits for that
  SUBACK first. If the broker refuses it, `send()` throws
  `SubscriptionException` at once and the command is not sent. The ack
  subscription is kept per device after the first command (until
  `close()`), so later commands don't wait for it again.
- `SubscriptionException` docs: how to tell a refusal (`reason`
  "rejected by the broker") from a request that could not be sent
  (`cause` set). `client.errors` docs list it.
- Tests: a raw MQTT stub server checks that a real SUBACK 0x80 reaches
  `MqttClientTransport` and `client.errors` (no Docker needed).
- `MqttTransport.subscribe` now completes on the broker's answer and
  throws `SubscriptionException` on a refusal. Custom transports should
  do the same.
- Testing: `MemoryBroker.rejectSubscription` refuses chosen filters, like
  an ACL.

## 0.2.1

- 0.2.0 was tagged in git but never published on pub.dev; 0.2.1 contains
  everything listed under it.
- Works with Dart 3.8 and later (was 3.13), so Flutter projects on older
  SDKs can use it. CI now runs the tests on Dart 3.8.0 too.
- README: new Security section (logins, a Mosquitto ACL per device, TLS,
  checking commands on the device); the Errors table covers `publish()`
  and `send()` fully; the Last Will delay; one device per client; a
  complete testing example.
- No behavior changes.

## 0.2.0

Reliability on bad networks. Read the "Behavior changes" first: three of
them can change what your app sees.

### Fixes

- **A dead link is now noticed.** When a network drops without closing
  the connection (WiFi lost, phone moving from WiFi to 4G), the client
  used to stay `Connected` for many minutes, until the operating system
  gave up, so it never reconnected and never queued. It now ends the
  connection when a ping gets no answer within one more `keepAlive`
  (about 2 × `keepAlive` in total), then reconnects as usual.
- **QoS 1 and 2 messages are no longer lost on a dropped connection.**
  Messages sent just before a silent drop were reported as sent but never
  reached the broker. They now go back into the offline queue and are sent
  again after the reconnect. A message can therefore arrive twice, which
  is what QoS 1 means; `Command.id` lets a device ignore a repeat.
- **A command that timed out could still be sent later.** `send()` used
  to put the command in the offline queue while offline; after
  `CommandTimeoutException` it stayed there and was sent on the next
  connect (a relay switching long after the app said "failed"). Commands
  are now never queued: `send()` waits for the connection only until its
  `timeout`, and a command that timed out is never sent again.
- **`disconnect()` while `connect()` was in progress was ignored.** The
  connect finished and the client ended up `Connected`. Now the connect is
  cancelled: it throws `ConnectionException`, the connection is closed,
  and a device does not go online. `disconnect()` also waits for a
  reconnect attempt already on the wire, so nothing reconnects after it
  returns.
- **`disconnect()` could be undone by a dropped connection.** If the
  connection dropped while `disconnect()` waited for a connect that was
  still sending the offline queue, the client reconnected after
  `disconnect()` returned. Now a drop during `disconnect()` never starts a
  reconnect, and `disconnect()` no longer waits for the queue: it closes
  the connection, and what was not sent stays queued for the next
  `connect()`.
- **A device could stay "online" after `disconnect()`.** `disconnect()`
  now sends `offline` straight to the broker (never through the queue,
  where it could arrive after the next `online`), waits at most 2 seconds
  for its ack, and first lets a reconnect that is still restoring send its
  `online`.
- Order is kept: while the offline queue is being sent, new messages on
  queued topics wait behind it instead of overtaking it ("on" then "off"
  can no longer arrive as "off" then "on").

### Behavior changes

- **`publish()` waits for the broker.** For QoS 1 and 2, the future now
  completes when the broker confirms the message (PUBACK or PUBCOMP),
  instead of as soon as the bytes are written. QoS 0 is unchanged. Code
  that awaits `publish()` in a tight loop for QoS 1 will be slower by one
  network round trip per message.
- **Telemetry and status are no longer queued while offline.** New
  `Topic.offline` (`OfflinePolicy.queue`, `drop`, `fail`).
  `BedayaTopics.telemetry` and `status` use `drop`, `command` uses `fail`,
  `commandAck` and your own topics keep `queue` (the default). If you relied
  on queued telemetry, define your own topic with `OfflinePolicy.queue`.
- **`send()`'s `timeout` covers the whole round trip**: waiting for the
  connection, the broker's confirmation, and the device's ack. A
  `CommandTimeoutException` means the outcome is unknown, not that nothing
  happened.
- `Device` (the controller role, from `client.device(id)`) is renamed to
  `DeviceRemote`, so it is not confused with `DeviceEndpoint`. `Device`
  still works as a deprecated alias and will be removed in 0.3.0.
- Calling `connect()` again while it is in progress returns the same
  future, instead of completing at once before the client is connected.
- `connect()` completes once the connection setup is done: subscriptions
  restored and one attempt at sending the offline queue made. If the
  connection drops during that, `connect()` still completes, what was not
  sent stays queued, and the client reconnects by itself; watch
  `connectionStates` for the current state.

### Other

- New `publish(..., deadline:)` and `PublishDeadlineException` (a
  `ConnectionException`): never queued, never sent after the deadline.
- Docs: "Delivery guarantees" and "Common mistakes" sections in the README.
- Examples use concrete names (`living-room-fan`, `fanRemote`).
- Integration tests: `tool/mosquitto.sh` also starts a Toxiproxy, and
  `test/integration/reliability_test.dart` covers a silent drop, no lost
  messages, order, and a single `offline` on disconnect. Run them with
  `--concurrency=1`, because some tests restart the shared broker.
- CI on GitHub Actions: unit tests, analyze and format, and the
  integration tests against Mosquitto and Toxiproxy.

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
