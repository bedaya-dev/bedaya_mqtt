# How bedaya_mqtt works

A map of the code for anyone who wants to understand it, debug it, or
change it. Each section follows one thing the package does, step by step,
with the file and function where it happens.

## The layers

```text
 Your app
    │
    ▼
 device/       DeviceRemote (controller role), DeviceEndpoint (device role), DeviceRegistry
    │          thin wrappers that fill {deviceId} and call the client
    ▼
 client/       BedayaClient: prefix, encode/decode, state, errors, tracing
    │   ├── subscriptions/   SubscriptionRegistry: who listens to which filter
    │   ├── reconnect/       ReconnectManager + ReconnectPolicy: retry with backoff
    │   └── queue/           MessageQueue / MemoryQueue: messages waiting while offline
    ▼
 transport/    MqttTransport interface: connect, subscribe, publish, raw bytes
    │   ├── MqttClientTransport   the real one, on package:mqtt_client
    │   └── MemoryTransport       a fake broker in memory, for tests
    ▼
 the broker (Mosquitto, HiveMQ, …)
```

Two more folders hold definitions, not behavior:

- `topic/`: `Topic<T>` (pattern + codec + QoS + retained), `TopicPattern`
  (`{param}` templates), `MessageCodec` (bytes ↔ values).
- `contract/`: the standard Bedaya topics and their message classes
  (`Telemetry`, `DeviceStatus`, `Command`, `CommandAck`).

Rules that keep this simple:

- Only `transport/` imports `mqtt_client`. Everything above uses
  `RawMessage` and `Qos`.
- The transport is dumb on purpose: no reconnect, no queue, and it forgets
  subscriptions when it drops. The client does all of that.
- Topics in the code have no prefix (`{deviceId}/telemetry`). The client
  adds `ClientConfig.topicPrefix` on the way out and strips it on the way
  in. `RawMessage.topic` is always the full topic.

## Trace it yourself

The fastest way to see what happens is the built-in trace:

```dart
BedayaClient(ClientConfig(host: 'broker.hivemq.com', logger: print));
```

```text
12:00:01.204 [info] connecting to broker.hivemq.com
12:00:01.391 [info] connected
12:00:01.392 [debug] subscribe bedaya/+/telemetry (atMostOnce)
12:00:03.410 [debug] received bedaya/a1/telemetry {"temp":24.5}
12:00:09.001 [warning] connection lost
12:00:09.002 [info] reconnect #1 in 0:00:01.000000
12:00:10.120 [info] connected
12:00:10.121 [debug] restoring 1 subscriptions
```

Every line comes from a `_log(...)` call in `bedaya_client.dart`, so you can
search for its text to find where it happens.

To see the raw traffic on the broker side, run a local broker
(`tool/mosquitto.sh start`) and watch everything:

```sh
docker exec -it bedaya-mosquitto mosquitto_sub -t '#' -v
```

## Flow 1: connecting

`await client.connect()`

1. `BedayaClient.connect` (`client/bedaya_client.dart`): if a connect is
   already in progress, returns that same future (`_connecting`), so every
   caller waits for the same result. Returns at once if already connected
   or reconnecting. Otherwise sets `Connecting`.
2. `MqttTransport.connect(lastWill: ...)`. The Last Will is only set in the
   device role (see Flow 5).
   - Real: `MqttClientTransport.connect` builds the MQTT CONNECT message
     (clean session, MQTT 3.1.1, the will), then calls
     `mqtt_client.connect` inside `_guarded` (catches socket errors
     mqtt_client leaves unhandled). A failure becomes a
     `ConnectionException`, with the broker's reason if it refused
     (`notAuthorized`, …).
   - Which real transport: `transport/transport_factory.dart` picks
     `_io.dart` (TCP, TLS, WebSocket) or `_web.dart` (browser WebSocket) at
     compile time.
3. If it fails: `Failed` and throw. Or, with `retryFirstConnect`,
   `_retryFirstConnect` starts the `ReconnectManager` (Flow 4) and waits.
4. If it works: `_onConnected`, which runs after **every** connect:
   1. `Connected`.
   2. Subscribe to every filter in `SubscriptionRegistry.filters` (the
      broker forgot them; clean session). All requests go out at once,
      then it waits until the broker has answered each (SUBACK). A refused
      one becomes a `SubscriptionException` on `client.errors` (context
      `firstConnect` or `reconnect`) and stays registered, so the next
      reconnect asks again; the others carry on.
   3. Device role: publish `{"online": true}` (retained), straight to the
      transport, never through the queue. Then `_ready` completes:
      commands (`_publishBefore`) and `disconnect()` wait for this point,
      not for the queue.
   4. Send what's in the offline queue: `_drainQueue` →
      `MessageQueue.drain`, oldest first, waiting for the broker's ack of
      each QoS 1/2 message. `_draining` is true meanwhile (see Flow 2).

   A drop during these steps is caught; the next reconnect finishes the
   job.
5. `disconnect()` while this runs (before `Connected`) cancels it: it sets
   `_connectCancelled` and `Disconnected`, then waits for the attempt. The
   attempt sees the flag after the transport answers, closes the
   connection without going online, and `connect()` throws
   `ConnectionException`. After `Connected` (restoring or sending the
   queue), `disconnect()` only waits for `_ready`, then closes the
   connection: the drain fails at once, what it had not sent stays
   queued, and `connect()` completes.

## Flow 2: publishing

`await client.publish(BedayaTopics.telemetry, Telemetry({...}), params: {'deviceId': 'a1'})`

1. `BedayaClient.publish` → `_encode`:
   - `TopicPattern.resolve(params)`: `{deviceId}/telemetry` → `a1/telemetry`
     (throws if a param is missing or has `/`, `+`, `#`).
   - `_addPrefix`: → `bedaya/a1/telemetry`.
   - `topic.codec.encode(value)`: `Telemetry` → JSON → UTF-8 bytes.
   - QoS and retained come from the `Topic`.
2. With a `deadline` (what `DeviceRemote.send` uses): never queue. If not
   connected, wait for `Connected` and for `_ready` (subscriptions back
   and "online" sent, but not the queue), then publish and wait for the
   broker's confirmation (step 3), all within the deadline. If the
   deadline passes first, throw `PublishDeadlineException`; the message is
   never sent again.
3. If `Connected` and nothing older is waiting (queue empty, not
   `_draining`; only checked for `queue` topics):
   `MqttTransport.publish(rawMessage)`.
   - Real: `MqttClientTransport.publish` hands it to
     `mqtt_client.publishMessage`, which returns a message id at once.
     QoS 0: done. QoS 1/2: a `Completer` goes into `_pendingAcks[id]`, and
     the future completes when that id comes back on mqtt_client's
     `published` stream (PUBACK for QoS 1, PUBCOMP for QoS 2), in
     `_onAck`.
   - If the connection ends first, `_cleanUp` fails every pending
     completer with a `ConnectionException`. mqtt_client itself forgets
     unconfirmed messages when a clean session ends, so this is the only
     place they can be saved.
4. Otherwise (offline, older messages waiting, or step 3 threw because the
   connection ended), the topic's `OfflinePolicy` decides: `drop` returns,
   `fail` throws `ConnectionException`, `queue` puts it at the end of the
   queue (`MessageQueue.enqueue`). The trace says which case it was. If
   the queue was full, the oldest message is dropped and a
   `MessageDroppedException` goes to `client.errors`. With the queue off
   (`maxQueueSize: 0`), throw `ConnectionException`.
5. If connected but no drain is running, start one (`_drainQueue`).

Why "nothing older is waiting" matters: after a reconnect the queue is
sent one confirmed message at a time. A new message sent straight away
would overtake the queued ones, and "on" then "off" could arrive as "off"
then "on". `drop` and `fail` topics (telemetry, commands) don't wait:
their order relative to queued messages doesn't matter, and a command
must not sit behind a long queue.

## Flow 3: subscribing and receiving

`client.device('a1').on(BedayaTopics.telemetry).listen(...)`

Subscribing (when you call `listen`):

1. `DeviceRemote.on` → `BedayaClient.subscribe(topic, params: {'deviceId': 'a1'})`.
2. The filter: `TopicPattern.filter(params)` → `a1/telemetry` (a missing
   param would be `+`), then the prefix → `bedaya/a1/telemetry`.
3. The stream is lazy. Its `onListen`:
   - listens to `MqttTransport.messages` (every incoming message);
   - `_addListener` → `SubscriptionRegistry.add`. Only the **first**
     listener of a filter (or a higher QoS) subscribes for real, and only if
     connected. Otherwise `_onConnected` does it later. A refusal goes to
     `client.errors` (context `newListener`), never to the stream: an
     error event on a stream listened to without `onError` would crash a
     CLI program.
   - Real: `MqttClientTransport.subscribe` completes on the broker's SUBACK
     (`onSubscribed`), throws `SubscriptionException` on a refusal
     (`onSubscribeFail`), and `ConnectionException` if the connection ends
     first (`_cleanUp`). A filter already confirmed on this connection
     completes at once, and one already requested waits for that same
     answer: `mqtt_client` sends no second request for either, so no
     second SUBACK would come.
4. `cancel()` → `_removeListener` → `SubscriptionRegistry.remove`. The
   **last** listener unsubscribes.

Receiving (for each message from the broker):

1. The transport emits a `RawMessage` on `messages`.
   - Real: `MqttClientTransport._onUpdates` copies the bytes from
     mqtt_client's message.
2. Every open stream's `onMessage` (inside `BedayaClient.subscribe`) checks
   it:
   - `_removePrefix`: not our prefix → ignore.
   - `TopicPattern.match`: not this topic → ignore. Otherwise it gives the
     params, like `{deviceId: a1}`.
   - Fixed params must match (`deviceId == 'a1'`).
   - `topic.codec.decode(payload)`. On failure: skip the message, send a
     `DecodeException` to `client.errors`, keep the stream going.
3. The stream gets a `TopicMessage` (value + params + topic + retained).
   `DeviceRemote.on` maps it to just the value.

## Flow 4: the connection drops

1. The transport sets its state to `disconnected`.
   - Real: mqtt_client calls `onDisconnected`
     → `MqttClientTransport._onDisconnected`. That happens when the socket
     closes, or when a keepAlive ping gets no answer within one more
     `keepAlive` (`disconnectOnNoResponsePeriod`, set in the
     `MqttClientTransport` constructor). The second case is what catches
     a link that died without closing (lost WiFi): without it the client
     stays "connected" until the operating system gives up, which takes
     many minutes.
   - Unconfirmed QoS 1/2 publishes fail and go back to the queue
     (Flow 2, step 3).
   - Test: `MemoryTransport.drop()`.
2. `BedayaClient._onTransportState` sees it. It's a real drop if we were
   `Connected` and not inside `disconnect()`.
3. `ReconnectManager.start` (`reconnect/reconnect_manager.dart`):
   1. `onRetryScheduled(attempt, delay)` → state `Reconnecting`.
      The delay is `ReconnectPolicy.delayFor(attempt)`
      (`reconnect/backoff.dart`): exponential, capped, with jitter.
   2. After the delay, `connect()` → `MqttTransport.connect`.
   3. Fails: `onAttemptFailed`, then the next attempt (or `onGaveUp` →
      `Failed` after `maxAttempts`).
   4. Works: `onReconnected` → `_onConnected` (Flow 1, step 4):
      resubscribe, online, send the queue.
4. `disconnect()` sets `_disconnecting` on its first line, so a drop
   while it waits is never taken for a reason to reconnect (step 2), and
   calls `ReconnectManager.stop` both first and last. A connect that
   finishes after that is closed again (`abandon`), and `disconnect()`
   waits for that (`ReconnectManager.settled`), so when it returns nothing
   is connected and nothing will reconnect.

## Flow 5: being a device (online, offline, Last Will)

`final me = client.asDevice('a1'); await client.connect();`

1. `BedayaClient.asDevice` (must be before `connect`): builds the Last Will,
   `{"online": false}` on `bedaya/a1/status`, retained, and keeps it in
   `_lastWill`. Returns a `DeviceEndpoint`.
2. `connect` passes `_lastWill` to the transport, which puts it in the
   CONNECT message. The **broker** now holds it.
3. `_onConnected` publishes `{"online": true}` (retained).
4. Leaving:
   - `disconnect()`: the client publishes `{"online": false}` itself,
     straight to the transport, and waits at most 2 seconds for the ack,
     then disconnects. (The broker never sends the will after a clean
     disconnect.) It never goes through the queue: a queued "offline"
     would be sent after the next connect's "online". If the link is
     already dead, the will covers it.
   - Power cut, lost WiFi, killed process: the broker notices (the socket
     closes, or no ping within 1.5 × `keepAlive`) and publishes the will.

## Flow 6: a command and its ack

Controller: `await client.device('a1').send(Command.relay(on: true))`

1. `DeviceRemote.send` (`device/device.dart`) first listens on
   `bedaya/a1/command/ack` (so a fast answer is not missed).
2. Publishes the command on `bedaya/a1/command` with a deadline of now +
   `timeout` (Flow 2, step 2). It carries a random `id`. It is never put in
   the offline queue: if the client is offline it waits for the
   connection, and if the deadline passes first it throws
   `CommandTimeoutException`. A command that timed out is never sent
   later.
3. Device side: `DeviceEndpoint.commands` delivers it; the app calls
   `me.ack(command)` → publishes `{"id": <same id>, "ok": true}` on the ack
   topic.
4. `DeviceRemote.send` gets an ack with the same id → returns it. No ack
   before the deadline → `CommandTimeoutException`. Either way it stops
   listening.

## Flow 7: all devices (DeviceRegistry)

`client.devices` creates one `DeviceRegistry` (`device/device_registry.dart`)
that listens to the status, telemetry and command ack topics of **all**
devices (`+` for `deviceId`). For each message it updates a `DeviceInfo`:

- `online`: from the status payload; any live telemetry or ack also means
  online.
- `lastSeen`: the time a **live** message arrived. A retained status
  (maybe old) leaves it unchanged.

## Where to look when…

| Symptom | Look at |
|---|---|
| `connect()` fails | The trace's `connect failed (...)` line, and `ConnectionException.cause`. TLS problems show a `HandshakeException`. |
| Messages don't arrive | Trace `subscribe ...` and `received ...` lines. Same `topicPrefix` on both sides? `mosquitto_sub -t '#' -v` shows what the broker really gets. |
| A stream gets nothing, but the data is there | `client.errors`: a `DecodeException` means the payload doesn't fit the topic's type. |
| Status stays `online` after a device died | The broker waits 1.5 × `keepAlive` before sending the will. |
| Two clients keep reconnecting | Same `clientId` on both: the broker kicks one off each time. |
| `send()` times out | The device must call `me.ack(command)`. Is it online? Same prefix? |
| Old messages arrive after a reconnect | The offline queue. Use `queueTtl`, or `OfflinePolicy.drop` on that topic. |
| The same QoS 1 message arrives twice | Expected after a dropped connection: its ack was lost, so it was sent again. Trace: `not confirmed before the connection ended`. |
| `await publish()` takes a while | QoS 1/2 waits for the broker's ack: one round trip, or until the link is found dead (about 2 × `keepAlive`). |
| Still "connected" on a dead network | `keepAlive` too long, or `disconnectOnNoResponsePeriod` not set (`MqttClientTransport` constructor). |

## Tests

| Folder | What |
|---|---|
| `test/topic/`, `test/contract/` | Pure logic: patterns, codecs, messages. |
| `test/transport/` | `MemoryTransport` and topic filters. |
| `test/client/` | The client on `MemoryTransport`: connect, prefix, subscribe, reconnect, queue, tracing. |
| `test/device/` | Controller and device talking through the memory broker. |
| `test/integration/` | A real Mosquitto in Docker, plus a Toxiproxy in front of it for bad-network tests (`reliability_test.dart`): `tool/mosquitto.sh start`, then `BEDAYA_MOSQUITTO=1 dart test test/integration --concurrency=1` (one file at a time, because some tests restart the shared broker). And a real browser (`dart test -p chrome test/integration/web_test.dart`). |
