# bedaya_mqtt

Typed, reliable MQTT for Dart: reconnect, offline queue, typed topics.
Built by Bedaya.

`bedaya_mqtt` sits on top of [`mqtt_client`](https://pub.dev/packages/mqtt_client)
and adds the parts every IoT app ends up writing by hand:

- **Automatic reconnect** with exponential backoff and jitter, and every
  subscription restored afterwards.
- **Offline queue**: publish while offline, and messages are sent once the
  connection is back. Per topic you choose queue, drop or fail, so stale
  readings and late commands are never replayed.
- **Typed topics**: `Topic<T>` with `{param}` patterns. You get `Telemetry`,
  not bytes.
- **Two roles**: a *controller* that watches devices and sends commands, and
  a *device* that publishes data and answers them, with online/offline
  status and a Last Will.
- **Clean streams**: a bad payload from someone else on the broker is skipped
  and reported, and your stream keeps going.
- **Everywhere Dart runs**: Flutter (mobile and desktop), CLI scripts,
  servers and the web. Pure Dart, no Flutter dependency.
- **TLS**, including private CAs and client certificates.
- **Test helpers**: an in-memory broker, so your tests need no network.

## Install

```sh
dart pub add bedaya_mqtt
```

## Quick start

These snippets use `broker.hivemq.com`, a free public broker that **everyone
shares**. Pick your own `topicPrefix` (your name, or anything unique), and use
the same one in the controller and the device. Otherwise you may see other
people's devices, and they may switch your relay.

### Controller: watch a device and send it a command

```dart
import 'package:bedaya_mqtt/bedaya_mqtt.dart';

final client = BedayaClient(
  ClientConfig(host: 'broker.hivemq.com', topicPrefix: 'bedaya-yourname'),
);
await client.connect();

// A remote for one device: watch it and send it commands.
final fanRemote = client.device('living-room-fan');
fanRemote.status.listen((s) => print(s.online ? 'online' : 'offline'));
fanRemote.on(BedayaTopics.telemetry).listen((t) => print('temp: ${t['temp']}'));

final ack = await fanRemote.send(Command.relay(on: true));
print(ack.ok ? 'fan is on' : 'failed: ${ack.error}');
```

### Device: publish data and answer commands

```dart
final client = BedayaClient(
  ClientConfig(host: 'broker.hivemq.com', topicPrefix: 'bedaya-yourname'),
);

// This program *is* the fan. Call asDevice before connect: it sets the
// Last Will, so the broker reports the fan offline if it drops off.
final fan = client.asDevice('living-room-fan');
await client.connect();

fan.commands.listen((command) {
  if (command.name == 'relay') {
    final on = command.params['on'] as bool;
    // ... switch the relay ...
    fan.ack(command);
  } else {
    fan.ack(command, error: 'unknown command');
  }
});

await fan.publish(BedayaTopics.telemetry, Telemetry({'temp': 24.5}));
```

The client publishes `online` when it connects and `offline` when it
disconnects. If the connection dies, the broker publishes `offline` for it
(the Last Will).

Runnable versions are in [`example/`](example/): `example.dart` runs both
roles in one program, and `simulated_device.dart` + `controller.dart` run in
two terminals.

### Common mistakes

- **`asDevice` after `connect`.** It throws: the Last Will has to be in the
  CONNECT message. Call `asDevice` first.
- **Different `topicPrefix` on the two sides.** Then they never see each
  other, with no error. Both must use the same prefix (and the same
  device id).
- **The same `clientId` on two clients.** The broker keeps kicking one off,
  so both reconnect forever. Leave `clientId` empty for a random one.
- **Not answering a command.** Every command needs `ack(command)`, even a
  failed one (`ack(command, error: ...)`), or `send` waits until its
  timeout.
- **Device ids with `/`, `+` or `#`.** They are MQTT topic levels and
  wildcards, so they are rejected. Use letters, digits, `-` and `_`.
- **Expecting QoS 0 to survive a dropped connection.** It is "send and
  forget". Use QoS 1 (the default for `Topic.json`) for anything that must
  arrive. See [Delivery guarantees](#delivery-guarantees).

## Topics

Topics are written without a prefix. The client adds
`ClientConfig.topicPrefix` (default `bedaya`) in front:

| Topic | On the wire | Payload |
|---|---|---|
| `BedayaTopics.telemetry` | `bedaya/{deviceId}/telemetry` | `Telemetry`: any readings, like `{"temp": 24.5}` |
| `BedayaTopics.status` | `bedaya/{deviceId}/status` | `DeviceStatus`: `{"online": true}`, retained |
| `BedayaTopics.command` | `bedaya/{deviceId}/command` | `Command`: `{"id", "name", "params"}` |
| `BedayaTopics.commandAck` | `bedaya/{deviceId}/command/ack` | `CommandAck`: `{"id", "ok", "error"?}` |

The full contract for firmware is in
[`contract/contract.yaml`](contract/contract.yaml).

### Your own typed topics

```dart
class Reading {
  Reading(this.lux);
  factory Reading.fromJson(Map<String, dynamic> j) => Reading(j['lux'] as num);
  final num lux;
  Map<String, dynamic> toJson() => {'lux': lux};
}

final light = Topic.json(
  '{room}/light',
  fromJson: Reading.fromJson,
  toJson: (r) => r.toJson(),
  qos: Qos.atMostOnce,
);

client.subscribe(light).listen((m) {
  print('${m.params['room']}: ${m.value.lux} lux'); // m.value is a Reading
});
await client.publish(light, Reading(300), params: {'room': 'kitchen'});
```

For payloads that are not JSON, implement `MessageCodec<T>` and use
`Topic(pattern, codec: ...)`.

## Connection state

```dart
client.connectionStates.listen((state) {
  switch (state) {
    case Connected():
      print('online');
    case Reconnecting(:final attempt, :final nextDelay):
      print('retry #$attempt in $nextDelay');
    case Failed(:final error):
      print('gave up: $error');
    case Disconnected() || Connecting():
      break;
  }
});
```

## Reconnect

On by default: about 1s, 2s, 4s, 8s, 16s, then every 30s, ±20%, forever.

```dart
ClientConfig(
  host: 'broker.example.com',
  reconnect: ReconnectPolicy(
    initialDelay: Duration(seconds: 2),
    maxDelay: Duration(minutes: 1),
    maxAttempts: 10, // then Failed; null = forever
  ),
  // Also retry the very first connect (for scripts that start before the
  // network is up). Off by default, so a wrong host fails fast.
  retryFirstConnect: true,
)
```

Use `ReconnectPolicy.never()` to turn it off.

## Delivery guarantees

What `await client.publish(...)` means, and what happens when the network
goes away:

| | QoS 0, `drop` topics (`BedayaTopics.telemetry`) | QoS 1, `queue` topics (default for `Topic.json`, acks) |
|---|---|---|
| Connected | Completes once sent. | Completes when the **broker confirms** it (PUBACK). |
| Offline | **Dropped**: a reading replayed later would look current. | Goes into the offline queue; sent after the reconnect. |
| Connection drops before the broker confirms | May be lost. | Goes back into the offline queue and is **sent again** after the reconnect. |
| Arrives | At most once. | **At least once** while it stays in the queue: possibly twice. |

Commands are different: `send()` never queues them (see
[Offline queue](#offline-queue)).

- **"At least once" has limits.** A queued message is sent again until
  the broker confirms it, as long as it is still in the queue and the
  connection comes back. It is lost if the queue is full (the oldest
  message goes, reported on `client.errors`), if it is older than
  `queueTtl`, or if the app stops (the default queue lives in memory;
  implement `MessageQueue` to keep it on disk).
- **Dead links are noticed.** If the network vanishes without closing the
  connection (lost WiFi, WiFi → 4G), a ping with no answer ends the
  connection after about 2 × `keepAlive`, and the client reconnects.
  Lower `keepAlive` to notice sooner, at the cost of more traffic.
- **Order is kept.** Queued messages are sent oldest first, and new ones
  on `queue` topics wait behind them until the queue is empty.
- **Duplicates are possible with QoS 1.** A message whose ack was lost is
  sent again. For commands, compare `Command.id` if running one twice
  would be a problem.
- **A `CommandTimeoutException` means the outcome is unknown**, not that
  nothing happened: the command may have reached the device and only the
  confirmation or the ack was late. It is never sent again after the
  timeout. Prefer commands that set a state (`Command.relay(on: true)`)
  over ones that flip it.
- **Receivers miss what is sent while they are offline.** Sessions are
  clean: the queue covers the *sender's* offline time, not the
  receiver's. A device's retained `status` is always delivered on
  subscribe.

## Offline queue

While not connected, what `publish` does depends on the topic's
`OfflinePolicy`:

| Policy | While offline | Used by |
|---|---|---|
| `queue` (default) | Kept, and sent in order after the next connect (after resubscribing), waiting for the broker's confirmation of each QoS 1 or 2 message | `commandAck`, your own topics |
| `drop` | Thrown away; `publish` completes normally | `telemetry`, `status` |
| `fail` | `publish` throws `ConnectionException` | `command` |

Telemetry is dropped because a reading replayed after a reconnect looks
current. Commands are never queued: `device.send()` waits for the
connection only until its timeout, and a command that timed out is
**never** sent later. Pick a policy for your own topics:

```dart
final alarm = Topic.json(
  '{deviceId}/alarm',
  fromJson: Alarm.fromJson,
  toJson: (a) => a.toJson(),
  offline: OfflinePolicy.queue, // an alarm should arrive, even late
);
```

The queue itself:

```dart
ClientConfig(
  host: 'broker.example.com',
  maxQueueSize: 500,              // drop the oldest when full; 0 = no queue
  queueTtl: Duration(minutes: 10), // drop messages older than this
)
```

A dropped message is reported on `client.errors` as a
`MessageDroppedException`. The default queue lives in memory. To keep
messages across app restarts, implement `MessageQueue` and pass it as
`BedayaClient(config, queue: myQueue)`.

## TLS

```dart
ClientConfig(
  host: 'broker.example.com',
  useTls: true, // port 8883 by default
  // Only for a self-signed or private broker certificate:
  caCertificate: await File('ca.crt').readAsString(),
  // Only if the broker checks client certificates (mutual TLS):
  clientCertificate: await File('client.crt').readAsString(),
  clientKey: await File('client.key').readAsString(),
)
```

Certificates are PEM text, so they can also come from Flutter assets.

## Web

In the browser the client always uses MQTT over WebSocket. Point it at the
broker's WebSocket port:

```dart
ClientConfig(
  host: 'broker.hivemq.com',
  port: 8884, // HiveMQ's secure WebSocket port
  useTls: true,
  webSocketPath: '/mqtt', // the default
)
```

The browser checks the certificate itself, so custom certificates are not
supported on the web. On native platforms, set `useWebSocket: true` to use
WebSocket instead of TCP.

## All devices at once

```dart
client.devices.changes.listen((all) {
  for (final d in all.values) {
    print('${d.id}: ${d.online ? 'online' : 'offline'}, last seen ${d.lastSeen}');
  }
});
```

## Tracing and debugging

Pass a `logger` to see every step the client takes. `print` works:

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
```

Each `BedayaLogRecord` has a `level` (`debug`, `info`, `warning`, `error`),
a `message`, the `error` behind it, and a `time`, so you can filter or send
records to your own logging. Off by default.

To understand the code itself, [doc/ARCHITECTURE.md](doc/ARCHITECTURE.md)
follows each operation (connect, publish, receive, reconnect, commands)
through the files, and lists where to look for common problems.

## Errors

| Where | What |
|---|---|
| `connect()` | `ConnectionException`: broker unreachable, or refused (for example `notAuthorized`) |
| `device.send()` | `CommandTimeoutException` when no ack arrives in time (10s by default) |
| `client.errors` | `DecodeException` for payloads that don't decode, `MessageDroppedException` when the queue is full |

Every exception extends `BedayaException`.

## Testing your code

`package:bedaya_mqtt/testing.dart` has an in-memory broker that supports
wildcards, retained messages and the Last Will:

```dart
import 'package:bedaya_mqtt/bedaya_mqtt.dart';
import 'package:bedaya_mqtt/testing.dart';

final broker = MemoryBroker();
final device = BedayaClient(
  ClientConfig(host: 'memory'),
  transport: MemoryTransport(broker),
);
final controller = BedayaClient(
  ClientConfig(host: 'memory'),
  transport: MemoryTransport(broker),
);
```

Use `transport.drop()` to simulate a lost connection, and
`broker.stop()` / `broker.start()` to simulate the broker going down.

## Good to know

- A second stream on a topic that is already subscribed does not get the
  retained message again (brokers send it only on subscribe).
- The offline queue lives in memory: it is lost if the app stops. To keep
  it across restarts, implement `MessageQueue` (see above).

See also [Common mistakes](#common-mistakes) and
[Delivery guarantees](#delivery-guarantees).

## About Bedaya

Bedaya is building a Flutter app for controlling ESP devices over MQTT. This
package is the free, open source part, and it works with any MQTT broker and
any devices that follow the [contract](contract/contract.yaml).

## License

BSD 3-Clause. See [LICENSE](LICENSE).
