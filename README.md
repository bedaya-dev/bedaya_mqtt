# bedaya_mqtt

Typed, reliable MQTT for Dart: reconnect, offline queue, typed topics.
Built by Bedaya.

`bedaya_mqtt` sits on top of [`mqtt_client`](https://pub.dev/packages/mqtt_client)
and adds the parts every IoT app ends up writing by hand:

- **Automatic reconnect** with exponential backoff and jitter, and every
  subscription restored afterwards.
- **Offline queue**: publish while offline, and messages are sent once the
  connection is back.
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

### Controller: watch a device and send it a command

```dart
import 'package:bedaya_mqtt/bedaya_mqtt.dart';

final client = BedayaClient(ClientConfig(host: 'broker.hivemq.com'));
await client.connect();

final device = client.device('a1');
device.status.listen((s) => print(s.online ? 'online' : 'offline'));
device.on(BedayaTopics.telemetry).listen((t) => print('temp: ${t['temp']}'));

final ack = await device.send(Command.relay(on: true));
print(ack.ok ? 'relay is on' : 'failed: ${ack.error}');
```

### Device: publish data and answer commands

```dart
final client = BedayaClient(ClientConfig(host: 'broker.hivemq.com'));

// Call asDevice before connect: it sets the Last Will, so the broker
// reports the device offline if it drops off.
final me = client.asDevice('a1');
await client.connect();

me.commands.listen((command) {
  if (command.name == 'relay') {
    final on = command.params['on'] as bool;
    // ... switch the relay ...
    me.ack(command);
  } else {
    me.ack(command, error: 'unknown command');
  }
});

await me.publish(BedayaTopics.telemetry, Telemetry({'temp': 24.5}));
```

The client publishes `online` when it connects and `offline` when it
disconnects. If the connection dies, the broker publishes `offline` for it
(the Last Will).

Runnable versions are in [`example/`](example/): `example.dart` runs both
roles in one program, and `simulated_device.dart` + `controller.dart` run in
two terminals.

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

## Offline queue

While not connected, `publish` puts the message in a queue and returns.
After the next connect, the client resubscribes, then sends the queue in
order.

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

- Client ids must be unique. Two connections with the same id keep kicking
  each other off the broker. Leave `clientId` empty for a random one.
- Sessions are clean: while a controller is offline, it misses messages
  that devices send. The offline queue covers the *sender's* offline time.
- A second stream on a topic that is already subscribed does not get the
  retained message again (brokers send it only on subscribe).

## About Bedaya

Bedaya is an IoT kit: ESP boards and a Flutter app. This package is the free,
open source part, and it works with any MQTT broker and any devices that
follow the [contract](contract/contract.yaml).

## License

BSD 3-Clause. See [LICENSE](LICENSE).
