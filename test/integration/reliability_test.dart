// Reliability on a bad network, against a real Mosquitto behind Toxiproxy.
//
//   tool/mosquitto.sh start
//   BEDAYA_MOSQUITTO=1 dart test test/integration/reliability_test.dart
//
// Toxiproxy sits between the client and the broker. A "timeout" toxic with
// timeout 0 drops every byte but keeps the TCP connection open: nothing
// tells the client that the link is dead. That is what a lost WiFi link or
// a phone moving from WiFi to 4G looks like.
@TestOn('vm')
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:bedaya_mqtt/bedaya_mqtt.dart';
import 'package:test/test.dart';

const directPort = 18830; // straight to Mosquitto
const proxyPort = 18890; // through Toxiproxy
const toxiproxyApi = 'http://127.0.0.1:8474';
const keepAlive = Duration(seconds: 2);

const fast = ReconnectPolicy(
  initialDelay: Duration(milliseconds: 200),
  maxDelay: Duration(seconds: 1),
  jitter: 0,
);

final enabled = Platform.environment['BEDAYA_MOSQUITTO'] == '1';

/// A QoS 1 topic for numbered messages, so we can count what arrives.
final numbered = Topic.json(
  '{deviceId}/n',
  fromJson: Telemetry.fromJson,
  toJson: (t) => t.toJson(),
  qos: Qos.atLeastOnce,
);

/// Talks to the Toxiproxy HTTP API.
Future<void> toxiproxy(String method, String path, [Object? body]) async {
  final http = HttpClient();
  try {
    final request = await http.openUrl(method, Uri.parse('$toxiproxyApi$path'));
    if (body != null) {
      request.headers.contentType = ContentType.json;
      request.write(jsonEncode(body));
    }
    final response = await request.close();
    final text = await response.transform(utf8.decoder).join();
    final missingIsFine = method == 'DELETE' && response.statusCode == 404;
    if (response.statusCode >= 300 && !missingIsFine) {
      throw StateError('Toxiproxy $method $path: ${response.statusCode} $text');
    }
  } finally {
    http.close();
  }
}

/// Drops all traffic in both directions, without closing the connection.
Future<void> cutNetwork() async {
  for (final stream in ['upstream', 'downstream']) {
    await toxiproxy('POST', '/proxies/mqtt/toxics', {
      'name': 'cut-$stream',
      'type': 'timeout',
      'stream': stream,
      'toxicity': 1.0,
      'attributes': {'timeout': 0},
    });
  }
}

/// Drops only what the broker sends back. The client's pings still reach
/// the broker, so the broker never closes the connection; the only way to
/// find out the link is bad is for the client to notice that no answers
/// come back. (A full cut is not enough to test that: the broker gives up
/// on a silent client after 1.5 × keepAlive and closes the connection, and
/// Toxiproxy passes that close on to the client.)
Future<void> cutDownstream() async {
  await toxiproxy('POST', '/proxies/mqtt/toxics', {
    'name': 'cut-downstream',
    'type': 'timeout',
    'stream': 'downstream',
    'toxicity': 1.0,
    'attributes': {'timeout': 0},
  });
}

/// Lets traffic through again.
Future<void> restoreNetwork() async {
  for (final stream in ['upstream', 'downstream']) {
    await toxiproxy('DELETE', '/proxies/mqtt/toxics/cut-$stream');
  }
}

void main() {
  late String prefix;
  final clients = <BedayaClient>[];

  BedayaClient make({required int port}) {
    final c = BedayaClient(
      ClientConfig(
        host: 'localhost',
        port: port,
        topicPrefix: prefix,
        keepAlive: keepAlive,
        reconnect: fast,
        logger: (r) => print('    [$port] ${r.level.name} ${r.message}'),
      ),
    );
    clients.add(c);
    return c;
  }

  Future<T> within<T>(Future<T> f, [int seconds = 10]) =>
      f.timeout(Duration(seconds: seconds));

  Future<void> connected(BedayaClient c) async {
    if (c.connectionState is Connected) return;
    await c.connectionStates.firstWhere((s) => s is Connected);
  }

  Future<void> until(bool Function() condition, [int seconds = 10]) async {
    final deadline = DateTime.now().add(Duration(seconds: seconds));
    while (!condition()) {
      if (DateTime.now().isAfter(deadline)) {
        throw TimeoutException('condition not met within ${seconds}s');
      }
      await Future<void>.delayed(const Duration(milliseconds: 50));
    }
  }

  setUp(() async {
    prefix = 'rel${Random().nextInt(1 << 32).toRadixString(16)}';
    if (!enabled) return;
    // A fresh proxy for every test, with no toxics left over.
    await toxiproxy('DELETE', '/proxies/mqtt');
    await toxiproxy('POST', '/proxies', {
      'name': 'mqtt',
      'listen': '0.0.0.0:$proxyPort',
      'upstream': 'bedaya-mosquitto:1883',
      'enabled': true,
    });
  });

  tearDown(() async {
    if (enabled) await restoreNetwork();
    for (final c in clients) {
      await c.close();
    }
    clients.clear();
  });

  group(
    'reliability',
    skip: enabled
        ? false
        : 'Run tool/mosquitto.sh start, then BEDAYA_MOSQUITTO=1 dart test',
    () {
      test('the client itself notices when the broker stops answering', () async {
        final device = make(port: proxyPort);
        await within(device.connect());

        final states = <MqttConnectionState>[];
        device.connectionStates.listen(states.add);

        final cutAt = DateTime.now();
        await cutDownstream();
        try {
          // keepAlive is 2s. A dead link should show up well before 10s.
          await within(
            device.connectionStates.firstWhere((s) => s is! Connected),
            10,
          );
        } on TimeoutException {
          fail(
            'Still "Connected" 10s after the network was cut, with a '
            'keepAlive of ${keepAlive.inSeconds}s. States: $states',
          );
        }
        print('  noticed after ${DateTime.now().difference(cutAt)}');
      });

      test('QoS 1 messages published during a silent drop are not lost', () async {
        // The controller is connected straight to the broker; only the
        // device's link goes bad.
        final controller = make(port: directPort);
        await within(controller.connect());
        final got = <int>[];
        controller
            .subscribe(numbered, params: {'deviceId': 'd1'})
            .listen((m) => got.add(m.value['n'] as int));

        final device = make(port: proxyPort);
        await within(device.connect());
        await Future<void>.delayed(const Duration(milliseconds: 300));

        Future<void> send(int n) => device.publish(
          numbered,
          Telemetry({'n': n}),
          params: {'deviceId': 'd1'},
        );

        await send(1);
        await until(() => got.contains(1));

        await cutNetwork();
        // The device can't know yet: these look sent.
        await send(2);
        await send(3);
        print('  after the cut: ${device.connectionState}, '
            '${device.queuedMessages} queued');

        // Leave the link dead for a few keepAlives, then bring it back.
        await Future<void>.delayed(keepAlive * 3);
        await restoreNetwork();
        await within(connected(device), 20);

        await send(4);
        try {
          await until(() => got.contains(4), 10);
        } on TimeoutException {
          // Keep going: the assertion below says what is missing.
        }
        await Future<void>.delayed(const Duration(milliseconds: 500));

        print('  controller got: $got');
        expect(
          got.toSet(),
          containsAll(<int>[1, 2, 3, 4]),
          reason: 'QoS 1 messages must arrive at least once',
        );
        // QoS 1 may deliver a message twice, so compare first arrivals.
        expect(
          got.toSet().toList(),
          [1, 2, 3, 4],
          reason: 'messages must arrive in the order they were published',
        );
      });

      test('disconnect() publishes offline exactly once', () async {
        // Several rounds, because the duplicate depends on timing.
        for (var round = 1; round <= 3; round++) {
          final id = 'g$round';
          final controller = make(port: directPort);
          await within(controller.connect());
          final statuses = <bool>[];
          controller.device(id).status.listen((s) => statuses.add(s.online));

          final device = make(port: directPort);
          device.asDevice(id);
          await within(device.connect());
          await until(() => statuses.contains(true));

          await device.disconnect();
          // Give a Last Will time to show up, if the broker sends one.
          await Future<void>.delayed(const Duration(milliseconds: 1500));

          print('  round $round: $statuses');
          expect(
            statuses,
            [true, false],
            reason: 'round $round: one online, then exactly one offline',
          );
        }
      });
    },
  );
}
