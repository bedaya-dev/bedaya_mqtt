// Tests against a real Mosquitto broker in Docker.
//
//   tool/mosquitto.sh start
//   BEDAYA_MOSQUITTO=1 dart test test/integration
//
// Skipped otherwise, so `dart test` works without Docker.
@TestOn('vm')
library;

import 'dart:async';
import 'dart:convert';
import 'dart:io';
import 'dart:math';

import 'package:bedaya_mqtt/bedaya_mqtt.dart';
import 'package:test/test.dart';

const container = 'bedaya-mosquitto';
const tcpPort = 18830;
const authPort = 18831;
const wsPort = 18080;
const wssPort = 18081;
const tlsPort = 18883;
const mtlsPort = 18884;

/// Made by tool/mosquitto.sh.
String cert(String name) =>
    File('tool/mosquitto/certs/$name').readAsStringSync();

const fast = ReconnectPolicy(
  initialDelay: Duration(milliseconds: 200),
  maxDelay: Duration(seconds: 1),
  jitter: 0,
);

final enabled = Platform.environment['BEDAYA_MOSQUITTO'] == '1';

void main() {
  late String prefix;
  final clients = <BedayaClient>[];

  BedayaClient make({
    int port = tcpPort,
    bool webSocket = false,
    String? username,
    String? password,
    bool retryFirstConnect = false,
    bool tls = false,
    String? ca,
    String? clientCertificate,
    String? clientKey,
  }) {
    final c = BedayaClient(
      ClientConfig(
        host: 'localhost',
        port: port,
        useWebSocket: webSocket,
        username: username,
        password: password,
        useTls: tls,
        caCertificate: ca,
        clientCertificate: clientCertificate,
        clientKey: clientKey,
        topicPrefix: prefix,
        keepAlive: const Duration(seconds: 2),
        reconnect: fast,
        retryFirstConnect: retryFirstConnect,
      ),
    );
    clients.add(c);
    return c;
  }

  Future<void> docker(String command) async {
    final result = await Process.run('docker', [command, container]);
    if (result.exitCode != 0) throw StateError('docker $command failed');
  }

  Future<T> within<T>(Future<T> f, [int seconds = 10]) =>
      f.timeout(Duration(seconds: seconds));

  /// Completes when [c] is connected, now or later. Waiting only for the
  /// next `Connected` event would hang if it already happened.
  Future<void> connected(BedayaClient c) async {
    if (c.connectionState is Connected) return;
    await c.connectionStates.firstWhere((s) => s is Connected);
  }

  /// Waits until [condition] is true, checking every 50ms.
  Future<void> until(bool Function() condition, [int seconds = 10]) async {
    final deadline = DateTime.now().add(Duration(seconds: seconds));
    while (!condition()) {
      if (DateTime.now().isAfter(deadline)) {
        throw TimeoutException('condition not met within ${seconds}s');
      }
      await Future<void>.delayed(const Duration(milliseconds: 50));
    }
  }

  setUp(() {
    // A fresh prefix per test, so retained messages don't leak between tests.
    prefix = 'it${Random().nextInt(1 << 32).toRadixString(16)}';
  });

  tearDown(() async {
    for (final c in clients) {
      await c.close();
    }
    clients.clear();
  });

  group(
    'mosquitto',
    skip: enabled
        ? false
        : 'Run tool/mosquitto.sh start, then BEDAYA_MOSQUITTO=1 dart test',
    () {
      test('TCP: telemetry, status and command + ack', () async {
        final deviceClient = make();
        final me = deviceClient.asDevice('a1');
        me.commands.listen((c) => me.ack(c));
        await deviceClient.connect();

        final controller = make();
        await controller.connect();
        final device = controller.device('a1');

        final status = await within(device.status.first);
        print('  status: $status');
        expect(status.online, isTrue);

        final next = device.on(BedayaTopics.telemetry).first;
        await Future<void>.delayed(const Duration(milliseconds: 200));
        await me.publish(BedayaTopics.telemetry, const Telemetry({'t': 1}));
        expect((await within(next))['t'], 1);

        final ack = await device.send(Command.relay(on: true));
        print('  ack: $ack');
        expect(ack.ok, isTrue);
      });

      test('login: right password connects, wrong one is refused', () async {
        final good = make(
          port: authPort,
          username: 'bedaya',
          password: 'secret',
        );
        await within(good.connect());
        expect(good.connectionState, isA<Connected>());

        final bad = make(port: authPort, username: 'bedaya', password: 'nope');
        try {
          await within(bad.connect());
          fail('should have been refused');
        } on ConnectionException catch (e) {
          print('  refused: ${e.message}');
        }
        expect(bad.connectionState, isA<Failed>());
      });

      test('WebSocket: messages go through', () async {
        final ws = make(port: wsPort, webSocket: true);
        await within(ws.connect());

        final next = ws.subscribe(BedayaTopics.telemetry).first;
        await Future<void>.delayed(const Duration(milliseconds: 200));
        await ws.publish(
          BedayaTopics.telemetry,
          const Telemetry({'via': 'ws'}),
          params: {'deviceId': 'w1'},
        );
        final m = await within(next);
        print('  got over WebSocket: $m');
        expect(m.value['via'], 'ws');
      });

      test('Last Will: a killed device goes offline', () async {
        final controller = make();
        await controller.connect();
        final statuses = <bool>[];
        controller.device('k1').status.listen((s) => statuses.add(s.online));

        final process = await Process.start('dart', [
          'run',
          'test/integration/will_device.dart',
          '$tcpPort',
          prefix,
          'k1',
        ]);
        await within(
          process.stdout
              .transform(utf8.decoder)
              .firstWhere((l) => l.contains('ready')),
          60,
        );
        // Watch the one stream we have. A second `status` stream would
        // miss messages: the filter is already subscribed, so the broker
        // doesn't resend the retained status.
        await until(() => statuses.contains(true));

        process.kill(ProcessSignal.sigkill);
        await until(() => statuses.last == false);
        print('  statuses: $statuses');
        expect(statuses, [true, false]);
      });

      test(
        'broker restart: reconnects, resubscribes, sends the queue',
        () async {
          final deviceClient = make();
          final me = deviceClient.asDevice('r1');
          await deviceClient.connect();

          final controller = make();
          await controller.connect();
          final got = <Object?>[];
          controller
              .device('r1')
              .on(BedayaTopics.telemetry)
              .listen((t) => got.add(t['n']));
          await Future<void>.delayed(const Duration(milliseconds: 200));
          await me.publish(BedayaTopics.telemetry, const Telemetry({'n': 1}));

          final states = <MqttConnectionState>[];
          deviceClient.connectionStates.listen(states.add);

          await docker('stop');
          await within(
            deviceClient.connectionStates.firstWhere((s) => s is Reconnecting),
          );
          // Published while the broker is down: queued.
          await me.publish(BedayaTopics.telemetry, const Telemetry({'n': 2}));
          expect(deviceClient.queuedMessages, 1);

          await docker('start');
          // Either client may come back first, so check the current state
          // before waiting for the event.
          await within(connected(deviceClient), 20);
          await within(connected(controller), 20);
          // The device drained its queue before the controller resubscribed
          // maybe, so publish once more to prove the subscription is back.
          await Future<void>.delayed(const Duration(milliseconds: 500));
          await me.publish(BedayaTopics.telemetry, const Telemetry({'n': 3}));
          await Future<void>.delayed(const Duration(milliseconds: 500));

          print('  device states: $states');
          print('  controller got: $got');
          expect(states.whereType<Reconnecting>(), isNotEmpty);
          expect(deviceClient.queuedMessages, 0);
          expect(got.first, 1);
          expect(got.last, 3);
        },
      );

      test('retryFirstConnect: waits for the broker to start', () async {
        await docker('stop');
        final patient = make(retryFirstConnect: true);
        final connecting = patient.connect();

        await within(
          patient.connectionStates.firstWhere(
            (s) => s is Reconnecting && s.attempt >= 2,
          ),
        );
        await docker('start');
        await within(connecting, 20);

        print('  state: ${patient.connectionState}');
        expect(patient.connectionState, isA<Connected>());
      });

      group('TLS', () {
        /// Publishes on [c] and checks it comes back, so the encrypted
        /// connection really carries messages.
        Future<void> roundTrip(BedayaClient c) async {
          final next = c.subscribe(BedayaTopics.telemetry).first;
          await Future<void>.delayed(const Duration(milliseconds: 200));
          await c.publish(
            BedayaTopics.telemetry,
            const Telemetry({'secure': true}),
            params: {'deviceId': 's1'},
          );
          final m = await within(next);
          print('  got: $m');
          expect(m.value['secure'], isTrue);
        }

        test('with our CA: connects and carries messages', () async {
          final c = make(port: tlsPort, tls: true, ca: cert('ca.crt'));
          await within(c.connect());
          await roundTrip(c);
        });

        test(
          'without our CA: refused, the certificate is not trusted',
          () async {
            final c = make(port: tlsPort, tls: true);
            try {
              await within(c.connect());
              fail('should not trust a self-signed broker');
            } on ConnectionException catch (e) {
              print('  refused: ${e.message} (${e.cause.runtimeType})');
            }
            expect(c.connectionState, isA<Failed>());
          },
        );

        test('mutual TLS with a client certificate: connects', () async {
          final c = make(
            port: mtlsPort,
            tls: true,
            ca: cert('ca.crt'),
            clientCertificate: cert('client.crt'),
            clientKey: cert('client.key'),
          );
          await within(c.connect());
          await roundTrip(c);
        });

        test('mutual TLS without a client certificate: refused', () async {
          final c = make(port: mtlsPort, tls: true, ca: cert('ca.crt'));
          try {
            await within(c.connect());
            fail('the broker requires a client certificate');
          } on ConnectionException catch (e) {
            print('  refused: ${e.message}');
          }
          expect(c.connectionState, isA<Failed>());
        });

        test('secure WebSocket with our CA: connects', () async {
          final c = make(
            port: wssPort,
            webSocket: true,
            tls: true,
            ca: cert('ca.crt'),
          );
          await within(c.connect());
          await roundTrip(c);
        });
      });
    },
  );
}
