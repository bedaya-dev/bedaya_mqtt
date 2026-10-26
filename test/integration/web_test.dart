// Runs in a real browser, with the browser transport (MqttBrowserClient).
//
//   tool/mosquitto.sh start
//   dart test -p chrome test/integration/web_test.dart
//
// The plain `dart test` runs on the VM only, so it skips this file.
@TestOn('browser')
library;

import 'package:bedaya_mqtt/bedaya_mqtt.dart';
import 'package:test/test.dart';

const wsPort = 18080;

void main() {
  late String prefix;
  final clients = <BedayaClient>[];

  BedayaClient make(ClientConfig config) {
    final c = BedayaClient(config);
    clients.add(c);
    return c;
  }

  Future<void> roundTrip(BedayaClient c) async {
    final next = c.subscribe(BedayaTopics.telemetry).first;
    await Future<void>.delayed(const Duration(milliseconds: 300));
    await c.publish(
      BedayaTopics.telemetry,
      const Telemetry({'from': 'browser'}),
      params: {'deviceId': 'b1'},
    );
    final m = await next.timeout(const Duration(seconds: 10));
    print('  got: $m');
    expect(m.value['from'], 'browser');
  }

  setUp(() {
    prefix = 'web${DateTime.now().microsecondsSinceEpoch}';
  });

  tearDown(() async {
    for (final c in clients) {
      await c.close();
    }
    clients.clear();
  });

  test('WebSocket to the local Mosquitto', () async {
    // useWebSocket is not needed: the browser always uses WebSocket.
    final c = make(
      ClientConfig(host: 'localhost', port: wsPort, topicPrefix: prefix),
    );
    await c.connect().timeout(const Duration(seconds: 10));
    await roundTrip(c);
  });

  test(
    'secure WebSocket to HiveMQ (the browser checks the certificate)',
    () async {
      final c = make(
        ClientConfig(
          host: 'broker.hivemq.com',
          port: 8884,
          useTls: true,
          topicPrefix: prefix,
          retryFirstConnect: true,
          reconnect: const ReconnectPolicy(maxAttempts: 3),
        ),
      );
      await c.connect().timeout(const Duration(seconds: 30));
      await roundTrip(c);
    },
  );

  test('custom certificates are refused in the browser', () {
    expect(
      () => BedayaClient(
        const ClientConfig(host: 'x', useTls: true, caCertificate: 'pem'),
      ),
      throwsArgumentError,
    );
  });
}
