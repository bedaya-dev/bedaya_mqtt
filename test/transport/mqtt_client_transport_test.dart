@TestOn('vm')
library;

import 'package:bedaya_mqtt/bedaya_mqtt.dart';
import 'package:bedaya_mqtt/src/transport/mqtt_transport.dart';
import 'package:bedaya_mqtt/src/transport/transport_factory.dart';
import 'package:bedaya_mqtt/src/transport/transport_options.dart';
import 'package:test/test.dart';

import '../support/stub_mqtt_server.dart';

/// The real MqttClientTransport against a raw MQTT 3.1.1 stub server that
/// answers SUBSCRIBE with SUBACK 0x80 for chosen filters. No Docker.
void main() {
  late StubMqttServer server;

  setUp(() async {
    server = await StubMqttServer.start(
      rejects: (filter) => filter.endsWith('/secret'),
    );
  });

  tearDown(() => server.close());

  test('SUBACK 0x80: subscribe() throws SubscriptionException; an accepted '
      'filter completes', () async {
    final transport = createTransport(
      TransportOptions(
        host: '127.0.0.1',
        port: server.port,
        clientId: 'stub-transport',
      ),
    );
    await transport.connect();
    expect(transport.state, TransportState.connected);

    await transport.subscribe('bedaya/a1/telemetry', Qos.atLeastOnce);

    final e = await transport
        .subscribe('bedaya/a1/secret', Qos.atLeastOnce)
        .then<Object?>((_) => null, onError: (Object e) => e);
    print('  subscribe threw: $e');
    print('  server saw: ${server.subscriptions}');
    expect(e, isA<SubscriptionException>());
    e as SubscriptionException;
    expect(e.filter, 'bedaya/a1/secret');
    expect(e.reason, 'rejected by the broker');
    expect(server.subscriptions, [
      ('bedaya/a1/telemetry', 1),
      ('bedaya/a1/secret', 0x80),
    ]);
    expect(transport.state, TransportState.connected, reason: 'still up');

    await transport.disconnect();
  });

  test('SUBACK 0x80 through BedayaClient: on client.errors, never on the '
      'stream', () async {
    final secret = Topic.json(
      '{deviceId}/secret',
      fromJson: Telemetry.fromJson,
      toJson: (t) => t.toJson(),
    );
    final client = BedayaClient(
      ClientConfig(
        host: '127.0.0.1',
        port: server.port,
        reconnect: const ReconnectPolicy.never(),
      ),
    );
    final errors = <BedayaException>[];
    client.errors.listen(errors.add);
    client
        .subscribe(secret, params: {'deviceId': 'a1'})
        .listen(
          (_) {},
          onError: (Object e) => fail('the stream got an error: $e'),
        );
    client.subscribe(BedayaTopics.telemetry).listen((_) {});

    // connect() waits for the broker's answers.
    await client.connect();
    print('  errors: $errors');
    final refused = errors.whereType<SubscriptionException>().single;
    expect(refused.filter, 'bedaya/a1/secret');
    expect(refused.context, SubscriptionContext.firstConnect);
    expect(client.connectionState, isA<Connected>());

    await client.close();
  });
}
