import 'dart:async';

import 'package:bedaya_mqtt/bedaya_mqtt.dart';
import 'package:bedaya_mqtt/testing.dart';
import 'package:test/test.dart';

/// A topic the broker refuses in these tests, like an ACL would.
final secret = Topic.json(
  '{deviceId}/secret',
  fromJson: Telemetry.fromJson,
  toJson: (t) => t.toJson(),
);

const fast = ReconnectPolicy(
  initialDelay: Duration(milliseconds: 10),
  maxDelay: Duration(milliseconds: 40),
  jitter: 0,
);

void main() {
  late MemoryBroker broker;
  late MemoryTransport transport;
  late BedayaClient client;
  late List<BedayaException> errors;
  late BedayaClient publisher;

  setUp(() async {
    broker = MemoryBroker()
      ..rejectSubscription = (filter) => filter.endsWith('/secret');
    transport = MemoryTransport(broker);
    client = BedayaClient(
      const ClientConfig(host: 'memory', reconnect: fast),
      transport: transport,
    );
    errors = [];
    client.errors.listen(errors.add);
    publisher = BedayaClient(
      const ClientConfig(host: 'memory'),
      transport: MemoryTransport(broker),
    );
    await publisher.connect();
  });

  tearDown(() async {
    await client.close();
    await publisher.close();
  });

  /// Listens with an onError that fails the test: a refusal must never
  /// reach the stream (an unhandled stream error would crash a CLI app).
  List<Object?> listenTo<T>(Topic<T> topic) {
    final got = <Object?>[];
    client
        .subscribe(topic, params: {'deviceId': 'a1'})
        .listen(
          (m) => got.add(m.value),
          onError: (Object e) => fail('the stream got an error: $e'),
        );
    return got;
  }

  Future<void> sendTelemetry() => publisher.publish(
    BedayaTopics.telemetry,
    const Telemetry({'temp': 1}),
    params: {'deviceId': 'a1'},
  );

  test('refused for a new listener: reported on errors, not on the '
      'stream', () async {
    await client.connect();
    listenTo(secret);
    await pumpEventQueue();

    print('  errors: $errors');
    expect(errors, hasLength(1));
    final e = errors.single as SubscriptionException;
    expect(e.filter, 'bedaya/a1/secret');
    expect(e.reason, 'rejected by the broker');
    expect(e.context, SubscriptionContext.newListener);
    expect(client.connectionState, isA<Connected>());
  });

  test('refused on the first connect: connect() still succeeds, and the '
      'other subscriptions work', () async {
    listenTo(secret);
    final telemetry = listenTo(BedayaTopics.telemetry);
    await client.connect();

    expect(client.connectionState, isA<Connected>());
    expect(
      errors.whereType<SubscriptionException>().single.context,
      SubscriptionContext.firstConnect,
    );

    await sendTelemetry();
    await pumpEventQueue();
    expect(telemetry, hasLength(1));
  });

  test('refused after a reconnect: reported as such, the others are '
      'restored', () async {
    broker.rejectSubscription = null; // allowed at first
    listenTo(secret);
    final telemetry = listenTo(BedayaTopics.telemetry);
    await client.connect();
    expect(errors, isEmpty);

    broker.rejectSubscription = (f) => f.endsWith('/secret'); // ACL changed
    transport.drop();
    await client.connectionStates.firstWhere((s) => s is Connected);
    await pumpEventQueue();

    expect(
      errors.whereType<SubscriptionException>().single.context,
      SubscriptionContext.reconnect,
    );
    await sendTelemetry();
    await pumpEventQueue();
    expect(telemetry, hasLength(1), reason: 'restored despite the refusal');
  });

  test('fixing the ACL is enough: the next reconnect asks again', () async {
    final got = listenTo(secret);
    await client.connect();
    expect(errors, hasLength(1));

    broker.rejectSubscription = null; // the ACL is fixed
    transport.drop();
    await client.connectionStates.firstWhere((s) => s is Connected);
    await pumpEventQueue();
    expect(errors, hasLength(1), reason: 'no new error');

    await publisher.publish(
      secret,
      const Telemetry({'temp': 2}),
      params: {'deviceId': 'a1'},
    );
    await pumpEventQueue();
    expect(got, hasLength(1));
  });

  test('the message says what, where and when', () async {
    listenTo(secret);
    await client.connect();
    final e = errors.single;
    print('  ${e.message}');
    expect(e.message, contains('bedaya/a1/secret'));
    expect(e.message, contains('rejected by the broker'));
    expect(e.message, contains('first connect'));
  });
}
