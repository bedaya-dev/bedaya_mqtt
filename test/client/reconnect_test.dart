import 'dart:convert';

import 'package:bedaya_mqtt/bedaya_mqtt.dart';
import 'package:bedaya_mqtt/testing.dart';
import 'package:test/test.dart';

/// Short delays, no jitter, so the tests are fast and predictable.
const fast = ReconnectPolicy(
  initialDelay: Duration(milliseconds: 10),
  maxDelay: Duration(milliseconds: 40),
  jitter: 0,
);

void main() {
  late MemoryBroker broker;
  late MemoryTransport transport;
  late BedayaClient client;
  late List<MqttConnectionState> states;

  BedayaClient makeClient({
    ReconnectPolicy reconnect = fast,
    bool retryFirstConnect = false,
    int maxQueueSize = 100,
    MqttTransport? using,
  }) => BedayaClient(
    ClientConfig(
      host: 'memory',
      reconnect: reconnect,
      retryFirstConnect: retryFirstConnect,
      maxQueueSize: maxQueueSize,
    ),
    transport: using ?? transport,
  );

  Future<void> connected(BedayaClient c) =>
      c.connectionStates.firstWhere((s) => s is Connected);

  setUp(() async {
    broker = MemoryBroker();
    transport = MemoryTransport(broker);
    client = makeClient();
    states = [];
    client.connectionStates.listen(states.add);
  });

  group('retryFirstConnect', () {
    test('off (default): the first connect throws at once', () async {
      broker.stop();
      await expectLater(client.connect(), throwsA(isA<ConnectionException>()));
      await pumpEventQueue();

      print('  states: $states');
      expect(states, [isA<Connecting>(), isA<Failed>()]);
    });

    test('on: retries until the broker is up, then completes', () async {
      final patient = makeClient(retryFirstConnect: true);
      final seen = <MqttConnectionState>[];
      patient.connectionStates.listen(seen.add);
      broker.stop();

      var done = false;
      final connecting = patient.connect().then((_) => done = true);
      await patient.connectionStates.firstWhere(
        (s) => s is Reconnecting && s.attempt == 3,
      );
      expect(done, isFalse, reason: 'still waiting for the broker');

      broker.start();
      await connecting;

      print('  states: $seen');
      expect(patient.connectionState, isA<Connected>());
      expect(seen.first, isA<Connecting>());
      expect(seen.whereType<Reconnecting>().map((s) => s.attempt).take(3), [
        1,
        2,
        3,
      ]);
      expect(seen.last, isA<Connected>());
    });

    test('on: subscriptions and queued messages are ready when connect '
        'completes', () async {
      final patient = makeClient(retryFirstConnect: true);
      patient.subscribe(BedayaTopics.telemetry).listen((_) {});
      await patient.publish(
        BedayaTopics.telemetry,
        const Telemetry({'n': 1}),
        params: {'deviceId': 'a1'},
      );
      broker.stop();

      final connecting = patient.connect();
      await patient.connectionStates.firstWhere((s) => s is Reconnecting);
      broker.start();
      await connecting;

      print('  subscriptions: ${transport.subscriptions.keys}');
      print('  queued: ${patient.queuedMessages}');
      expect(transport.subscriptions.keys, ['bedaya/+/telemetry']);
      expect(patient.queuedMessages, 0);
    });

    test('on: throws after maxAttempts', () async {
      final limited = makeClient(
        retryFirstConnect: true,
        reconnect: const ReconnectPolicy(
          initialDelay: Duration(milliseconds: 5),
          jitter: 0,
          maxAttempts: 2,
        ),
      );
      broker.stop();

      await expectLater(limited.connect(), throwsA(isA<ConnectionException>()));
      print('  state: ${limited.connectionState}');
      expect(limited.connectionState, isA<Failed>());
    });

    test('on: disconnect() while retrying makes connect throw', () async {
      final patient = makeClient(retryFirstConnect: true);
      broker.stop();

      final connecting = patient.connect();
      await patient.connectionStates.firstWhere((s) => s is Reconnecting);
      await patient.disconnect();

      await expectLater(connecting, throwsA(isA<ConnectionException>()));
      broker.start();
      await Future<void>.delayed(const Duration(milliseconds: 100));
      expect(patient.connectionState, isA<Disconnected>());
      expect(transport.state, TransportState.disconnected);
    });

    test('on, but with ReconnectPolicy.never(): throws at once', () async {
      final never = makeClient(
        retryFirstConnect: true,
        reconnect: const ReconnectPolicy.never(),
      );
      broker.stop();
      await expectLater(never.connect(), throwsA(isA<ConnectionException>()));
      expect(never.connectionState, isA<Failed>());
    });
  });

  group('reconnect', () {
    test('a drop goes Reconnecting → Connected', () async {
      await client.connect();
      states.clear();

      transport.drop();
      await connected(client);

      print('  states: $states');
      expect(states, [
        isA<Reconnecting>()
            .having((s) => s.attempt, 'attempt', 1)
            .having((s) => s.nextDelay, 'nextDelay', fast.initialDelay),
        isA<Connected>(),
      ]);
    });

    test('keeps retrying while the broker is down', () async {
      await client.connect();
      states.clear();

      broker.stop();
      await client.connectionStates.firstWhere(
        (s) => s is Reconnecting && s.attempt == 3,
      );
      broker.start();
      await connected(client);

      print('  states: $states');
      final attempts = states.whereType<Reconnecting>().map((s) => s.attempt);
      expect(attempts.take(3), [1, 2, 3]);
      expect(states.last, isA<Connected>());
    });

    test('becomes Failed after maxAttempts', () async {
      final limited = makeClient(
        reconnect: const ReconnectPolicy(
          initialDelay: Duration(milliseconds: 5),
          jitter: 0,
          maxAttempts: 2,
        ),
      );
      await limited.connect();
      broker.stop();

      final failed = await limited.connectionStates.firstWhere(
        (s) => s is Failed,
      );
      print('  $failed');
      expect((failed as Failed).error, isA<ConnectionException>());
    });

    test('disconnect() while reconnecting stops the retries', () async {
      await client.connect();
      broker.stop();
      await client.connectionStates.firstWhere((s) => s is Reconnecting);

      await client.disconnect();
      broker.start();
      await Future<void>.delayed(const Duration(milliseconds: 100));

      expect(client.connectionState, isA<Disconnected>());
      expect(transport.state, TransportState.disconnected);
    });

    test('subscriptions are restored and streams keep working', () async {
      final got = <Object?>[];
      client.subscribe(BedayaTopics.telemetry).listen((m) {
        got.add(m.value['n']);
      });
      await client.connect();

      transport.drop();
      expect(transport.subscriptions, isEmpty);
      await connected(client);
      print('  after reconnect: ${transport.subscriptions.keys}');
      expect(transport.subscriptions.keys, ['bedaya/+/telemetry']);

      final spy = MemoryTransport(broker);
      await spy.connect();
      await spy.publish(
        RawMessage('bedaya/a1/telemetry', utf8.encode('{"n":1}')),
      );
      await pumpEventQueue();
      expect(got, [1]);
    });
  });

  group('offline queue', () {
    late MemoryTransport spy;
    late List<String> wire;

    Future<void> startSpy() async {
      spy = MemoryTransport(broker);
      await spy.connect();
      await spy.subscribe('#', Qos.atMostOnce);
    }

    setUp(() async {
      wire = [];
      await startSpy();
      spy.messages.listen((m) => wire.add(utf8.decode(m.payload)));
    });

    Future<void> publishN(BedayaClient c, int n) => c.publish(
      BedayaTopics.telemetry,
      Telemetry({'n': n}),
      params: {'deviceId': 'a1'},
    );

    test('messages published while offline are sent after reconnect', () async {
      await client.connect();
      broker.stop();
      await startSpy().catchError((_) {}); // broker is down: fails

      await publishN(client, 1);
      await publishN(client, 2);
      print('  queued while offline: ${client.queuedMessages}');
      expect(client.queuedMessages, 2);

      broker.start();
      await startSpy();
      spy.messages.listen((m) => wire.add(utf8.decode(m.payload)));
      await connected(client);
      await pumpEventQueue();

      print('  on the wire: $wire');
      expect(wire, ['{"n":1}', '{"n":2}']);
      expect(client.queuedMessages, 0);
    });

    test(
      'messages published before the first connect are sent on connect',
      () async {
        await publishN(client, 7);
        expect(client.queuedMessages, 1);

        await client.connect();
        await pumpEventQueue();
        expect(wire, ['{"n":7}']);
      },
    );

    test('a full queue drops the oldest and reports it', () async {
      final small = makeClient(maxQueueSize: 2);
      final errors = <BedayaException>[];
      small.errors.listen(errors.add);

      await publishN(small, 1);
      await publishN(small, 2);
      await publishN(small, 3);
      await pumpEventQueue();

      print('  errors: $errors');
      expect(errors.single, isA<MessageDroppedException>());

      await small.connect();
      await pumpEventQueue();
      expect(wire, ['{"n":2}', '{"n":3}']);
    });
  });

  test('device role: online again after a reconnect', () async {
    final deviceTransport = MemoryTransport(broker);
    final device = makeClient(using: deviceTransport);
    device.asDevice('a1');

    final controller = makeClient(using: MemoryTransport(broker));
    await controller.connect();
    final statuses = <bool>[];
    controller.device('a1').status.listen((s) => statuses.add(s.online));

    await device.connect();
    await pumpEventQueue();
    deviceTransport.drop(); // the Last Will says offline
    await connected(device);
    await pumpEventQueue();

    print('  statuses: $statuses');
    expect(statuses, [true, false, true]);
  });
}
