import 'dart:convert';

import 'package:bedaya_mqtt/bedaya_mqtt.dart';
import 'package:bedaya_mqtt/testing.dart';
import 'package:test/test.dart';

void main() {
  late MemoryBroker broker;
  late MemoryTransport transport;
  late BedayaClient client;

  /// Another raw connection on the same broker, to see the wire.
  late MemoryTransport spy;

  setUp(() async {
    broker = MemoryBroker();
    transport = MemoryTransport(broker);
    client = BedayaClient(
      const ClientConfig(host: 'memory'),
      transport: transport,
    );
    spy = MemoryTransport(broker);
    await spy.connect();
  });

  group('connection', () {
    test('goes Connecting → Connected → Disconnected', () async {
      final states = <MqttConnectionState>[];
      client.connectionStates.listen(states.add);

      await client.connect();
      await client.disconnect();
      await pumpEventQueue();

      print('  states: $states');
      expect(states, [
        isA<Connecting>(),
        isA<Connected>(),
        isA<Disconnected>(),
      ]);
    });

    test('becomes Failed when the broker is down', () async {
      broker.stop();
      await expectLater(client.connect(), throwsA(isA<ConnectionException>()));
      print('  state: ${client.connectionState}');
      expect(client.connectionState, isA<Failed>());
    });

    test('a drop with ReconnectPolicy.never() → Disconnected', () async {
      final noRetry = BedayaClient(
        const ClientConfig(host: 'memory', reconnect: ReconnectPolicy.never()),
        transport: transport,
      );
      await noRetry.connect();
      transport.drop();
      await pumpEventQueue();
      expect(noRetry.connectionState, isA<Disconnected>());
    });

    test('rejects a bad topicPrefix', () {
      for (final bad in ['a/+', '#', '/bedaya', 'bedaya/']) {
        expect(
          () => BedayaClient(
            ClientConfig(host: 'x', topicPrefix: bad),
            transport: transport,
          ),
          throwsArgumentError,
          reason: bad,
        );
      }
    });
  });

  group('certificate settings', () {
    // No transport passed: the checks run before the real one is built.
    // Nothing connects, so no broker is needed.

    test('certificates without useTls are rejected', () {
      expect(
        () => BedayaClient(const ClientConfig(host: 'x', caCertificate: 'pem')),
        throwsArgumentError,
      );
    });

    test('a client certificate needs its key, and the other way round', () {
      expect(
        () => BedayaClient(
          const ClientConfig(host: 'x', useTls: true, clientCertificate: 'pem'),
        ),
        throwsArgumentError,
      );
      expect(
        () => BedayaClient(
          const ClientConfig(host: 'x', useTls: true, clientKey: 'pem'),
        ),
        throwsArgumentError,
      );
    });

    test('TLS without custom certificates is fine', () {
      BedayaClient(const ClientConfig(host: 'x', useTls: true));
    });
  }, testOn: 'vm');

  group('prefix', () {
    test('publish adds the prefix', () async {
      await spy.subscribe('#', Qos.atMostOnce);
      final next = spy.messages.first;

      await client.connect();
      await client.publish(
        BedayaTopics.telemetry,
        const Telemetry({'temp': 24.5}),
        params: {'deviceId': 'a1'},
      );

      final m = await next;
      print('  on the wire: ${m.topic}  ${utf8.decode(m.payload)}');
      expect(m.topic, 'bedaya/a1/telemetry');
    });

    test('a custom prefix is used', () async {
      final custom = BedayaClient(
        const ClientConfig(host: 'memory', topicPrefix: 'acme/home'),
        transport: MemoryTransport(broker),
      );
      await spy.subscribe('#', Qos.atMostOnce);
      final next = spy.messages.first;

      await custom.connect();
      await custom.publish(
        BedayaTopics.telemetry,
        const Telemetry({}),
        params: {'deviceId': 'a1'},
      );
      expect((await next).topic, 'acme/home/a1/telemetry');
    });

    test('an empty prefix adds nothing', () async {
      final bare = BedayaClient(
        const ClientConfig(host: 'memory', topicPrefix: ''),
        transport: MemoryTransport(broker),
      );
      await spy.subscribe('#', Qos.atMostOnce);
      final next = spy.messages.first;

      await bare.connect();
      await bare.publish(
        BedayaTopics.telemetry,
        const Telemetry({}),
        params: {'deviceId': 'a1'},
      );
      expect((await next).topic, 'a1/telemetry');
    });
  });

  group('subscribe', () {
    setUp(() => client.connect());

    test('decodes messages and reads the params', () async {
      final next = client.subscribe(BedayaTopics.telemetry).first;
      await spy.publish(
        RawMessage('bedaya/a7/telemetry', utf8.encode('{"temp":30}')),
      );

      final m = await next;
      print('  got: $m  params: ${m.params}');
      expect(m.value['temp'], 30);
      expect(m.params, {'deviceId': 'a7'});
    });

    test('fixed params filter out other devices', () async {
      final got = <String>[];
      client
          .subscribe(BedayaTopics.telemetry, params: {'deviceId': 'a1'})
          .listen((m) => got.add(m.params['deviceId']!));
      print('  transport filters: ${transport.subscriptions.keys}');

      await spy.publish(RawMessage('bedaya/a2/telemetry', utf8.encode('{}')));
      await spy.publish(RawMessage('bedaya/a1/telemetry', utf8.encode('{}')));
      await pumpEventQueue();

      expect(got, ['a1']);
    });

    test('ignores topics without the prefix', () async {
      final got = <Object>[];
      client.subscribe(BedayaTopics.telemetry).listen(got.add);
      await spy.publish(RawMessage('other/a1/telemetry', utf8.encode('{}')));
      await pumpEventQueue();
      expect(got, isEmpty);
    });

    test('a bad payload is skipped and reported on errors', () async {
      final got = <Object>[];
      final errors = <BedayaException>[];
      client.subscribe(BedayaTopics.status).listen(got.add);
      client.errors.listen(errors.add);

      await spy.publish(RawMessage('bedaya/a1/status', utf8.encode('oops')));
      await spy.publish(
        RawMessage('bedaya/a1/status', utf8.encode('{"online":true}')),
      );
      await pumpEventQueue();

      print('  errors: $errors');
      expect(got, hasLength(1));
      expect(errors.single, isA<DecodeException>());
    });

    test('subscribes once per filter, and unsubscribes after the last '
        'listener', () async {
      final a = client.subscribe(BedayaTopics.telemetry).listen((_) {});
      final b = client.subscribe(BedayaTopics.telemetry).listen((_) {});
      expect(transport.subscriptions.keys, ['bedaya/+/telemetry']);

      await a.cancel();
      expect(transport.subscriptions, isNotEmpty);
      await b.cancel();
      expect(transport.subscriptions, isEmpty);
    });
  });

  test('listening before connect subscribes on connect', () async {
    final next = client.subscribe(BedayaTopics.telemetry).first;
    expect(transport.subscriptions, isEmpty);

    await client.connect();
    print('  after connect: ${transport.subscriptions}');
    expect(transport.subscriptions.keys, ['bedaya/+/telemetry']);

    await spy.publish(RawMessage('bedaya/a1/telemetry', utf8.encode('{}')));
    expect((await next).params, {'deviceId': 'a1'});
  });

  test('publish throws when offline with the queue turned off', () {
    final noQueue = BedayaClient(
      const ClientConfig(host: 'memory', maxQueueSize: 0),
      transport: transport,
    );
    expect(
      () => noQueue.publish(
        BedayaTopics.commandAck, // OfflinePolicy.queue
        CommandAck.success(Command('ping')),
        params: {'deviceId': 'a1'},
      ),
      throwsA(isA<ConnectionException>()),
    );
  });

  group('offline policy', () {
    late List<String> wire;

    setUp(() async {
      wire = [];
      await spy.subscribe('#', Qos.atMostOnce);
      spy.messages.listen((m) => wire.add(m.topic));
    });

    Topic<Telemetry> topic(OfflinePolicy offline) => Topic.json(
      '{deviceId}/reading',
      fromJson: Telemetry.fromJson,
      toJson: (t) => t.toJson(),
      offline: offline,
    );

    test('queue (the default): sent after connect', () async {
      expect(topic(OfflinePolicy.queue).offline, OfflinePolicy.queue);
      await client.publish(
        topic(OfflinePolicy.queue),
        const Telemetry({'n': 1}),
        params: {'deviceId': 'a1'},
      );
      expect(client.queuedMessages, 1);
      await client.connect();
      await pumpEventQueue();
      expect(wire, ['bedaya/a1/reading']);
    });

    test('drop: completes, and is never sent', () async {
      await client.publish(
        topic(OfflinePolicy.drop),
        const Telemetry({'n': 1}),
        params: {'deviceId': 'a1'},
      );
      expect(client.queuedMessages, 0);
      await client.connect();
      await pumpEventQueue();
      expect(wire, isEmpty);
    });

    test('fail: throws, and is never sent', () async {
      await expectLater(
        client.publish(
          topic(OfflinePolicy.fail),
          const Telemetry({'n': 1}),
          params: {'deviceId': 'a1'},
        ),
        throwsA(isA<ConnectionException>()),
      );
      expect(client.queuedMessages, 0);
      await client.connect();
      await pumpEventQueue();
      expect(wire, isEmpty);
    });

    test('drop also applies when the connection drops during publish',
        () async {
      await client.connect();
      transport.drop();
      await client.publish(
        topic(OfflinePolicy.drop),
        const Telemetry({'n': 1}),
        params: {'deviceId': 'a1'},
      );
      expect(client.queuedMessages, 0);
      await client.disconnect(); // stop the reconnect timer
    });

    test('the standard topics', () {
      expect(BedayaTopics.telemetry.offline, OfflinePolicy.drop);
      expect(BedayaTopics.status.offline, OfflinePolicy.drop);
      expect(BedayaTopics.command.offline, OfflinePolicy.fail);
      expect(BedayaTopics.commandAck.offline, OfflinePolicy.queue);
    });

    test('telemetry published offline is not replayed after a reconnect',
        () async {
      await client.publish(
        BedayaTopics.telemetry,
        const Telemetry({'temp': 20}),
        params: {'deviceId': 'a1'},
      );
      await client.connect();
      await pumpEventQueue();
      expect(wire, isEmpty);
    });
  });

  group('publish with a deadline', () {
    test('offline: waits, then throws PublishDeadlineException and '
        'never sends it', () async {
      final wire = <String>[];
      await spy.subscribe('#', Qos.atMostOnce);
      spy.messages.listen((m) => wire.add(m.topic));

      await expectLater(
        client.publish(
          BedayaTopics.commandAck,
          CommandAck.success(Command('ping')),
          params: {'deviceId': 'a1'},
          deadline: DateTime.now().add(const Duration(milliseconds: 50)),
        ),
        throwsA(isA<PublishDeadlineException>()),
      );
      expect(client.queuedMessages, 0);
      await client.connect();
      await pumpEventQueue();
      expect(wire, isEmpty);
    });

    test('a deadline already past throws even when connected', () async {
      await client.connect();
      await expectLater(
        client.publish(
          BedayaTopics.commandAck,
          CommandAck.success(Command('ping')),
          params: {'deviceId': 'a1'},
          deadline: DateTime.now().subtract(const Duration(seconds: 1)),
        ),
        throwsA(isA<PublishDeadlineException>()),
      );
    });
  });

  test('device() rejects a bad id', () {
    expect(() => client.device('a/1'), throwsArgumentError);
  });
}
