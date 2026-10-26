import 'dart:convert';

import 'package:bedaya_mqtt/bedaya_mqtt.dart';
import 'package:bedaya_mqtt/testing.dart';
import 'package:test/test.dart';

void main() {
  late MemoryBroker broker;
  late MemoryTransport transport;
  late List<BedayaLogRecord> records;

  BedayaClient make({bool retryFirstConnect = false}) => BedayaClient(
    ClientConfig(
      host: 'memory',
      reconnect: const ReconnectPolicy(
        initialDelay: Duration(milliseconds: 10),
        jitter: 0,
      ),
      retryFirstConnect: retryFirstConnect,
      logger: records.add,
    ),
    transport: transport,
  );

  /// The messages logged at [level], in order.
  List<String> at(BedayaLogLevel level) => [
    for (final r in records)
      if (r.level == level) r.message,
  ];

  setUp(() {
    broker = MemoryBroker();
    transport = MemoryTransport(broker);
    records = [];
  });

  tearDown(() {
    print('  trace:\n    ${records.join('\n    ')}');
  });

  test('no logger: nothing is traced, nothing breaks', () async {
    final quiet = BedayaClient(
      const ClientConfig(host: 'memory'),
      transport: transport,
    );
    await quiet.connect();
    await quiet.publish(
      BedayaTopics.telemetry,
      const Telemetry({}),
      params: {'deviceId': 'a1'},
    );
    expect(records, isEmpty);
  });

  test('connect, subscribe, publish and receive', () async {
    final client = make();
    await client.connect();
    client.subscribe(BedayaTopics.telemetry).listen((_) {});
    await client.publish(
      BedayaTopics.telemetry,
      const Telemetry({'temp': 24.5}),
      params: {'deviceId': 'a1'},
    );
    await pumpEventQueue();

    expect(at(BedayaLogLevel.info), ['connecting to memory', 'connected']);
    expect(at(BedayaLogLevel.debug), [
      'subscribe bedaya/+/telemetry (atMostOnce)',
      'publish bedaya/a1/telemetry {"temp":24.5}',
      'received bedaya/a1/telemetry {"temp":24.5}',
    ]);
  });

  test('a dropped connection: lost, reconnect, restore', () async {
    final client = make();
    client.subscribe(BedayaTopics.telemetry).listen((_) {});
    await client.connect();
    records.clear();

    transport.drop();
    await client.connectionStates.firstWhere((s) => s is Connected);
    await pumpEventQueue();

    expect(at(BedayaLogLevel.warning), ['connection lost']);
    expect(at(BedayaLogLevel.info), [
      'reconnect #1 in 0:00:00.010000',
      'connected',
    ]);
    expect(at(BedayaLogLevel.debug), [
      'restoring 1 subscriptions',
      'subscribe bedaya/+/telemetry (atMostOnce)',
    ]);
  });

  test('offline: queued, then sent after connecting', () async {
    final client = make();
    await client.publish(
      BedayaTopics.telemetry,
      const Telemetry({}),
      params: {'deviceId': 'a1'},
    );
    await client.connect();

    expect(
      at(BedayaLogLevel.debug).first,
      'offline: queued bedaya/a1/telemetry (1 waiting)',
    );
    expect(at(BedayaLogLevel.info), contains('sending 1 queued messages'));
  });

  test('a failed connect is an error, with its cause', () async {
    final client = make();
    broker.stop();
    await expectLater(client.connect(), throwsA(isA<ConnectionException>()));

    final error = records.singleWhere((r) => r.level == BedayaLogLevel.error);
    expect(error.message, 'connect failed');
    expect(error.error, isA<ConnectionException>());
  });

  test('retries are traced, with each failure', () async {
    final client = make(retryFirstConnect: true);
    broker.stop();
    final connecting = client.connect();
    await client.connectionStates.firstWhere(
      (s) => s is Reconnecting && s.attempt == 2,
    );
    broker.start();
    await connecting;

    expect(at(BedayaLogLevel.warning), [
      'connect failed, retrying',
      'reconnect #1 failed',
      ...at(BedayaLogLevel.warning).skip(2),
    ]);
    expect(at(BedayaLogLevel.info).last, 'connected');
  });

  test('a bad payload is a warning', () async {
    final client = make();
    await client.connect();
    client.subscribe(BedayaTopics.status).listen((_) {});
    final spy = MemoryTransport(broker);
    await spy.connect();
    await spy.publish(RawMessage('bedaya/a1/status', utf8.encode('oops')));
    await pumpEventQueue();

    expect(at(BedayaLogLevel.warning), ['Payload is not valid UTF-8 JSON']);
  });

  test('the device role traces its Last Will and status', () async {
    final client = make();
    client.asDevice('a1');
    await client.connect();
    await client.disconnect();

    expect(at(BedayaLogLevel.info), [
      'acting as device "a1" (Last Will: offline on bedaya/a1/status)',
      'connecting to memory',
      'connected',
      'device "a1" is online',
      'disconnecting',
      'device "a1" is offline',
      'disconnected',
    ]);
  });

  test('a record prints as one line', () {
    final r = BedayaLogRecord(
      BedayaLogLevel.warning,
      'connection lost',
      error: 'socket closed',
      time: DateTime(2026, 10, 26, 12, 0, 1, 204),
    );
    expect(
      r.toString(),
      '12:00:01.204 [warning] connection lost (socket closed)',
    );
  });
}
