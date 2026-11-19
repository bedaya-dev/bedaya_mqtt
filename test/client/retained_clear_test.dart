import 'package:bedaya_mqtt/bedaya_mqtt.dart';
import 'package:bedaya_mqtt/testing.dart';
import 'package:test/test.dart';

/// An empty retained payload clears a retained topic. It is not a bad
/// message, so it must not reach the stream or `client.errors`.
void main() {
  late MemoryBroker broker;
  late BedayaClient client;
  late MemoryTransport raw;
  late List<BedayaException> errors;

  setUp(() async {
    broker = MemoryBroker();
    client = BedayaClient(
      const ClientConfig(host: 'memory'),
      transport: MemoryTransport(broker),
    );
    errors = [];
    client.errors.listen(errors.add);
    await client.connect();
    raw = MemoryTransport(broker);
    await raw.connect();
  });

  tearDown(() async {
    await client.close();
    await raw.disconnect();
  });

  test('clearing a retained status: no error, nothing on the stream', () async {
    await raw.publish(
      RawMessage(
        'bedaya/a1/status',
        DeviceStatus.onlineStatus.toJsonBytes(),
        retained: true,
      ),
    );
    final statuses = <DeviceStatus>[];
    client.device('a1').status.listen(statuses.add);
    await pumpEventQueue();
    expect(statuses, hasLength(1), reason: 'the retained status arrives');

    // Clear it, the MQTT way: an empty retained payload.
    await raw.publish(const RawMessage('bedaya/a1/status', [], retained: true));
    await pumpEventQueue();

    print('  statuses: $statuses, errors: $errors');
    expect(statuses, hasLength(1), reason: 'the clear is not a status');
    expect(errors, isEmpty);
    expect(broker.retained, isNot(contains('bedaya/a1/status')));
  });

  test('an empty payload on a non-retained topic is still a decode '
      'error', () async {
    client.subscribe(BedayaTopics.telemetry).listen((_) {});
    await pumpEventQueue();

    await raw.publish(const RawMessage('bedaya/a1/telemetry', []));
    await pumpEventQueue();

    print('  errors: $errors');
    expect(errors.single, isA<DecodeException>());
  });
}

extension on DeviceStatus {
  List<int> toJsonBytes() => BedayaTopics.status.codec.encode(this);
}
