import 'package:bedaya_mqtt/bedaya_mqtt.dart';
import 'package:bedaya_mqtt/testing.dart';
import 'package:test/test.dart';

void main() {
  late MemoryBroker broker;
  late BedayaClient controller;
  late DateTime now;

  BedayaClient deviceClient(String id, MemoryTransport transport) {
    final c = BedayaClient(
      const ClientConfig(host: 'memory', reconnect: ReconnectPolicy.never()),
      transport: transport,
    );
    c.asDevice(id);
    return c;
  }

  setUp(() async {
    broker = MemoryBroker();
    now = DateTime(2026, 10, 26, 12);
    controller = BedayaClient(
      const ClientConfig(host: 'memory'),
      transport: MemoryTransport(broker),
    );
    await controller.connect();
  });

  test('a device that connects shows up online, with lastSeen', () async {
    final registry = DeviceRegistry(controller, clock: () => now);
    await pumpEventQueue();

    await deviceClient('a1', MemoryTransport(broker)).connect();
    await pumpEventQueue();

    print('  ${registry['a1']}');
    expect(registry['a1']!.online, isTrue);
    expect(registry['a1']!.lastSeen, now);
  });

  test('a retained status gives online but no lastSeen', () async {
    // The device was online before the registry started.
    await deviceClient('a1', MemoryTransport(broker)).connect();
    await pumpEventQueue();

    final registry = DeviceRegistry(controller, clock: () => now);
    await pumpEventQueue();

    print('  ${registry['a1']}');
    expect(registry['a1']!.online, isTrue);
    expect(registry['a1']!.lastSeen, isNull);
  });

  test('goes offline on disconnect and on a dropped connection', () async {
    final registry = DeviceRegistry(controller, clock: () => now);
    await pumpEventQueue();

    final t1 = MemoryTransport(broker);
    final t2 = MemoryTransport(broker);
    final a1 = deviceClient('a1', t1);
    await a1.connect();
    await deviceClient('a2', t2).connect();
    await pumpEventQueue();

    await a1.disconnect(); // says offline itself
    t2.drop(); // the Last Will says offline
    await pumpEventQueue();

    print('  ${registry.devices.values.join('\n  ')}');
    expect(registry['a1']!.online, isFalse);
    expect(registry['a2']!.online, isFalse);
  });

  test('telemetry updates lastSeen', () async {
    final registry = DeviceRegistry(controller, clock: () => now);
    await pumpEventQueue();
    final device = deviceClient('a1', MemoryTransport(broker));
    final me = device.asDevice('a1');
    await device.connect();
    await pumpEventQueue();

    now = now.add(const Duration(minutes: 5));
    await me.publish(BedayaTopics.telemetry, const Telemetry({'temp': 1}));
    await pumpEventQueue();

    expect(registry['a1']!.lastSeen, DateTime(2026, 10, 26, 12, 5));
  });

  test('telemetry from a device with no status counts as online', () async {
    final registry = DeviceRegistry(controller, clock: () => now);
    await pumpEventQueue();

    await controller.publish(
      BedayaTopics.telemetry,
      const Telemetry({}),
      params: {'deviceId': 'raw1'},
    );
    await pumpEventQueue();

    expect(registry['raw1']!.online, isTrue);
  });

  test('changes emits the whole map', () async {
    final registry = DeviceRegistry(controller, clock: () => now);
    final snapshots = <Map<String, DeviceInfo>>[];
    registry.changes.listen(snapshots.add);
    await pumpEventQueue();

    await deviceClient('a1', MemoryTransport(broker)).connect();
    await deviceClient('a2', MemoryTransport(broker)).connect();
    await pumpEventQueue();

    print('  last snapshot: ${snapshots.last.keys}');
    expect(snapshots.last.keys, unorderedEquals(['a1', 'a2']));
  });

  test('client.devices returns one shared registry', () {
    expect(controller.devices, same(controller.devices));
  });
}
