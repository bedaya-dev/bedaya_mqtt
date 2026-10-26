/// Test helpers: an in-memory MQTT broker, so tests run fast and without a
/// network.
///
/// ```dart
/// import 'package:bedaya_mqtt/bedaya_mqtt.dart';
/// import 'package:bedaya_mqtt/testing.dart';
/// import 'package:test/test.dart';
///
/// void main() {
///   test('the controller gets the telemetry', () async {
///     final broker = MemoryBroker();
///     final device = BedayaClient(ClientConfig(host: 'memory'),
///         transport: MemoryTransport(broker));
///     final controller = BedayaClient(ClientConfig(host: 'memory'),
///         transport: MemoryTransport(broker));
///     final me = device.asDevice('a1');
///     await device.connect();
///     await controller.connect();
///
///     final next = controller.device('a1').on(BedayaTopics.telemetry).first;
///     await me.publish(BedayaTopics.telemetry, Telemetry({'temp': 20}));
///     expect((await next)['temp'], 20);
///   });
/// }
/// ```
///
/// To test failures:
/// - `transport.drop()`: the connection is lost (the Last Will is sent),
///   and the client reconnects.
/// - `broker.stop()` / `broker.start()`: the broker goes down and comes
///   back.
///
/// [MqttTransport] and [RawMessage] are exported too, for writing your own
/// transport.
library;

export 'src/transport/memory_transport.dart';
export 'src/transport/mqtt_transport.dart';
export 'src/transport/raw_message.dart';
