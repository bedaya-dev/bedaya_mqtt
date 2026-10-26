/// Typed, reliable MQTT for Dart: reconnect, offline queue, typed topics.
///
/// ## Where to start
///
/// 1. Make a [BedayaClient] with a [ClientConfig] and `connect()` it.
/// 2. Pick a role:
///    - **Controller** (an app watching boards): `client.device(id)` gives a
///      [Device]. Listen with `on(topic)` and `status`, command with
///      `send(Command)`.
///    - **Device** (a board, a script, a phone): `client.asDevice(id)`
///      gives a [DeviceEndpoint]. Call it before `connect()`. Publish with
///      `publish(topic, value)`, answer `commands` with `ack`.
/// 3. Use the ready-made [BedayaTopics] (telemetry, status, command,
///    command ack), or define your own [Topic] for any payload.
///
/// ```dart
/// final client = BedayaClient(ClientConfig(host: 'broker.hivemq.com'));
/// await client.connect();
/// client.device('a1').on(BedayaTopics.telemetry).listen((t) => print(t['temp']));
/// ```
///
/// ## The parts
///
/// | Part | Types |
/// |---|---|
/// | Client and settings | [BedayaClient], [ClientConfig], [MqttConnectionState] |
/// | Roles | [Device], [DeviceEndpoint], [DeviceRegistry] |
/// | Topics and payloads | [Topic], [TopicPattern], [MessageCodec], [JsonMessageCodec], [Qos] |
/// | Standard messages | [BedayaTopics], [Telemetry], [DeviceStatus], [Command], [CommandAck] |
/// | Reliability | [ReconnectPolicy], [MessageQueue], [MemoryQueue] |
/// | Errors | [BedayaException] and its subclasses |
/// | Tracing | [ClientConfig.logger], [BedayaLogRecord] |
///
/// For tests without a network, see `package:bedaya_mqtt/testing.dart`.
library;

export 'src/client/bedaya_client.dart';
export 'src/client/bedaya_log.dart';
export 'src/client/client_config.dart';
export 'src/client/mqtt_connection_state.dart';
export 'src/client/topic_message.dart';
export 'src/contract/bedaya_topics.dart';
export 'src/contract/messages/command.dart';
export 'src/contract/messages/command_ack.dart';
export 'src/contract/messages/device_status.dart';
export 'src/contract/messages/telemetry.dart';
export 'src/device/device.dart';
export 'src/device/device_endpoint.dart';
export 'src/device/device_registry.dart';
export 'src/errors.dart';
export 'src/queue/memory_queue.dart';
export 'src/queue/message_queue.dart';
export 'src/queue/queued_message.dart';
export 'src/reconnect/backoff.dart' show ReconnectPolicy;
export 'src/topic/codec.dart';
export 'src/topic/topic.dart';
export 'src/topic/topic_pattern.dart';
export 'src/transport/qos.dart';
