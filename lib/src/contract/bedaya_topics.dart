import '../topic/topic.dart';
import '../transport/qos.dart';
import 'messages/command.dart';
import 'messages/command_ack.dart';
import 'messages/device_status.dart';
import 'messages/telemetry.dart';

/// The standard Bedaya topics. Firmware uses the same ones.
///
/// The patterns have no prefix. The client adds `ClientConfig.topicPrefix`
/// (like `bedaya`) in front, so `{deviceId}/telemetry` goes on the wire as
/// `bedaya/a1/telemetry`.
abstract final class BedayaTopics {
  /// Sensor readings from a device. QoS 0: a lost reading is replaced by
  /// the next one.
  static final Topic<Telemetry> telemetry = Topic.json(
    '{deviceId}/telemetry',
    fromJson: Telemetry.fromJson,
    toJson: (t) => t.toJson(),
    qos: Qos.atMostOnce,
  );

  /// Online or offline. Retained, so a new controller sees it right away.
  static final Topic<DeviceStatus> status = Topic.json(
    '{deviceId}/status',
    fromJson: DeviceStatus.fromJson,
    toJson: (s) => s.toJson(),
    retained: true,
  );

  /// Commands from a controller to a device.
  static final Topic<Command> command = Topic.json(
    '{deviceId}/command',
    fromJson: Command.fromJson,
    toJson: (c) => c.toJson(),
  );

  /// A device's answers to commands.
  static final Topic<CommandAck> commandAck = Topic.json(
    '{deviceId}/command/ack',
    fromJson: CommandAck.fromJson,
    toJson: (a) => a.toJson(),
  );
}
