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
  /// the next one. Dropped while offline ([OfflinePolicy.drop]): a reading
  /// replayed after a reconnect would look current.
  static final Topic<Telemetry> telemetry = Topic.json(
    '{deviceId}/telemetry',
    fromJson: Telemetry.fromJson,
    toJson: (t) => t.toJson(),
    qos: Qos.atMostOnce,
    offline: OfflinePolicy.drop,
  );

  /// Online or offline. Retained, so a new controller sees it right away.
  /// Dropped while offline ([OfflinePolicy.drop]): the client publishes
  /// "online" itself on every connect, and the Last Will covers "offline".
  static final Topic<DeviceStatus> status = Topic.json(
    '{deviceId}/status',
    fromJson: DeviceStatus.fromJson,
    toJson: (s) => s.toJson(),
    retained: true,
    offline: OfflinePolicy.drop,
  );

  /// Commands from a controller to a device. Never queued
  /// ([OfflinePolicy.fail]): use `DeviceRemote.send`, which waits for the
  /// connection only until its timeout.
  static final Topic<Command> command = Topic.json(
    '{deviceId}/command',
    fromJson: Command.fromJson,
    toJson: (c) => c.toJson(),
    offline: OfflinePolicy.fail,
  );

  /// A device's answers to commands.
  static final Topic<CommandAck> commandAck = Topic.json(
    '{deviceId}/command/ack',
    fromJson: CommandAck.fromJson,
    toJson: (a) => a.toJson(),
  );
}
