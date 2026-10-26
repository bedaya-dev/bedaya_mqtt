import '../client/bedaya_client.dart';
import '../contract/bedaya_topics.dart';
import '../contract/messages/command.dart';
import '../contract/messages/command_ack.dart';
import '../topic/topic.dart';

/// This client acting as a device: publish data and answer commands.
///
/// Get one with `client.asDevice(id)`, before connecting. The client then
/// publishes the device as online when it connects, and the broker
/// publishes it as offline (the Last Will) if the connection drops.
///
/// ```dart
/// final me = client.asDevice('a1');
/// await client.connect();
/// me.commands.listen((c) => me.ack(c));
/// await me.publish(BedayaTopics.telemetry, Telemetry({'temp': 24.5}));
/// ```
class DeviceEndpoint {
  DeviceEndpoint(this._client, this.id);

  final BedayaClient _client;

  /// This device's id, like `a1`.
  final String id;

  Map<String, String> get _params => {'deviceId': id};

  /// Publishes [value] on [topic] as this device.
  ///
  /// [topic] must have a `{deviceId}` param.
  Future<void> publish<T>(Topic<T> topic, T value) =>
      _client.publish(topic, value, params: _params);

  /// Commands sent to this device.
  Stream<Command> get commands => _client
      .subscribe(BedayaTopics.command, params: _params)
      .map((m) => m.value);

  /// Answers [command]: success, or failure when [error] is given.
  Future<void> ack(Command command, {String? error}) => publish(
    BedayaTopics.commandAck,
    error == null
        ? CommandAck.success(command)
        : CommandAck.failure(command, error),
  );
}
