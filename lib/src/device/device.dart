import 'dart:async';

import '../client/bedaya_client.dart';
import '../contract/bedaya_topics.dart';
import '../contract/messages/command.dart';
import '../contract/messages/command_ack.dart';
import '../contract/messages/device_status.dart';
import '../errors.dart';
import '../topic/topic.dart';

/// A remote for one device: watch it and send it commands.
///
/// ```dart
/// final deviceRemote = client.device('device1');
/// deviceRemote.status.listen((s) => print(s.online));
/// deviceRemote.on(BedayaTopics.telemetry).listen((t) => print(t['temp']));
/// final ack = await deviceRemote.send(Command.relay(on: true));
/// ```
///
/// It is cheap to create and holds no subscriptions by itself: each stream
/// subscribes when you listen and unsubscribes when you cancel.
class DeviceRemote {
  /// Use `client.device(id)` instead.
  DeviceRemote(this._client, this.id);

  final BedayaClient _client;

  /// The device id, like `device1`.
  final String id;

  Map<String, String> get _params => {'deviceId': id};

  /// Messages this device publishes on [topic].
  ///
  /// [topic] must have a `{deviceId}` param. Each call returns a new
  /// stream.
  Stream<T> on<T>(Topic<T> topic) =>
      _client.subscribe(topic, params: _params).map((m) => m.value);

  /// Online and offline updates. The current status arrives first, because
  /// the status topic is retained.
  Stream<DeviceStatus> get status => on(BedayaTopics.status);

  /// Sends [command] and waits for the device's ack.
  ///
  /// Step by step: listen on `{deviceId}/command/ack`, publish the command
  /// on `{deviceId}/command`, wait for an ack with the same id, stop
  /// listening.
  ///
  /// Returns the ack even if it says the command failed; check
  /// [CommandAck.ok]. Throws a [CommandTimeoutException] if no ack arrives
  /// within [timeout], and a [ConnectionException] if offline with the
  /// queue turned off. (With the queue on, an offline command waits in the
  /// queue, and the timeout keeps counting.)
  Future<CommandAck> send(
    Command command, {
    Duration timeout = const Duration(seconds: 10),
  }) async {
    final ack = Completer<CommandAck>();
    // Listen before publishing, so a fast answer is not missed.
    final acks = on(BedayaTopics.commandAck).listen((a) {
      if (a.commandId == command.id && !ack.isCompleted) ack.complete(a);
    });

    try {
      await _client.publish(BedayaTopics.command, command, params: _params);
      return await ack.future.timeout(
        timeout,
        onTimeout: () => throw CommandTimeoutException(
          'Device "$id" did not answer "${command.name}" '
          '(${command.id}) within $timeout',
        ),
      );
    } finally {
      await acks.cancel();
    }
  }
}

/// Old name of [DeviceRemote].
@Deprecated('Use DeviceRemote. Device will be removed in 0.3.0.')
typedef Device = DeviceRemote;
