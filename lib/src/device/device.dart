import 'dart:async';

import '../client/bedaya_client.dart';
import '../client/topic_message.dart';
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
/// It is cheap to create: each stream subscribes when you listen and
/// unsubscribes when you cancel. The one exception is the ack subscription
/// of [send], which the client keeps per device after the first command
/// (see [send]).
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
  /// Step by step: listen on `{deviceId}/command/ack`, wait until the
  /// broker has confirmed that subscription (SUBACK), publish the command
  /// on `{deviceId}/command`, wait for an ack with the same id.
  ///
  /// The ack subscription is made on the first command to this device and
  /// then kept until the client is closed, so later commands don't wait for
  /// it again. After a reconnect, a command waits until it is restored.
  ///
  /// If the broker refuses the ack subscription (SUBACK 0x80), this throws
  /// that [SubscriptionException] at once and the command is **not** sent,
  /// since its ack could never arrive. (Mosquitto accepts a subscription
  /// its ACL refuses, so there the command goes out and times out.)
  ///
  /// [timeout] covers the whole round trip: waiting for the connection,
  /// the broker's confirmation, and the device's ack. If the client is
  /// offline, the command is **not** queued: it waits for the connection,
  /// but only within [timeout]. Once this throws a
  /// [CommandTimeoutException], the command is never sent again.
  ///
  /// A timeout means the outcome is **unknown**, not that nothing
  /// happened: the command may have reached the device and only the
  /// confirmation or the ack was late. So prefer commands that set a state
  /// (`Command.relay(on: true)`) over ones that flip it, and let devices
  /// ignore a [Command.id] they already handled.
  ///
  /// Returns the ack even if it says the command failed; check
  /// [CommandAck.ok]. Throws a [CommandTimeoutException] if the command
  /// could not be sent, or no ack arrived, within [timeout], a
  /// [SubscriptionException] if the broker refused the ack subscription,
  /// and a [ConnectionException] if the client is closed meanwhile.
  Future<CommandAck> send(
    Command command, {
    Duration timeout = const Duration(seconds: 10),
  }) async {
    final deadline = DateTime.now().add(timeout);
    // Listen before publishing, so a fast answer is not missed.
    final acks = _AckListener.of(_client, id);
    final ack = acks.expect(command.id);

    try {
      try {
        await publishAfterSubscribed(
          _client,
          BedayaTopics.command,
          command,
          ackTopic: BedayaTopics.commandAck,
          params: _params,
          deadline: deadline,
        );
      } on PublishDeadlineException catch (e) {
        throw CommandTimeoutException(
          'Device "$id": "${command.name}" (${command.id}) timed out after '
          '$timeout: ${e.message}',
          e,
        );
      }

      final remaining = deadline.difference(DateTime.now());
      return await ack.timeout(
        remaining.isNegative ? Duration.zero : remaining,
        onTimeout: () => throw CommandTimeoutException(
          'Device "$id" did not answer "${command.name}" '
          '(${command.id}) within $timeout',
        ),
      );
    } finally {
      acks.forget(command.id);
    }
  }
}

/// The ack subscription of one device on one client, shared by every
/// command sent to it. Made on the first command and kept until the client
/// is closed, so the next command doesn't wait for another SUBACK.
class _AckListener {
  _AckListener(BedayaClient client, String deviceId) {
    _acks = client
        .subscribe(BedayaTopics.commandAck, params: {'deviceId': deviceId})
        .listen((m) => _waiting.remove(m.value.commandId)?.complete(m.value));
  }

  /// One per client and device id.
  static final _all = Expando<Map<String, _AckListener>>();

  static _AckListener of(BedayaClient client, String deviceId) {
    var byDevice = _all[client];
    if (byDevice == null) {
      final created = byDevice = {};
      _all[client] = created;
      onClientClose(client, () async {
        _all[client] = null;
        for (final listener in created.values) {
          await listener._acks.cancel();
        }
      });
    }
    return byDevice[deviceId] ??= _AckListener(client, deviceId);
  }

  late final StreamSubscription<TopicMessage<CommandAck>> _acks;

  /// The commands waiting for their ack, by command id.
  final Map<String, Completer<CommandAck>> _waiting = {};

  /// Starts waiting for the ack of [commandId].
  Future<CommandAck> expect(String commandId) =>
      (_waiting[commandId] = Completer<CommandAck>()).future;

  /// Stops waiting for [commandId] (answered, timed out or failed).
  void forget(String commandId) => _waiting.remove(commandId);
}

/// Old name of [DeviceRemote].
@Deprecated('Use DeviceRemote. Device will be removed in 0.3.0.')
typedef Device = DeviceRemote;
