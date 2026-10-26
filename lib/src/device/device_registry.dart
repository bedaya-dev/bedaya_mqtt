import 'dart:async';

import '../client/bedaya_client.dart';
import '../client/topic_message.dart';
import '../contract/bedaya_topics.dart';

/// What the controller knows about one device.
class DeviceInfo {
  /// Creates the info. The registry does this; you only read them.
  const DeviceInfo({required this.id, required this.online, this.lastSeen});

  /// The device id, like `a1`.
  final String id;

  /// From the device's status topic, or `true` once it sends anything live.
  final bool online;

  /// When a live message from the device last arrived.
  ///
  /// `null` if the only thing known is the broker's retained status, which
  /// may be old.
  final DateTime? lastSeen;

  @override
  String toString() => 'DeviceInfo($id, online: $online, lastSeen: $lastSeen)';
}

/// Every device on the broker (under the client's prefix), with its online
/// status and when it was last seen.
///
/// Get it with `client.devices`. It listens to the status, telemetry and
/// command ack topics of all devices.
///
/// ```dart
/// client.devices.changes.listen((all) {
///   for (final d in all.values) print('${d.id}: ${d.online}');
/// });
/// ```
class DeviceRegistry {
  /// Use `client.devices` instead. [clock] replaces `DateTime.now` in
  /// tests.
  DeviceRegistry(BedayaClient client, {DateTime Function()? clock})
    : _clock = clock ?? DateTime.now {
    _subscriptions = [
      client
          .subscribe(BedayaTopics.status)
          .listen((m) => _update(m, online: m.value.online)),
      client.subscribe(BedayaTopics.telemetry).listen(_update),
      client.subscribe(BedayaTopics.commandAck).listen(_update),
    ];
  }

  final DateTime Function() _clock;
  final Map<String, DeviceInfo> _devices = {};
  final StreamController<Map<String, DeviceInfo>> _changes =
      StreamController.broadcast();
  late final List<StreamSubscription<Object?>> _subscriptions;

  /// Every known device by id, right now.
  Map<String, DeviceInfo> get devices => Map.unmodifiable(_devices);

  /// The device [id], or `null` if nothing was heard from it yet.
  DeviceInfo? operator [](String id) => _devices[id];

  /// The full map again, after every change.
  Stream<Map<String, DeviceInfo>> get changes => _changes.stream;

  /// Stops listening.
  Future<void> close() async {
    for (final s in _subscriptions) {
      await s.cancel();
    }
    await _changes.close();
  }

  /// [online] comes from a status message. Any other live message means
  /// the device is up.
  void _update(TopicMessage<Object?> message, {bool? online}) {
    final id = message.params['deviceId'];
    if (id == null) return;
    final previous = _devices[id];
    final live = !message.retained;

    _devices[id] = DeviceInfo(
      id: id,
      online: online ?? (live || (previous?.online ?? false)),
      lastSeen: live ? _clock() : previous?.lastSeen,
    );
    _changes.add(devices);
  }
}
