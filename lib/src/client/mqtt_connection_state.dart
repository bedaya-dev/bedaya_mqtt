/// The client's connection to the broker, as the app sees it.
///
/// ```dart
/// client.connectionStates.listen((state) {
///   switch (state) {
///     case Connected():
///       print('online');
///     case Reconnecting(:final attempt, :final nextDelay):
///       print('retry #$attempt in $nextDelay');
///     case Failed(:final error):
///       print('failed: $error');
///     case Disconnected() || Connecting():
///       break;
///   }
/// });
/// ```
sealed class MqttConnectionState {
  const MqttConnectionState();
}

/// Not connected, and not trying to connect.
final class Disconnected extends MqttConnectionState {
  const Disconnected();

  @override
  String toString() => 'Disconnected()';
}

/// The first connection attempt is in progress.
final class Connecting extends MqttConnectionState {
  const Connecting();

  @override
  String toString() => 'Connecting()';
}

/// Connected to the broker.
final class Connected extends MqttConnectionState {
  const Connected();

  @override
  String toString() => 'Connected()';
}

/// The connection dropped, and the client will try again after
/// [nextDelay].
final class Reconnecting extends MqttConnectionState {
  const Reconnecting({required this.attempt, required this.nextDelay});

  /// 1 for the first retry, 2 for the second, and so on.
  final int attempt;

  final Duration nextDelay;

  @override
  String toString() => 'Reconnecting(attempt: $attempt, nextDelay: $nextDelay)';
}

/// The client stopped trying. [error] says why.
final class Failed extends MqttConnectionState {
  const Failed(this.error);

  final Object error;

  @override
  String toString() => 'Failed($error)';
}
