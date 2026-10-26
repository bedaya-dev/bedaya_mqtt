/// Where the client's connection to the broker stands.
///
/// The normal life of a client:
///
/// ```text
///  Disconnected ──connect()──▶ Connecting ──▶ Connected
///                                  │              │ connection lost
///                                  │ fails        ▼
///                                  ▼          Reconnecting(1) ─▶ Reconnecting(2) ─▶ … ─▶ Connected
///                               Failed                                  │ maxAttempts reached
///                                                                       ▼
///                                                                     Failed
/// ```
///
/// `disconnect()` goes back to [Disconnected] from any state.
///
/// Listen to `client.connectionStates` and switch on the type:
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
///
/// (Named `MqttConnectionState`, not `ConnectionState`, so it doesn't clash
/// with Flutter's `ConnectionState`.)
sealed class MqttConnectionState {
  /// The states are the subclasses; this is never created directly.
  const MqttConnectionState();
}

/// Not connected, and not trying to connect. The state before `connect()`
/// and after `disconnect()`.
final class Disconnected extends MqttConnectionState {
  /// The disconnected state.
  const Disconnected();

  @override
  String toString() => 'Disconnected()';
}

/// The first connection attempt is in progress.
final class Connecting extends MqttConnectionState {
  /// The connecting state.
  const Connecting();

  @override
  String toString() => 'Connecting()';
}

/// Connected to the broker. Subscriptions are active and publishing goes
/// straight out.
final class Connected extends MqttConnectionState {
  /// The connected state.
  const Connected();

  @override
  String toString() => 'Connected()';
}

/// The connection dropped (or the first connect failed with
/// `retryFirstConnect`), and the client will try again after [nextDelay].
///
/// Messages you publish now go into the offline queue.
final class Reconnecting extends MqttConnectionState {
  /// The reconnecting state, before attempt number [attempt].
  const Reconnecting({required this.attempt, required this.nextDelay});

  /// 1 for the first retry, 2 for the second, and so on.
  final int attempt;

  /// How long until this attempt runs.
  final Duration nextDelay;

  @override
  String toString() => 'Reconnecting(attempt: $attempt, nextDelay: $nextDelay)';
}

/// The client stopped trying: the first connect failed, or reconnecting
/// ran out of attempts. Call `connect()` to try again.
final class Failed extends MqttConnectionState {
  /// The failed state, because of [error].
  const Failed(this.error);

  /// Why it failed, usually a `ConnectionException`.
  final Object error;

  @override
  String toString() => 'Failed($error)';
}
