import 'dart:async';
import 'dart:math';

import 'backoff.dart';

/// Retries a connection after a drop, waiting longer each time.
///
/// It knows nothing about MQTT. It only calls [connect] until it works,
/// and reports what happens through callbacks. The client decides what
/// "connected" means (resubscribe, publish online, send the queue).
///
/// One round of retries, step by step:
///
/// ```text
/// start()
///   → onRetryScheduled(1, 1s) … wait 1s … connect() fails → onAttemptFailed(1, e)
///   → onRetryScheduled(2, 2s) … wait 2s … connect() works → onReconnected()
/// ```
///
/// If [ReconnectPolicy.maxAttempts] runs out, [onGaveUp] is called
/// instead. [stop] ends the round at any point.
class ReconnectManager {
  /// Creates a manager. Nothing happens until [start].
  ///
  /// Pass `random` to make the jitter predictable in tests.
  ReconnectManager({
    required this.policy,
    required this.connect,
    required this.abandon,
    required this.onRetryScheduled,
    required this.onReconnected,
    required this.onGaveUp,
    this.onAttemptFailed,
    this._random,
  });

  /// How long to wait between attempts, and when to give up.
  final ReconnectPolicy policy;

  /// Tries to connect once. Throws on failure.
  final Future<void> Function() connect;

  /// Closes a connection that succeeded after [stop] was called.
  final Future<void> Function() abandon;

  /// Attempt number `attempt` (1, 2, …) will run after `delay`.
  final void Function(int attempt, Duration delay) onRetryScheduled;

  /// Attempt number `attempt` failed with `error`. Another one follows,
  /// unless the policy gives up.
  final void Function(int attempt, Object error)? onAttemptFailed;

  /// An attempt worked. The round is over.
  final void Function() onReconnected;

  /// [ReconnectPolicy.maxAttempts] attempts failed. `error` is the last
  /// one. The round is over.
  final void Function(Object? error) onGaveUp;

  final Random? _random;
  Timer? _timer;
  bool _running = false;
  int _attempt = 0;
  Object? _lastError;

  /// Whether a round of retries is in progress.
  bool get isRunning => _running;

  /// Starts a round of retries. Does nothing if one is already running, or
  /// if the policy is `ReconnectPolicy.never()`.
  void start() {
    if (_running || !policy.enabled) return;
    _running = true;
    _attempt = 0;
    _lastError = null;
    _scheduleNext();
  }

  /// Stops retrying, for example because the app called `disconnect()`.
  void stop() {
    _running = false;
    _timer?.cancel();
    _timer = null;
  }

  void _scheduleNext() {
    _attempt++;
    final max = policy.maxAttempts;
    if (max != null && _attempt > max) {
      _running = false;
      onGaveUp(_lastError);
      return;
    }
    final delay = policy.delayFor(_attempt, random: _random);
    onRetryScheduled(_attempt, delay);
    _timer = Timer(delay, _try);
  }

  Future<void> _try() async {
    _timer = null;
    try {
      await connect();
    } catch (e) {
      _lastError = e;
      if (!_running) return;
      onAttemptFailed?.call(_attempt, e);
      _scheduleNext();
      return;
    }

    if (!_running) {
      // stop() was called while this attempt was connecting.
      await abandon();
      return;
    }
    _running = false;
    onReconnected();
  }
}
