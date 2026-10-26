import 'dart:async';
import 'dart:math';

import 'backoff.dart';

/// Retries a connection after a drop, waiting longer each time.
///
/// It knows nothing about MQTT: it calls [connect] until it succeeds, and
/// reports progress through callbacks. The client decides what "connected"
/// means (resubscribe, drain the queue, ...).
class ReconnectManager {
  ReconnectManager({
    required this.policy,
    required this.connect,
    required this.abandon,
    required this.onRetryScheduled,
    required this.onReconnected,
    required this.onGaveUp,
    this._random,
  });

  final ReconnectPolicy policy;

  /// Tries to connect once. Throws on failure.
  final Future<void> Function() connect;

  /// Closes a connection that succeeded after [stop] was called.
  final Future<void> Function() abandon;

  /// A retry will run after `delay`.
  final void Function(int attempt, Duration delay) onRetryScheduled;

  /// A retry worked.
  final void Function() onReconnected;

  /// [ReconnectPolicy.maxAttempts] retries failed. [error] is the last one.
  final void Function(Object? error) onGaveUp;

  final Random? _random;
  Timer? _timer;
  bool _running = false;
  int _attempt = 0;
  Object? _lastError;

  /// Whether retries are in progress.
  bool get isRunning => _running;

  /// Starts retrying. Does nothing if already running or if the policy is
  /// disabled.
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
      if (_running) _scheduleNext();
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
