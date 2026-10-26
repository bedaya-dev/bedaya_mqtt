import 'dart:math';

/// How the client retries after the connection drops.
///
/// The wait before attempt `n` is `initialDelay * multiplier^(n-1)`, capped
/// at [maxDelay], then moved up or down by up to [jitter] (a fraction) so
/// that many devices don't all retry at the same moment after a broker
/// restart.
///
/// With the defaults: about 1s, 2s, 4s, 8s, 16s, 30s, 30s, ...
class ReconnectPolicy {
  const ReconnectPolicy({
    this.initialDelay = const Duration(seconds: 1),
    this.maxDelay = const Duration(seconds: 30),
    this.multiplier = 2.0,
    this.jitter = 0.2,
    this.maxAttempts,
  }) : assert(multiplier >= 1),
       assert(jitter >= 0 && jitter <= 1);

  /// Never reconnect. A drop leaves the client `Disconnected`.
  const ReconnectPolicy.never()
    : initialDelay = Duration.zero,
      maxDelay = Duration.zero,
      multiplier = 1,
      jitter = 0,
      maxAttempts = 0;

  /// The wait before the first retry.
  final Duration initialDelay;

  /// The longest wait between two retries.
  final Duration maxDelay;

  /// How much longer each wait is than the one before.
  final double multiplier;

  /// How much each wait may vary, from 0 (exact) to 1 (anywhere from zero
  /// to double). 0.2 means ±20%.
  final double jitter;

  /// Gives up after this many failed retries. `null` retries forever.
  final int? maxAttempts;

  /// Whether a drop should trigger retries at all.
  bool get enabled => maxAttempts != 0;

  /// The wait before retry number [attempt] (1 for the first).
  ///
  /// Pass [random] to make the jitter predictable in tests.
  Duration delayFor(int attempt, {Random? random}) {
    if (attempt < 1) {
      throw ArgumentError.value(attempt, 'attempt', 'must be ≥ 1');
    }

    final maxMicros = maxDelay.inMicroseconds;
    // pow can overflow to infinity for big attempts; min() caps it first.
    final base = min(
      initialDelay.inMicroseconds * pow(multiplier, attempt - 1),
      maxMicros.toDouble(),
    );
    final spread = jitter == 0
        ? 1.0
        : 1 + jitter * (2 * (random ?? Random()).nextDouble() - 1);
    final micros = (base * spread).round().clamp(0, maxMicros);
    return Duration(microseconds: micros);
  }
}
