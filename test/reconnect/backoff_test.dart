import 'dart:math';

import 'package:bedaya_mqtt/src/reconnect/backoff.dart';
import 'package:test/test.dart';

void main() {
  group('ReconnectPolicy.delayFor', () {
    const exact = ReconnectPolicy(jitter: 0);

    test('doubles each time, then stops at maxDelay', () {
      final delays = [for (var i = 1; i <= 8; i++) exact.delayFor(i)];
      print('  ${delays.map((d) => '${d.inSeconds}s').join(', ')}');
      expect(delays.map((d) => d.inSeconds), [1, 2, 4, 8, 16, 30, 30, 30]);
    });

    test('a huge attempt number does not overflow', () {
      expect(exact.delayFor(10000), const Duration(seconds: 30));
    });

    test('jitter stays within ±jitter of the base', () {
      const policy = ReconnectPolicy(jitter: 0.2);
      final random = Random(42);
      for (var i = 0; i < 200; i++) {
        final ms = policy.delayFor(3, random: random).inMilliseconds;
        expect(ms, inInclusiveRange(3200, 4800));
      }
    });

    test('jitter never goes above maxDelay', () {
      const policy = ReconnectPolicy(jitter: 1);
      final random = Random(1);
      for (var i = 0; i < 200; i++) {
        expect(
          policy.delayFor(20, random: random),
          lessThanOrEqualTo(const Duration(seconds: 30)),
        );
      }
    });

    test('the same seed gives the same delays', () {
      const policy = ReconnectPolicy();
      expect(
        policy.delayFor(2, random: Random(7)),
        policy.delayFor(2, random: Random(7)),
      );
    });

    test('attempt must be at least 1', () {
      expect(() => exact.delayFor(0), throwsArgumentError);
    });
  });

  group('enabled', () {
    test('is true by default, false for never()', () {
      expect(const ReconnectPolicy().enabled, isTrue);
      expect(const ReconnectPolicy(maxAttempts: 3).enabled, isTrue);
      expect(const ReconnectPolicy.never().enabled, isFalse);
    });
  });
}
