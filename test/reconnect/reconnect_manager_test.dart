import 'dart:async';

import 'package:bedaya_mqtt/src/reconnect/backoff.dart';
import 'package:bedaya_mqtt/src/reconnect/reconnect_manager.dart';
import 'package:test/test.dart';

void main() {
  const fast = ReconnectPolicy(
    initialDelay: Duration(milliseconds: 5),
    maxDelay: Duration(milliseconds: 20),
    jitter: 0,
  );

  /// A manager whose connect fails [failures] times, then works.
  ({ReconnectManager manager, List<String> log, Future<void> done}) build({
    ReconnectPolicy policy = fast,
    int failures = 0,
  }) {
    final log = <String>[];
    final done = Completer<void>();
    var calls = 0;
    final manager = ReconnectManager(
      policy: policy,
      connect: () async {
        calls++;
        if (calls <= failures) throw StateError('fail #$calls');
      },
      abandon: () async => log.add('abandon'),
      onRetryScheduled: (a, d) => log.add('retry $a in ${d.inMilliseconds}ms'),
      onReconnected: () {
        log.add('reconnected');
        done.complete();
      },
      onGaveUp: (e) {
        log.add('gave up: $e');
        done.complete();
      },
    );
    return (manager: manager, log: log, done: done.future);
  }

  test('retries with growing delays until it works', () async {
    final (:manager, :log, :done) = build(failures: 3);
    manager.start();
    await done;
    print('  ${log.join('\n  ')}');
    expect(log, [
      'retry 1 in 5ms',
      'retry 2 in 10ms',
      'retry 3 in 20ms',
      'retry 4 in 20ms',
      'reconnected',
    ]);
    expect(manager.isRunning, isFalse);
  });

  test('gives up after maxAttempts', () async {
    final (:manager, :log, :done) = build(
      policy: const ReconnectPolicy(
        initialDelay: Duration(milliseconds: 1),
        jitter: 0,
        maxAttempts: 2,
      ),
      failures: 99,
    );
    manager.start();
    await done;
    print('  ${log.join('\n  ')}');
    expect(log.last, startsWith('gave up: Bad state: fail #2'));
  });

  test('stop() cancels the pending retry', () async {
    final (:manager, :log, done: _) = build();
    manager.start();
    manager.stop();
    await Future<void>.delayed(const Duration(milliseconds: 30));
    expect(log, ['retry 1 in 5ms']);
  });

  test('a connect that finishes after stop() is abandoned', () async {
    final log = <String>[];
    final connecting = Completer<void>();
    final manager = ReconnectManager(
      policy: fast,
      connect: () => connecting.future,
      abandon: () async => log.add('abandon'),
      onRetryScheduled: (_, _) {},
      onReconnected: () => log.add('reconnected'),
      onGaveUp: (_) {},
    );
    manager.start();
    await Future<void>.delayed(const Duration(milliseconds: 10));
    manager.stop();
    connecting.complete();
    await pumpEventQueue();
    expect(log, ['abandon']);
  });

  test('does nothing with ReconnectPolicy.never()', () {
    final (:manager, :log, done: _) = build(
      policy: const ReconnectPolicy.never(),
    );
    manager.start();
    expect(manager.isRunning, isFalse);
    expect(log, isEmpty);
  });
}
