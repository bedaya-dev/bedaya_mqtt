import 'dart:convert';

import 'package:bedaya_mqtt/src/queue/memory_queue.dart';
import 'package:bedaya_mqtt/src/transport/raw_message.dart';
import 'package:test/test.dart';

RawMessage msg(String body) => RawMessage('t', utf8.encode(body));
String body(RawMessage m) => utf8.decode(m.payload);

void main() {
  test('drains in order and empties', () async {
    final queue = MemoryQueue()
      ..enqueue(msg('1'))
      ..enqueue(msg('2'))
      ..enqueue(msg('3'));
    final sent = <String>[];

    await queue.drain((m) async => sent.add(body(m)));

    expect(sent, ['1', '2', '3']);
    expect(queue.length, 0);
  });

  test('when full, drops the oldest and returns it', () {
    final queue = MemoryQueue(maxSize: 2)
      ..enqueue(msg('1'))
      ..enqueue(msg('2'));
    final dropped = queue.enqueue(msg('3'));

    print('  dropped: ${body(dropped!.message)}');
    expect(body(dropped.message), '1');
    expect(queue.length, 2);
  });

  test('a failed send keeps that message and the rest', () async {
    final queue = MemoryQueue()
      ..enqueue(msg('1'))
      ..enqueue(msg('2'))
      ..enqueue(msg('3'));
    final sent = <String>[];

    await expectLater(
      queue.drain((m) async {
        if (body(m) == '2') throw StateError('offline');
        sent.add(body(m));
      }),
      throwsStateError,
    );
    print('  sent: $sent, left: ${queue.length}');
    expect(sent, ['1']);
    expect(queue.length, 2);

    await queue.drain((m) async => sent.add(body(m)));
    expect(sent, ['1', '2', '3']);
  });

  test('expired messages are dropped, not sent', () async {
    var now = DateTime(2026);
    final queue = MemoryQueue(ttl: const Duration(minutes: 5), clock: () => now)
      ..enqueue(msg('old'));
    now = now.add(const Duration(minutes: 4));
    queue.enqueue(msg('new'));
    now = now.add(const Duration(minutes: 2)); // old: 6 min, new: 2 min

    final sent = <String>[];
    await queue.drain((m) async => sent.add(body(m)));

    expect(sent, ['new']);
  });

  test('clear empties the queue', () {
    final queue = MemoryQueue()..enqueue(msg('1'));
    queue.clear();
    expect(queue.length, 0);
  });
}
