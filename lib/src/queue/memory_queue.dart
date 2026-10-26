import 'dart:collection';

import '../transport/raw_message.dart';
import 'message_queue.dart';
import 'queued_message.dart';

/// A [MessageQueue] in memory. Bounded: when full, the oldest message is
/// dropped, because for sensor data the newest is the most useful.
///
/// Messages are lost if the app stops. The client makes one of these from
/// `ClientConfig.maxQueueSize` and `ClientConfig.queueTtl`; you only need
/// to create one yourself to pass a custom [clock] in tests.
class MemoryQueue implements MessageQueue {
  /// Creates a queue that holds up to [maxSize] messages, each for at most
  /// [ttl]. [clock] replaces `DateTime.now` in tests.
  MemoryQueue({this.maxSize = 100, this.ttl, DateTime Function()? clock})
    : assert(maxSize > 0),
      _clock = clock ?? DateTime.now;

  /// The most messages kept at once.
  final int maxSize;

  /// Messages older than this are dropped instead of sent. `null` keeps
  /// them forever.
  final Duration? ttl;

  final DateTime Function() _clock;
  final Queue<QueuedMessage> _items = Queue();

  @override
  int get length => _items.length;

  @override
  QueuedMessage? enqueue(RawMessage message) {
    final dropped = _items.length >= maxSize ? _items.removeFirst() : null;
    _items.addLast(QueuedMessage(message, _clock()));
    return dropped;
  }

  @override
  Future<void> drain(Future<void> Function(RawMessage message) send) async {
    while (_items.isNotEmpty) {
      final next = _items.first;
      if (_isExpired(next)) {
        _items.removeFirst();
        continue;
      }
      await send(next.message);
      // Only now: if send threw, the message is still first in line.
      // Check it's still there, in case clear() ran during send.
      if (_items.isNotEmpty && identical(_items.first, next)) {
        _items.removeFirst();
      }
    }
  }

  @override
  void clear() => _items.clear();

  bool _isExpired(QueuedMessage m) {
    final ttl = this.ttl;
    return ttl != null && _clock().difference(m.enqueuedAt) > ttl;
  }
}
