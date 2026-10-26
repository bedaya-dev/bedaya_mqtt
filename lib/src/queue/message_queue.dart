import '../transport/raw_message.dart';
import 'queued_message.dart';

/// Holds messages published while offline, until the connection is back.
///
/// The default is `MemoryQueue`. Implement this to keep messages somewhere
/// else, like a file or a database, so they survive an app restart.
abstract interface class MessageQueue {
  /// How many messages are waiting.
  int get length;

  /// Adds [message] at the end.
  ///
  /// Returns the message that was dropped to make room, or `null`.
  QueuedMessage? enqueue(RawMessage message);

  /// Sends the waiting messages in order with [send].
  ///
  /// A message is removed only after [send] succeeds. If [send] throws,
  /// draining stops, that message and the rest stay queued, and the error
  /// is rethrown. Expired messages are dropped without sending.
  Future<void> drain(Future<void> Function(RawMessage message) send);

  /// Removes every waiting message.
  void clear();
}
