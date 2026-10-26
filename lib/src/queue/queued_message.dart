import '../transport/raw_message.dart';

/// A message waiting in the offline queue, and when it started waiting.
class QueuedMessage {
  /// Wraps [message], queued at [enqueuedAt].
  const QueuedMessage(this.message, this.enqueuedAt);

  /// The message to send: full topic, payload bytes, QoS and retained flag.
  final RawMessage message;

  /// When it was queued. Used to drop messages that waited longer than the
  /// queue's TTL.
  final DateTime enqueuedAt;

  @override
  String toString() => 'QueuedMessage(${message.topic}, at $enqueuedAt)';
}
