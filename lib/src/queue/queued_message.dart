import '../transport/raw_message.dart';

/// A message waiting to be sent, and when it started waiting.
class QueuedMessage {
  const QueuedMessage(this.message, this.enqueuedAt);

  final RawMessage message;

  /// Used to drop messages that waited longer than the queue's TTL.
  final DateTime enqueuedAt;

  @override
  String toString() => 'QueuedMessage(${message.topic}, at $enqueuedAt)';
}
