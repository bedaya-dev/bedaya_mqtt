import 'qos.dart';

/// An MQTT message as bytes, before any codec runs.
///
/// This is what moves between the client and the transport. Typed messages
/// only exist above this layer.
class RawMessage {
  /// Creates a message for the full [topic] with [payload] bytes.
  const RawMessage(
    this.topic,
    this.payload, {
    this.qos = Qos.atLeastOnce,
    this.retained = false,
  });

  /// The full topic, prefix included, like `bedaya/a1/telemetry`.
  final String topic;

  /// The message body.
  final List<int> payload;

  /// The delivery guarantee.
  final Qos qos;

  /// When publishing: ask the broker to keep this as the topic's last
  /// message. When receiving: this came from the broker's store, not live.
  final bool retained;

  @override
  String toString() =>
      'RawMessage($topic, ${payload.length} bytes, $qos, retained: $retained)';
}
