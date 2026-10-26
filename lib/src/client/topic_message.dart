/// A decoded message that arrived on a topic.
class TopicMessage<T> {
  const TopicMessage(
    this.value, {
    required this.params,
    required this.topic,
    this.retained = false,
  });

  /// The decoded payload.
  final T value;

  /// The params read from the topic, like `{deviceId: a1}`.
  final Map<String, String> params;

  /// The full topic it arrived on, prefix included.
  final String topic;

  /// Whether the broker sent this from its retained store, rather than
  /// live.
  final bool retained;

  @override
  String toString() => 'TopicMessage($topic, $value)';
}
