/// A message that arrived on a topic, already decoded.
///
/// `client.subscribe(topic)` gives a stream of these:
///
/// ```dart
/// client.subscribe(BedayaTopics.telemetry).listen((m) {
///   print(m.topic);              // bedaya/a1/telemetry
///   print(m.params['deviceId']); // a1
///   print(m.value['temp']);      // 24.5 (m.value is a Telemetry)
/// });
/// ```
///
/// `device.on(topic)` gives only the [value]s.
class TopicMessage<T> {
  /// Creates a message. The client does this; you only read them.
  const TopicMessage(
    this.value, {
    required this.params,
    required this.topic,
    this.retained = false,
  });

  /// The decoded payload, as the topic's type.
  final T value;

  /// The params read from the topic, like `{deviceId: a1}`.
  final Map<String, String> params;

  /// The full topic it arrived on, prefix included.
  final String topic;

  /// Whether the broker sent this from its retained store (the last value
  /// it kept, maybe old) rather than live.
  final bool retained;

  @override
  String toString() => 'TopicMessage($topic, $value)';
}
