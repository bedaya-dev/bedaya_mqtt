import '../transport/qos.dart';
import 'codec.dart';
import 'topic_pattern.dart';

/// A typed MQTT topic: where messages go, how they are encoded, and how
/// they are delivered.
///
/// A topic is only a definition. It does not publish or subscribe by
/// itself; the client uses it to do that.
///
/// ```dart
/// final telemetry = Topic.json(
///   'bedaya/{deviceId}/telemetry',
///   fromJson: Telemetry.fromJson,
///   toJson: (t) => t.toJson(),
///   qos: Qos.atMostOnce,
/// );
/// ```
class Topic<T> {
  /// Creates a topic for [pattern] that uses [codec] for its payloads.
  ///
  /// Throws an [ArgumentError] if [pattern] is not valid. See [TopicPattern].
  Topic(
    String pattern, {
    required this.codec,
    this.qos = Qos.atLeastOnce,
    this.retained = false,
  }) : pattern = TopicPattern(pattern);

  /// Creates a topic whose payloads are JSON objects.
  ///
  /// [fromJson] and [toJson] convert between [T] and a JSON map.
  Topic.json(
    String pattern, {
    required T Function(Map<String, dynamic> json) fromJson,
    required Map<String, dynamic> Function(T value) toJson,
    Qos qos = Qos.atLeastOnce,
    bool retained = false,
  }) : this(
         pattern,
         codec: JsonMessageCodec<T>(fromJson: fromJson, toJson: toJson),
         qos: qos,
         retained: retained,
       );

  /// The topic template, like `bedaya/{deviceId}/telemetry`.
  final TopicPattern pattern;

  /// Converts messages to payload bytes and back.
  final MessageCodec<T> codec;

  /// The delivery guarantee used when publishing and subscribing.
  final Qos qos;

  /// Whether the broker keeps the last message on this topic.
  ///
  /// A new subscriber gets the retained message right away. This is useful
  /// for state like device status, and not needed for frequent telemetry.
  final bool retained;
}
