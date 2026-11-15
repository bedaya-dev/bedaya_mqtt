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
    this.offline = OfflinePolicy.queue,
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
    OfflinePolicy offline = OfflinePolicy.queue,
  }) : this(
         pattern,
         codec: JsonMessageCodec<T>(fromJson: fromJson, toJson: toJson),
         qos: qos,
         retained: retained,
         offline: offline,
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

  /// What `publish` does with a message on this topic while the client is
  /// not connected. See [OfflinePolicy].
  final OfflinePolicy offline;
}

/// What `BedayaClient.publish` does with a message while the client is not
/// connected.
///
/// A message replayed after a reconnect arrives late. That is fine for an
/// event log, misleading for a sensor reading (it looks current), and
/// dangerous for a command (a relay switching an hour later). Pick per
/// topic.
enum OfflinePolicy {
  /// Keep it in the offline queue and send it after the next connect. The
  /// queue's size and TTL still apply (`ClientConfig.maxQueueSize`,
  /// `ClientConfig.queueTtl`). The default.
  queue,

  /// Throw it away. `publish` completes normally, so a timer that
  /// publishes readings keeps running while offline. Use for data where
  /// only the latest value matters, like telemetry.
  drop,

  /// Throw a `ConnectionException`, so the caller decides. Use for
  /// messages that must not arrive late, like commands.
  fail,
}
