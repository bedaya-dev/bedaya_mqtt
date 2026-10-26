/// MQTT Quality of Service: how hard the broker and client try to deliver
/// a message.
///
/// This is the package's own type, so code outside `transport/` never needs
/// to import `mqtt_client`.
enum Qos {
  /// QoS 0. Sent once, with no acknowledgment. The message can be lost.
  ///
  /// Good for frequent telemetry, where the next reading comes soon.
  atMostOnce,

  /// QoS 1. Resent until acknowledged. The message can arrive more than once.
  ///
  /// Good for commands. Receivers should ignore duplicates, for example by
  /// command id.
  atLeastOnce,

  /// QoS 2. A four-step handshake makes sure the message arrives exactly
  /// once. This is the slowest level.
  exactlyOnce,
}
