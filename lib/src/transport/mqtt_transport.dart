import 'qos.dart';
import 'raw_message.dart';

/// Whether a transport is connected to the broker.
///
/// Simpler than the client's `MqttConnectionState`: a transport has no
/// idea of reconnecting or giving up. The client builds those on top.
enum TransportState {
  /// Not connected. The start, and after any disconnect or drop.
  disconnected,

  /// A connect is in progress.
  connecting,

  /// Connected; subscribe and publish work.
  connected,
}

/// The connection to an MQTT broker, as the rest of the package sees it.
///
/// This is the only seam to the MQTT library. The real implementation wraps
/// `mqtt_client`; tests use `MemoryTransport`.
///
/// A transport is simple on purpose. It does not reconnect, queue, or
/// remember subscriptions: when the connection drops, subscriptions are
/// gone and [state] becomes [TransportState.disconnected]. The layers above
/// handle all of that.
abstract interface class MqttTransport {
  /// The current connection state.
  TransportState get state;

  /// Every change of [state].
  Stream<TransportState> get states;

  /// Messages that arrive on any subscribed filter.
  Stream<RawMessage> get messages;

  /// Connects to the broker.
  ///
  /// [lastWill] is published by the broker if this connection drops without
  /// a [disconnect]. Throws a `ConnectionException` if the broker can't be
  /// reached or refuses the connection.
  Future<void> connect({RawMessage? lastWill});

  /// Closes the connection on purpose. The last will is not published.
  Future<void> disconnect();

  /// Starts receiving messages that match [filter] on [messages].
  ///
  /// Throws a `ConnectionException` if not connected.
  Future<void> subscribe(String filter, Qos qos);

  /// Stops receiving messages for [filter].
  ///
  /// Throws a `ConnectionException` if not connected.
  Future<void> unsubscribe(String filter);

  /// Sends [message] to the broker.
  ///
  /// For QoS 0, completes once the message is sent. For QoS 1 and 2,
  /// completes only when the broker has confirmed it, so a completed future
  /// means the broker has the message.
  ///
  /// Throws a `ConnectionException` if not connected, or if the connection
  /// ends before the broker confirms a QoS 1 or 2 message. The message may
  /// still have reached the broker in that case (QoS 1 means "at least
  /// once"), so sending it again can deliver it twice.
  Future<void> publish(RawMessage message);
}
