import 'dart:async';

import '../errors.dart';
import 'mqtt_transport.dart';
import 'qos.dart';
import 'raw_message.dart';
import 'topic_filter.dart';

/// An in-memory MQTT broker for tests.
///
/// Connect several [MemoryTransport]s to one broker to make a controller
/// and a device talk without a network:
///
/// ```dart
/// final broker = MemoryBroker();
/// final controller = MemoryTransport(broker);
/// final device = MemoryTransport(broker);
/// ```
///
/// It supports `+`/`#` filters, retained messages and last wills. Use
/// [stop] and [start] to test what happens when the broker goes away.
class MemoryBroker {
  /// Creates a running broker with no clients and no retained messages.
  MemoryBroker();

  final Set<MemoryTransport> _clients = {};
  final Map<String, RawMessage> _retained = {};
  bool _running = true;

  /// Whether new connections are accepted.
  bool get isRunning => _running;

  /// The retained message of each topic.
  Map<String, RawMessage> get retained => Map.unmodifiable(_retained);

  /// How many transports are connected.
  int get clientCount => _clients.length;

  /// Simulates the broker going down.
  ///
  /// Every client is disconnected and new connections fail until [start].
  /// Last wills are not published, because the broker itself is gone.
  /// Retained messages are kept.
  void stop() {
    _running = false;
    for (final client in _clients.toList()) {
      client._leave();
    }
  }

  /// Accepts connections again after [stop].
  void start() => _running = true;

  void _publish(RawMessage message) {
    if (message.retained) {
      // An empty retained payload clears the topic, like a real broker.
      if (message.payload.isEmpty) {
        _retained.remove(message.topic);
      } else {
        _retained[message.topic] = message;
      }
    }
    for (final client in _clients.toList()) {
      client._deliver(message);
    }
  }
}

/// An [MqttTransport] connected to a [MemoryBroker].
///
/// Pass it to a client in tests:
///
/// ```dart
/// final client = BedayaClient(
///   ClientConfig(host: 'memory'), // host is not used
///   transport: MemoryTransport(broker),
/// );
/// ```
///
/// Like a real connection, it forgets its subscriptions when it drops; the
/// client restores them.
class MemoryTransport implements MqttTransport {
  /// Creates a transport for [broker]. It connects on `connect()`.
  MemoryTransport(this.broker);

  /// The broker this transport connects to.
  final MemoryBroker broker;

  final StreamController<RawMessage> _messages = StreamController.broadcast();
  final StreamController<TransportState> _states = StreamController.broadcast();
  final Map<String, Qos> _subscriptions = {};
  TransportState _state = TransportState.disconnected;
  RawMessage? _lastWill;

  /// The current filters and their QoS. Empty after a drop.
  Map<String, Qos> get subscriptions => Map.unmodifiable(_subscriptions);

  @override
  TransportState get state => _state;

  @override
  Stream<TransportState> get states => _states.stream;

  @override
  Stream<RawMessage> get messages => _messages.stream;

  @override
  Future<void> connect({RawMessage? lastWill}) async {
    if (_state == TransportState.connected) return;
    if (lastWill != null) checkTopicName(lastWill.topic);

    _setState(TransportState.connecting);
    if (!broker._running) {
      _setState(TransportState.disconnected);
      throw const ConnectionException('Broker is not running');
    }
    _lastWill = lastWill;
    broker._clients.add(this);
    _setState(TransportState.connected);
  }

  @override
  Future<void> disconnect() async {
    if (_state != TransportState.connected) return;
    _leave();
  }

  /// Simulates losing the network, as if the cable was pulled.
  ///
  /// Unlike [disconnect], the broker publishes the last will.
  void drop() {
    if (_state != TransportState.connected) return;
    final will = _lastWill;
    _leave();
    if (will != null) broker._publish(will);
  }

  @override
  Future<void> subscribe(String filter, Qos qos) async {
    _checkConnected();
    checkTopicFilter(filter);
    _subscriptions[filter] = qos;

    // A real broker sends retained messages after the subscribe completes,
    // so code that listens right after `await subscribe(...)` still gets
    // them. Timer.run waits until the caller's code has run.
    final retained = broker._retained.values
        .where((m) => topicMatchesFilter(filter, m.topic))
        .toList();
    if (retained.isEmpty) return;
    Timer.run(() {
      if (_subscriptions[filter] != qos) return;
      for (final message in retained) {
        _messages.add(
          RawMessage(
            message.topic,
            message.payload,
            qos: _min(message.qos, qos),
            retained: true,
          ),
        );
      }
    });
  }

  @override
  Future<void> unsubscribe(String filter) async {
    _checkConnected();
    _subscriptions.remove(filter);
  }

  @override
  Future<void> publish(RawMessage message) async {
    _checkConnected();
    checkTopicName(message.topic);
    broker._publish(message);
  }

  /// Delivers a live message once, even if several filters match it.
  void _deliver(RawMessage message) {
    Qos? granted;
    for (final MapEntry(key: filter, value: qos) in _subscriptions.entries) {
      if (topicMatchesFilter(filter, message.topic)) {
        granted = granted == null ? qos : _max(granted, qos);
      }
    }
    if (granted == null) return;

    // Live messages arrive with retained = false, like a real broker.
    _messages.add(
      RawMessage(
        message.topic,
        message.payload,
        qos: _min(message.qos, granted),
      ),
    );
  }

  void _leave() {
    broker._clients.remove(this);
    _subscriptions.clear();
    _lastWill = null;
    _setState(TransportState.disconnected);
  }

  void _checkConnected() {
    if (_state != TransportState.connected) {
      throw const ConnectionException('Not connected');
    }
  }

  void _setState(TransportState state) {
    _state = state;
    _states.add(state);
  }

  static Qos _min(Qos a, Qos b) => a.index <= b.index ? a : b;
  static Qos _max(Qos a, Qos b) => a.index >= b.index ? a : b;
}
