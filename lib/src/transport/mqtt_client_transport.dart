import 'dart:async';
import 'dart:convert';
import 'dart:typed_data';

// Prefixed, because mqtt_client also has `ConnectionException` and
// `MqttConnectionState`.
import 'package:mqtt_client/mqtt_client.dart' as mqtt;
import 'package:typed_data/typed_data.dart' show Uint8Buffer;

import '../errors.dart';
import 'mqtt_transport.dart';
import 'qos.dart';
import 'raw_message.dart';
import 'topic_filter.dart';

/// The real [MqttTransport], built on `mqtt_client`.
///
/// This is the only file in the package (with the factories) that knows
/// about `mqtt_client`. It takes an already configured [mqtt.MqttClient]
/// (server or browser), so the same code works on every platform.
///
/// Limits of `mqtt_client` to know about:
/// - The Last Will payload must be UTF-8 text.
/// - Subscribing again to a filter at a higher QoS keeps the old QoS.
class MqttClientTransport implements MqttTransport {
  /// Wraps [_client], which must already have its server, port, client id
  /// and TLS settings. `createTransport` builds one from `ClientConfig`.
  /// [username] and [password] are sent when connecting.
  MqttClientTransport(this._client, {String? username, String? password})
    : _username = username,
      _password = password {
    _client
      ..logging(on: false)
      // mqtt_client defaults to MQTT 3.1, where it sends UNSUBSCRIBE with
      // wrong header flags. Strict brokers like Mosquitto 2 then drop the
      // connection ("malformed packet"). 3.1.1 is what every broker speaks.
      ..setProtocolV311()
      // mqtt_client offers several WebSocket subprotocols by default, and
      // Mosquitto refuses the handshake. Every broker accepts plain "mqtt".
      ..websocketProtocols = const ['mqtt']
      // Reconnecting is our job (ReconnectManager), not mqtt_client's.
      ..autoReconnect = false
      // mqtt_client sends a ping every keepAlive but, by default, never
      // gives up when no answer comes back. On a dead link with no TCP
      // close (lost WiFi, WiFi → 4G) that left us "connected" until the
      // OS gave up, which takes many minutes. Now a ping with no answer
      // within one more keepAlive ends the connection, and we reconnect.
      ..disconnectOnNoResponsePeriod = _client.keepAlivePeriod
      ..onDisconnected = _onDisconnected;
  }

  final mqtt.MqttClient _client;
  final String? _username;
  final String? _password;

  final StreamController<RawMessage> _messages = StreamController.broadcast();
  final StreamController<TransportState> _states = StreamController.broadcast();
  StreamSubscription<List<mqtt.MqttReceivedMessage<mqtt.MqttMessage>>>?
  _updates;
  StreamSubscription<mqtt.MqttPublishMessage>? _acks;
  TransportState _state = TransportState.disconnected;

  /// QoS 1 and 2 messages sent on this connection and not yet confirmed
  /// by the broker, by message id. Completed by [_onAck], failed by
  /// [_cleanUp] if the connection ends first.
  final Map<int, Completer<void>> _pendingAcks = {};

  @override
  TransportState get state => _state;

  @override
  Stream<TransportState> get states => _states.stream;

  @override
  Stream<RawMessage> get messages => _messages.stream;

  @override
  Future<void> connect({RawMessage? lastWill}) async {
    if (_state != TransportState.disconnected) return;
    _setState(TransportState.connecting);

    // Clean session: the broker forgets our subscriptions when we leave.
    // SubscriptionRegistry remembers them instead.
    final message = mqtt.MqttConnectMessage()
        .withClientIdentifier(_client.clientIdentifier)
        .startClean();
    if (lastWill != null) {
      checkTopicName(lastWill.topic);
      message
        ..withWillTopic(lastWill.topic)
        ..withWillMessage(utf8.decode(lastWill.payload))
        ..withWillQos(_toMqttQos(lastWill.qos));
      if (lastWill.retained) message.withWillRetain();
    }
    _client.connectionMessage = message;

    try {
      await _guarded(() => _client.connect(_username, _password));
    } on Exception catch (e) {
      // Read the broker's answer before _fail() resets the client.
      final code = _client.connectionStatus?.returnCode;
      _fail();
      final refused =
          code != null &&
          code != mqtt.MqttConnectReturnCode.noneSpecified &&
          code != mqtt.MqttConnectReturnCode.connectionAccepted;
      throw ConnectionException(
        refused
            ? 'Broker refused the connection: ${code.name}'
            : 'Could not connect to ${_client.server}:${_client.port}',
        e,
      );
    }

    final status = _client.connectionStatus;
    if (status?.state != mqtt.MqttConnectionState.connected) {
      _fail();
      throw ConnectionException(
        'Broker refused the connection: ${status?.returnCode?.name}',
      );
    }

    // mqtt_client makes new updates and published streams on every connect.
    _updates = _client.updates?.listen(_onUpdates);
    // `published` gets a QoS 1 message when its PUBACK arrives, and a
    // QoS 2 message when its PUBCOMP arrives.
    _acks = _client.published?.listen(_onAck);
    _setState(TransportState.connected);
  }

  /// Runs [body] in a zone that catches errors mqtt_client leaves
  /// unhandled.
  ///
  /// mqtt_client listens to its socket without an error handler, so some
  /// socket errors escape as uncaught errors and could crash the app. For
  /// example, with TLS 1.3 a broker that wants a client certificate
  /// rejects us only after the handshake. Here, such an error fails the
  /// connect while connecting, drops the connection once connected, and
  /// is ignored once we're already disconnected.
  Future<void> _guarded(Future<void> Function() body) {
    final result = Completer<void>();
    runZonedGuarded(
      () async {
        try {
          await body();
          if (!result.isCompleted) result.complete();
        } catch (e, s) {
          if (!result.isCompleted) result.completeError(e, s);
        }
      },
      (error, stack) {
        if (!result.isCompleted) {
          result.completeError(error, stack);
        } else if (_state == TransportState.connected) {
          _fail();
        }
      },
    );
    return result.future;
  }

  @override
  Future<void> disconnect() async {
    if (_state != TransportState.connected) return;
    _client.disconnect();
    _cleanUp();
  }

  @override
  Future<void> subscribe(String filter, Qos qos) async {
    _checkConnected();
    checkTopicFilter(filter);
    if (_client.subscribe(filter, _toMqttQos(qos)) == null) {
      throw ConnectionException('Could not subscribe to "$filter"');
    }
  }

  @override
  Future<void> unsubscribe(String filter) async {
    _checkConnected();
    _client.unsubscribe(filter);
  }

  /// Sends [message]. For QoS 0, completes once it is handed to the socket.
  /// For QoS 1 and 2, completes only when the broker confirms it (PUBACK or
  /// PUBCOMP), and throws a [ConnectionException] if the connection ends
  /// first.
  ///
  /// `mqtt_client` returns as soon as the bytes are written, and forgets
  /// unconfirmed messages when the connection ends (clean session). Waiting
  /// here is what lets the client put such a message back in its offline
  /// queue instead of losing it.
  @override
  Future<void> publish(RawMessage message) async {
    _checkConnected();
    checkTopicName(message.topic);
    final int id;
    try {
      id = _client.publishMessage(
        message.topic,
        _toMqttQos(message.qos),
        Uint8Buffer()..addAll(message.payload),
        retain: message.retained,
      );
    } on Exception catch (e) {
      throw ConnectionException('Could not publish to "${message.topic}"', e);
    }
    if (message.qos == Qos.atMostOnce) return;

    // Registered right after publishMessage returns, and before the event
    // loop can deliver the broker's answer, so no ack is missed.
    final ack = Completer<void>();
    _pendingAcks[id] = ack;
    await ack.future;
  }

  void _onAck(mqtt.MqttPublishMessage message) {
    final id = message.variableHeader?.messageIdentifier;
    if (id == null) return;
    _pendingAcks.remove(id)?.complete();
  }

  void _onUpdates(List<mqtt.MqttReceivedMessage<mqtt.MqttMessage>> batch) {
    for (final received in batch) {
      final publish = received.payload;
      if (publish is! mqtt.MqttPublishMessage) continue;
      _messages.add(
        RawMessage(
          received.topic,
          Uint8List.fromList(publish.payload.message),
          qos: _fromMqttQos(publish.header?.qos),
          retained: publish.header?.retain ?? false,
        ),
      );
    }
  }

  /// Called by mqtt_client after every disconnect, asked for or not.
  void _onDisconnected() {
    if (_state == TransportState.disconnected) return;
    _cleanUp();
  }

  /// Resets after a failed connect, so the next connect starts fresh.
  void _fail() {
    try {
      _client.disconnect();
    } on Object {
      // Already down; nothing to close.
    }
    _cleanUp();
  }

  void _cleanUp() {
    _updates?.cancel();
    _updates = null;
    _acks?.cancel();
    _acks = null;
    // The broker never confirmed these, and mqtt_client forgets them with
    // the connection. Fail them, so the client can queue them again.
    final unconfirmed = _pendingAcks.values.toList();
    _pendingAcks.clear();
    for (final ack in unconfirmed) {
      ack.completeError(
        const ConnectionException(
          'Connection ended before the broker confirmed the message',
        ),
      );
    }
    if (_state != TransportState.disconnected) {
      _setState(TransportState.disconnected);
    }
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

  static mqtt.MqttQos _toMqttQos(Qos qos) => switch (qos) {
    Qos.atMostOnce => mqtt.MqttQos.atMostOnce,
    Qos.atLeastOnce => mqtt.MqttQos.atLeastOnce,
    Qos.exactlyOnce => mqtt.MqttQos.exactlyOnce,
  };

  static Qos _fromMqttQos(mqtt.MqttQos? qos) => switch (qos) {
    mqtt.MqttQos.atLeastOnce => Qos.atLeastOnce,
    mqtt.MqttQos.exactlyOnce => Qos.exactlyOnce,
    _ => Qos.atMostOnce,
  };
}
