import 'dart:async';
import 'dart:math';

import '../contract/bedaya_topics.dart';
import '../contract/messages/device_status.dart';
import '../device/device.dart';
import '../device/device_registry.dart';
import '../device/device_endpoint.dart';
import '../errors.dart';
import '../queue/memory_queue.dart';
import '../queue/message_queue.dart';
import '../reconnect/reconnect_manager.dart';
import '../subscriptions/subscription_registry.dart';
import '../topic/topic.dart';
import '../transport/mqtt_transport.dart';
import '../transport/qos.dart';
import '../transport/raw_message.dart';
import '../transport/topic_filter.dart';
import '../transport/transport_factory.dart';
import '../transport/transport_options.dart';
import 'client_config.dart';
import 'mqtt_connection_state.dart';
import 'topic_message.dart';

/// The entry point: connects to the broker and sends and receives typed
/// messages.
///
/// ```dart
/// final client = BedayaClient(ClientConfig(host: 'broker.hivemq.com'));
/// await client.connect();
/// client.device('a1').on(BedayaTopics.telemetry).listen(print);
/// ```
///
/// Topics are written without the prefix. The client adds
/// [ClientConfig.topicPrefix] when it publishes and subscribes, and removes
/// it from incoming topics.
class BedayaClient {
  /// Creates a client. It does not connect until [connect].
  ///
  /// Pass [transport] to use a custom connection, like `MemoryTransport`
  /// in tests. Otherwise a real one is made from [config]: TCP/TLS on
  /// native platforms, WebSocket in the browser.
  ///
  /// Pass [queue] to keep offline messages somewhere other than memory.
  /// Otherwise a `MemoryQueue` is made from [ClientConfig.maxQueueSize] and
  /// [ClientConfig.queueTtl].
  BedayaClient(this.config, {MqttTransport? transport, MessageQueue? queue})
    : _queue =
          queue ??
          (config.maxQueueSize > 0
              ? MemoryQueue(maxSize: config.maxQueueSize, ttl: config.queueTtl)
              : null),
      _transport = transport ?? createTransport(_transportOptions(config)) {
    _checkPrefix(config.topicPrefix);
    _transportStates = _transport.states.listen(_onTransportState);
    _reconnect = ReconnectManager(
      policy: config.reconnect,
      connect: () => _transport.connect(lastWill: _lastWill),
      abandon: _transport.disconnect,
      onRetryScheduled: (attempt, delay) =>
          _setState(Reconnecting(attempt: attempt, nextDelay: delay)),
      onReconnected: () => _restoring = _onConnected(),
      onGaveUp: (error) => _setState(
        Failed(error ?? const ConnectionException('Gave up reconnecting')),
      ),
    );
  }

  final ClientConfig config;
  final MqttTransport _transport;
  final MessageQueue? _queue;
  late final ReconnectManager _reconnect;

  /// The latest run of [_onConnected] after a reconnect.
  Future<void>? _restoring;
  final SubscriptionRegistry _registry = SubscriptionRegistry();
  final StreamController<MqttConnectionState> _states =
      StreamController.broadcast();
  final StreamController<BedayaException> _errors =
      StreamController.broadcast();
  late final StreamSubscription<TransportState> _transportStates;

  MqttConnectionState _state = const Disconnected();
  RawMessage? _lastWill;
  DeviceEndpoint? _endpoint;
  bool _disconnecting = false;

  /// The current connection state.
  MqttConnectionState get connectionState => _state;

  /// Every change of [connectionState].
  Stream<MqttConnectionState> get connectionStates => _states.stream;

  /// Problems that don't belong to one call, like a payload that could not
  /// be decoded or a subscribe the broker refused.
  ///
  /// Bad messages are skipped instead of breaking your streams, because
  /// anyone on a shared broker can publish to your topics.
  Stream<BedayaException> get errors => _errors.stream;

  /// How many messages are waiting in the offline queue.
  int get queuedMessages => _queue?.length ?? 0;

  /// Connects to the broker.
  ///
  /// Subscribes to every topic that is already being listened to. In the
  /// device role, also publishes the device as online. Then sends what was
  /// queued while offline.
  ///
  /// If the connection drops later, the client reconnects by itself (see
  /// [ClientConfig.reconnect]).
  ///
  /// If this first attempt fails:
  /// - By default, throws a [ConnectionException] right away, so mistakes
  ///   like a wrong host show up at once.
  /// - With [ClientConfig.retryFirstConnect], keeps retrying like a
  ///   reconnect (`Reconnecting` states) and completes once connected.
  ///   Throws a [ConnectionException] if [ReconnectPolicy.maxAttempts]
  ///   runs out, or if [disconnect] is called first.
  Future<void> connect() async {
    if (_state is Connected || _state is Connecting || _state is Reconnecting) {
      return;
    }

    _setState(const Connecting());
    try {
      await _transport.connect(lastWill: _lastWill);
    } on ConnectionException catch (e) {
      if (!config.retryFirstConnect || !config.reconnect.enabled) {
        _setState(Failed(e));
        rethrow;
      }
      await _retryFirstConnect();
      return;
    }
    await _onConnected();
  }

  /// Retries the first connect with the reconnect policy, and waits for
  /// the end: connected, gave up, or disconnect() was called.
  Future<void> _retryFirstConnect() async {
    final end = _states.stream.firstWhere(
      (s) => s is Connected || s is Failed || s is Disconnected,
    );
    _reconnect.start();

    switch (await end) {
      case Connected():
        // Wait for resubscribe, online status and the queue, like a
        // normal connect does.
        await _restoring;
      case Failed(:final error):
        throw error is ConnectionException
            ? error
            : ConnectionException('Could not connect', error);
      case _:
        throw const ConnectionException(
          'disconnect() was called before the first connect succeeded',
        );
    }
  }

  /// Runs after every successful connect, first or reconnect.
  Future<void> _onConnected() async {
    _setState(const Connected());
    try {
      // Clean session: the broker forgot our subscriptions, so restore
      // them first, before anything can be missed.
      for (final MapEntry(key: filter, value: qos)
          in _registry.filters.entries) {
        await _transport.subscribe(filter, qos);
      }

      final endpoint = _endpoint;
      if (endpoint != null) {
        // Straight to the transport: a stale "online" must never be queued.
        await _transport.publish(
          _encode(BedayaTopics.status, DeviceStatus.onlineStatus, {
            'deviceId': endpoint.id,
          }),
        );
      }

      await _queue?.drain(_transport.publish);
    } on ConnectionException {
      // Dropped again while restoring. _onTransportState starts another
      // round of retries, and what's left in the queue stays there.
    }
  }

  /// Disconnects on purpose, and stops any reconnecting.
  ///
  /// In the device role, publishes the device as offline first, because
  /// the broker only sends the Last Will when a connection drops. Queued
  /// messages are kept for the next [connect].
  Future<void> disconnect() async {
    _reconnect.stop();
    if (_state is! Connected) {
      if (_state is! Disconnected) _setState(const Disconnected());
      return;
    }

    _disconnecting = true;
    try {
      final endpoint = _endpoint;
      if (endpoint != null) {
        await endpoint.publish(BedayaTopics.status, DeviceStatus.offlineStatus);
      }
      await _transport.disconnect();
    } finally {
      _disconnecting = false;
    }
    _setState(const Disconnected());
  }

  /// Disconnects and closes every stream. The client can't be used after.
  Future<void> close() async {
    await _devices?.close();
    await disconnect();
    await _transportStates.cancel();
    await _states.close();
    await _errors.close();
  }

  /// Every device seen on the broker, with online status and last seen
  /// time. Starts listening the first time it is used.
  DeviceRegistry get devices => _devices ??= DeviceRegistry(this);
  DeviceRegistry? _devices;

  /// Talks to device [id] as a controller: listen to it and send commands.
  ///
  /// Throws an [ArgumentError] if [id] contains `/`, `+` or `#`.
  Device device(String id) {
    _checkDeviceId(id);
    return Device(this, id);
  }

  /// Acts as device [id]: publish data and receive commands.
  ///
  /// Call this **before** [connect], because the device's Last Will
  /// (offline status) is sent when connecting. A client can be only one
  /// device; calling this again with the same [id] returns the same
  /// endpoint.
  ///
  /// Throws a [StateError] if connected, or if this client is already a
  /// different device.
  DeviceEndpoint asDevice(String id) {
    final existing = _endpoint;
    if (existing != null) {
      if (existing.id == id) return existing;
      throw StateError('This client is already device "${existing.id}"');
    }
    if (_state is Connected || _state is Connecting || _state is Reconnecting) {
      throw StateError(
        'Call asDevice before connect, so the Last Will can be set',
      );
    }
    _checkDeviceId(id);

    _lastWill = _encode(BedayaTopics.status, DeviceStatus.offlineStatus, {
      'deviceId': id,
    });
    return _endpoint = DeviceEndpoint(this, id);
  }

  /// Publishes [value] on [topic], with [params] filling the pattern.
  ///
  /// While offline, the message goes into the offline queue and is sent
  /// after the next connect; the future completes once it is queued. If
  /// the queue is full, its oldest message is dropped and reported on
  /// [errors].
  ///
  /// Throws an [ArgumentError] if a param is missing or invalid, and a
  /// [ConnectionException] if offline with the queue turned off.
  Future<void> publish<T>(
    Topic<T> topic,
    T value, {
    Map<String, String> params = const {},
  }) async {
    final message = _encode(topic, value, params);
    if (_state is Connected) {
      try {
        await _transport.publish(message);
        return;
      } on ConnectionException {
        // The connection just dropped. Queue it below, if we can.
        if (_queue == null) rethrow;
      }
    }

    final queue = _queue;
    if (queue == null) throw const ConnectionException('Not connected');
    final dropped = queue.enqueue(message);
    if (dropped != null) {
      _errors.add(
        MessageDroppedException(
          'Offline queue is full; dropped the oldest message '
          '(to ${dropped.message.topic})',
        ),
      );
    }
  }

  /// Listens to [topic]. Params in [params] are fixed; the others match
  /// anything.
  ///
  /// The subscription starts when the stream is listened to (even before
  /// [connect]) and ends when it is cancelled. Messages that fail to
  /// decode are skipped and reported on [errors].
  Stream<TopicMessage<T>> subscribe<T>(
    Topic<T> topic, {
    Map<String, String> params = const {},
  }) {
    final filter = _addPrefix(topic.pattern.filter(params));
    StreamSubscription<RawMessage>? incoming;
    late final StreamController<TopicMessage<T>> controller;

    void onMessage(RawMessage raw) {
      final local = _removePrefix(raw.topic);
      if (local == null) return;
      final matched = topic.pattern.match(local);
      if (matched == null) return;
      for (final MapEntry(:key, :value) in params.entries) {
        if (matched[key] != value) return;
      }

      final T value;
      try {
        value = topic.codec.decode(raw.payload);
      } on DecodeException catch (e) {
        _errors.add(e);
        return;
      } catch (e) {
        _errors.add(DecodeException('Codec failed on ${raw.topic}', e));
        return;
      }
      controller.add(
        TopicMessage(
          value,
          params: matched,
          topic: raw.topic,
          retained: raw.retained,
        ),
      );
    }

    controller = StreamController(
      onListen: () {
        incoming = _transport.messages.listen(onMessage);
        _addListener(filter, topic.qos);
      },
      onCancel: () async {
        await incoming?.cancel();
        _removeListener(filter);
      },
    );
    return controller.stream;
  }

  void _addListener(String filter, Qos qos) {
    final subscribeWith = _registry.add(filter, qos);
    if (subscribeWith != null && _state is Connected) {
      _transport
          .subscribe(filter, subscribeWith)
          .catchError((Object e) => _reportError('subscribe', filter, e));
    }
  }

  void _removeListener(String filter) {
    if (_registry.remove(filter) && _state is Connected) {
      _transport
          .unsubscribe(filter)
          .catchError((Object e) => _reportError('unsubscribe', filter, e));
    }
  }

  void _reportError(String action, String filter, Object error) {
    _errors.add(
      error is BedayaException
          ? error
          : ConnectionException('Could not $action "$filter"', error),
    );
  }

  void _onTransportState(TransportState state) {
    // The connection dropped without disconnect(). The second check skips
    // late events from a connection that is already back.
    if (state == TransportState.disconnected &&
        _transport.state == TransportState.disconnected &&
        _state is Connected &&
        !_disconnecting) {
      if (config.reconnect.enabled) {
        _reconnect.start();
      } else {
        _setState(const Disconnected());
      }
    }
  }

  RawMessage _encode<T>(Topic<T> topic, T value, Map<String, String> params) =>
      RawMessage(
        _addPrefix(topic.pattern.resolve(params)),
        topic.codec.encode(value),
        qos: topic.qos,
        retained: topic.retained,
      );

  String _addPrefix(String topic) =>
      config.topicPrefix.isEmpty ? topic : '${config.topicPrefix}/$topic';

  /// The topic without the prefix, or `null` if it doesn't have it.
  String? _removePrefix(String topic) {
    final prefix = config.topicPrefix;
    if (prefix.isEmpty) return topic;
    if (!topic.startsWith('$prefix/')) return null;
    return topic.substring(prefix.length + 1);
  }

  void _setState(MqttConnectionState state) {
    _state = state;
    _states.add(state);
  }

  static void _checkPrefix(String prefix) {
    if (prefix.isEmpty) return;
    checkTopicName(prefix);
    if (prefix.startsWith('/') || prefix.endsWith('/')) {
      throw ArgumentError('topicPrefix must not start or end with "/"');
    }
  }

  /// The connection settings from [config], checked.
  ///
  /// Throws an [ArgumentError] for certificates without TLS, or a client
  /// certificate without its key (or the other way round).
  static TransportOptions _transportOptions(ClientConfig config) {
    final hasCerts =
        config.caCertificate != null ||
        config.clientCertificate != null ||
        config.clientKey != null;
    if (hasCerts && !config.useTls) {
      throw ArgumentError('Certificates need useTls: true');
    }
    if ((config.clientCertificate == null) != (config.clientKey == null)) {
      throw ArgumentError('clientCertificate and clientKey go together');
    }
    return TransportOptions(
      host: config.host,
      port: config.port,
      clientId: config.clientId ?? _randomClientId(),
      username: config.username,
      password: config.password,
      useTls: config.useTls,
      useWebSocket: config.useWebSocket,
      webSocketPath: config.webSocketPath,
      keepAlive: config.keepAlive,
      caCertificate: config.caCertificate,
      clientCertificate: config.clientCertificate,
      clientKey: config.clientKey,
    );
  }

  /// `bedaya-` plus 12 random hex characters. Stays under the 23 characters
  /// that MQTT 3.1.1 brokers must accept.
  static String _randomClientId() {
    final random = Random.secure();
    final hex = List.generate(
      6,
      (_) => random.nextInt(256).toRadixString(16).padLeft(2, '0'),
    ).join();
    return 'bedaya-$hex';
  }

  static void _checkDeviceId(String id) =>
      BedayaTopics.status.pattern.resolve({'deviceId': id});
}
