import 'dart:async';
import 'dart:convert';
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
import 'bedaya_log.dart';
import 'client_config.dart';
import 'mqtt_connection_state.dart';
import 'topic_message.dart';

/// The entry point: connects to an MQTT broker, and sends and receives
/// typed messages.
///
/// ```dart
/// final client = BedayaClient(ClientConfig(host: 'broker.hivemq.com'));
/// await client.connect();
/// client.device('a1').on(BedayaTopics.telemetry).listen(print);
/// ```
///
/// What it does for you:
///
/// - **Prefix**: topics are written without a prefix, like
///   `{deviceId}/telemetry`. The client adds [ClientConfig.topicPrefix] when
///   sending and removes it from what arrives.
/// - **Reconnect**: if the connection drops, it retries (see
///   [ClientConfig.reconnect]) and then restores every subscription.
/// - **Offline queue**: [publish] while offline keeps the message and sends
///   it after the next connect.
/// - **Roles**: [device] to watch and command a device, [asDevice] to be
///   one.
///
/// To see what it is doing, set [ClientConfig.logger] (for example to
/// `print`).
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
  ///
  /// Throws an [ArgumentError] for an invalid [ClientConfig.topicPrefix] or
  /// certificate settings.
  BedayaClient(this.config, {MqttTransport? transport, MessageQueue? queue})
    : _queue =
          queue ??
          (config.maxQueueSize > 0
              ? MemoryQueue(maxSize: config.maxQueueSize, ttl: config.queueTtl)
              : null),
      _transport = transport ?? createTransport(_transportOptions(config)) {
    _checkPrefix(config.topicPrefix);
    _transportStates = _transport.states.listen(_onTransportState);
    if (config.logger != null) {
      // One listener for tracing, however many streams are open.
      _traceIncoming = _transport.messages.listen(
        (m) => _log(
          BedayaLogLevel.debug,
          'received ${m.topic}${m.retained ? ' (retained)' : ''} '
          '${_preview(m.payload)}',
        ),
      );
    }
    _reconnect = ReconnectManager(
      policy: config.reconnect,
      connect: () => _transport.connect(lastWill: _lastWill),
      abandon: _transport.disconnect,
      onRetryScheduled: (attempt, delay) {
        _log(BedayaLogLevel.info, 'reconnect #$attempt in $delay');
        _setState(Reconnecting(attempt: attempt, nextDelay: delay));
      },
      onAttemptFailed: (attempt, error) =>
          _log(BedayaLogLevel.warning, 'reconnect #$attempt failed', error),
      onReconnected: () => _restoring = _onConnected(),
      onGaveUp: (error) {
        _log(BedayaLogLevel.error, 'gave up reconnecting', error);
        _setState(
          Failed(error ?? const ConnectionException('Gave up reconnecting')),
        );
      },
    );
  }

  /// The settings this client was created with.
  final ClientConfig config;

  final MqttTransport _transport;
  final MessageQueue? _queue;
  final SubscriptionRegistry _registry = SubscriptionRegistry();
  late final ReconnectManager _reconnect;
  late final StreamSubscription<TransportState> _transportStates;
  StreamSubscription<RawMessage>? _traceIncoming;

  final StreamController<MqttConnectionState> _states =
      StreamController.broadcast();
  final StreamController<BedayaException> _errors =
      StreamController.broadcast();

  MqttConnectionState _state = const Disconnected();

  /// Sent to the broker on connect; the broker publishes it if we drop.
  /// Set by [asDevice].
  RawMessage? _lastWill;
  DeviceEndpoint? _endpoint;
  DeviceRegistry? _devices;

  /// True while [disconnect] runs, so its own "disconnected" is not taken
  /// for a dropped connection.
  bool _disconnecting = false;

  /// The latest run of [_onConnected] after a reconnect, so the first
  /// connect can wait for it (see [_retryFirstConnect]).
  Future<void>? _restoring;

  // --------------------------------------------------------------------
  // State
  // --------------------------------------------------------------------

  /// The current connection state: [Disconnected], [Connecting],
  /// [Connected], [Reconnecting] or [Failed].
  MqttConnectionState get connectionState => _state;

  /// Every change of [connectionState]. Only new changes arrive; read
  /// [connectionState] for the current one.
  Stream<MqttConnectionState> get connectionStates => _states.stream;

  /// Problems that don't belong to one call:
  ///
  /// - [DecodeException]: a payload that could not be decoded. The message
  ///   is skipped, and your stream keeps going.
  /// - [MessageDroppedException]: the offline queue was full.
  /// - [ConnectionException]: a subscribe or unsubscribe that failed.
  ///
  /// Bad messages are skipped instead of breaking your streams, because
  /// anyone on a shared broker can publish to your topics.
  Stream<BedayaException> get errors => _errors.stream;

  /// How many messages are waiting in the offline queue.
  int get queuedMessages => _queue?.length ?? 0;

  // --------------------------------------------------------------------
  // Connection
  // --------------------------------------------------------------------

  /// Connects to the broker.
  ///
  /// Once connected, it subscribes to every topic that is already being
  /// listened to. In the device role it also publishes the device as
  /// online. Then it sends what was queued while offline.
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
  ///
  /// Does nothing if already connected or connecting.
  Future<void> connect() async {
    if (_state is Connected || _state is Connecting || _state is Reconnecting) {
      return;
    }

    _log(BedayaLogLevel.info, 'connecting to ${_address()}');
    _setState(const Connecting());
    try {
      await _transport.connect(lastWill: _lastWill);
    } on ConnectionException catch (e) {
      if (!config.retryFirstConnect || !config.reconnect.enabled) {
        _log(BedayaLogLevel.error, 'connect failed', e);
        _setState(Failed(e));
        rethrow;
      }
      _log(BedayaLogLevel.warning, 'connect failed, retrying', e);
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
  ///
  /// The order matters: subscriptions first (so nothing is missed), then
  /// the online status, then the queue.
  Future<void> _onConnected() async {
    _log(BedayaLogLevel.info, 'connected');
    _setState(const Connected());
    try {
      // Clean session: the broker forgot our subscriptions, so restore
      // them first, before anything can be missed.
      final filters = _registry.filters;
      if (filters.isNotEmpty) {
        _log(BedayaLogLevel.debug, 'restoring ${filters.length} subscriptions');
      }
      for (final MapEntry(key: filter, value: qos) in filters.entries) {
        _log(BedayaLogLevel.debug, 'subscribe $filter (${qos.name})');
        await _transport.subscribe(filter, qos);
      }

      final endpoint = _endpoint;
      if (endpoint != null) {
        // Straight to the transport: a stale "online" must never be queued.
        _log(BedayaLogLevel.info, 'device "${endpoint.id}" is online');
        await _transport.publish(
          _encode(BedayaTopics.status, DeviceStatus.onlineStatus, {
            'deviceId': endpoint.id,
          }),
        );
      }

      final queue = _queue;
      if (queue != null && queue.length > 0) {
        _log(BedayaLogLevel.info, 'sending ${queue.length} queued messages');
        await queue.drain(_transport.publish);
      }
    } on ConnectionException catch (e) {
      // Dropped again while restoring. _onTransportState starts another
      // round of retries, and what's left in the queue stays there.
      _log(BedayaLogLevel.warning, 'connection lost while restoring', e);
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
      if (_state is! Disconnected) {
        _log(BedayaLogLevel.info, 'disconnected (was ${_state.runtimeType})');
        _setState(const Disconnected());
      }
      return;
    }

    _log(BedayaLogLevel.info, 'disconnecting');
    _disconnecting = true;
    try {
      final endpoint = _endpoint;
      if (endpoint != null) {
        _log(BedayaLogLevel.info, 'device "${endpoint.id}" is offline');
        await endpoint.publish(BedayaTopics.status, DeviceStatus.offlineStatus);
      }
      await _transport.disconnect();
    } finally {
      _disconnecting = false;
    }
    _log(BedayaLogLevel.info, 'disconnected');
    _setState(const Disconnected());
  }

  /// Disconnects and closes every stream. The client can't be used after.
  Future<void> close() async {
    await _devices?.close();
    await disconnect();
    await _traceIncoming?.cancel();
    await _transportStates.cancel();
    await _states.close();
    await _errors.close();
  }

  /// Called by the transport on every state change. Notices a connection
  /// that dropped by itself, and starts reconnecting.
  void _onTransportState(TransportState state) {
    // The second check skips late events from a connection that is
    // already back.
    final dropped =
        state == TransportState.disconnected &&
        _transport.state == TransportState.disconnected &&
        _state is Connected &&
        !_disconnecting;
    if (!dropped) return;

    _log(BedayaLogLevel.warning, 'connection lost');
    if (config.reconnect.enabled) {
      _reconnect.start();
    } else {
      _log(BedayaLogLevel.info, 'not reconnecting: ReconnectPolicy.never()');
      _setState(const Disconnected());
    }
  }

  // --------------------------------------------------------------------
  // Roles
  // --------------------------------------------------------------------

  /// Every device seen on the broker, with its online status and last
  /// seen time. Starts listening the first time it is used.
  DeviceRegistry get devices => _devices ??= DeviceRegistry(this);

  /// Talks to device [id] as a controller: listen to it and send it
  /// commands. Cheap: call it as often as you like.
  ///
  /// Throws an [ArgumentError] if [id] is empty or contains `/`, `+` or
  /// `#`.
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

    final will = _encode(BedayaTopics.status, DeviceStatus.offlineStatus, {
      'deviceId': id,
    });
    _log(
      BedayaLogLevel.info,
      'acting as device "$id" (Last Will: offline on ${will.topic})',
    );
    _lastWill = will;
    return _endpoint = DeviceEndpoint(this, id);
  }

  // --------------------------------------------------------------------
  // Publish and subscribe
  // --------------------------------------------------------------------

  /// Publishes [value] on [topic], with [params] filling the pattern.
  ///
  /// ```dart
  /// await client.publish(
  ///   BedayaTopics.telemetry,
  ///   Telemetry({'temp': 24.5}),
  ///   params: {'deviceId': 'a1'},
  /// ); // → bedaya/a1/telemetry
  /// ```
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
        _log(
          BedayaLogLevel.debug,
          'publish ${message.topic} ${_preview(message.payload)}',
        );
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
    _log(
      BedayaLogLevel.debug,
      'offline: queued ${message.topic} (${queue.length} waiting)',
    );
    if (dropped != null) {
      _error(
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
  /// ```dart
  /// // Every device:
  /// client.subscribe(BedayaTopics.telemetry).listen((m) {
  ///   print('${m.params['deviceId']}: ${m.value}');
  /// });
  /// // Only device a1:
  /// client.subscribe(BedayaTopics.telemetry, params: {'deviceId': 'a1'});
  /// ```
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

    // Every stream sees every incoming message, and keeps its own.
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
        _error(e);
        return;
      } catch (e) {
        _error(DecodeException('Codec failed on ${raw.topic}', e));
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

  /// A stream started listening to [filter]. Subscribes if it is the
  /// first one (or needs a higher QoS) and we're connected; otherwise
  /// [_onConnected] does it later.
  void _addListener(String filter, Qos qos) {
    final subscribeWith = _registry.add(filter, qos);
    if (subscribeWith != null && _state is Connected) {
      _log(BedayaLogLevel.debug, 'subscribe $filter (${subscribeWith.name})');
      _transport
          .subscribe(filter, subscribeWith)
          .catchError((Object e) => _reportError('subscribe', filter, e));
    }
  }

  /// A stream stopped listening to [filter]. Unsubscribes if it was the
  /// last one.
  void _removeListener(String filter) {
    if (_registry.remove(filter) && _state is Connected) {
      _log(BedayaLogLevel.debug, 'unsubscribe $filter');
      _transport
          .unsubscribe(filter)
          .catchError((Object e) => _reportError('unsubscribe', filter, e));
    }
  }

  // --------------------------------------------------------------------
  // Helpers
  // --------------------------------------------------------------------

  void _reportError(String action, String filter, Object error) {
    _error(
      error is BedayaException
          ? error
          : ConnectionException('Could not $action "$filter"', error),
    );
  }

  /// Reports [e] on [errors], and in the trace.
  void _error(BedayaException e) {
    _log(BedayaLogLevel.warning, e.message, e.cause);
    _errors.add(e);
  }

  /// Sends a line to [ClientConfig.logger], if one is set.
  void _log(BedayaLogLevel level, String message, [Object? error]) =>
      config.logger?.call(BedayaLogRecord(level, message, error: error));

  /// The start of a payload as text, for the trace.
  static String _preview(List<int> payload) {
    final text = utf8.decode(payload, allowMalformed: true);
    return text.length <= 80 ? text : '${text.substring(0, 80)}…';
  }

  String _address() =>
      config.port == null ? config.host : '${config.host}:${config.port}';

  /// Topic, payload bytes, QoS and retained flag for [value] on [topic].
  RawMessage _encode<T>(Topic<T> topic, T value, Map<String, String> params) =>
      RawMessage(
        _addPrefix(topic.pattern.resolve(params)),
        topic.codec.encode(value),
        qos: topic.qos,
        retained: topic.retained,
      );

  /// `a1/telemetry` → `bedaya/a1/telemetry`.
  String _addPrefix(String topic) =>
      config.topicPrefix.isEmpty ? topic : '${config.topicPrefix}/$topic';

  /// `bedaya/a1/telemetry` → `a1/telemetry`, or `null` if the topic has
  /// another prefix.
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

  /// Throws an [ArgumentError] if [id] can't be a topic level.
  static void _checkDeviceId(String id) =>
      BedayaTopics.status.pattern.resolve({'deviceId': id});
}
