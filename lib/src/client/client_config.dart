import '../reconnect/backoff.dart';

/// How to reach the broker, and how the client behaves.
class ClientConfig {
  const ClientConfig({
    required this.host,
    this.port,
    this.clientId,
    this.username,
    this.password,
    this.useTls = false,
    this.caCertificate,
    this.clientCertificate,
    this.clientKey,
    this.useWebSocket = false,
    this.webSocketPath = '/mqtt',
    this.keepAlive = const Duration(seconds: 30),
    this.topicPrefix = 'bedaya',
    this.reconnect = const ReconnectPolicy(),
    this.retryFirstConnect = false,
    this.maxQueueSize = 100,
    this.queueTtl,
  });

  /// The broker's host name, like `broker.hivemq.com`.
  final String host;

  /// The broker's port. When `null`, the usual port for the connection
  /// type is used: 1883 for TCP, 8883 for TLS, 80 for WebSocket and 443
  /// for secure WebSocket.
  ///
  /// WebSocket ports differ a lot between brokers (HiveMQ's public broker
  /// uses 8000, and 8884 for TLS), so set this when using WebSocket.
  final int? port;

  /// The MQTT client id. When `null`, a random one is generated.
  ///
  /// Two connections with the same id kick each other off the broker.
  final String? clientId;

  final String? username;
  final String? password;

  /// Encrypts the connection with TLS.
  ///
  /// The broker's certificate must be signed by a CA the system trusts,
  /// or by [caCertificate].
  final bool useTls;

  /// PEM text (`-----BEGIN CERTIFICATE-----...`) of an extra CA to trust,
  /// for a broker with a self-signed or private certificate. The system's
  /// CAs are still trusted. Needs [useTls].
  ///
  /// Not supported in the browser, where the browser decides what to trust.
  final String? caCertificate;

  /// PEM text of this client's certificate, for brokers that check who the
  /// client is (mutual TLS, like AWS IoT). Needs [clientKey] and [useTls].
  /// Not supported in the browser.
  final String? clientCertificate;

  /// PEM text of the private key for [clientCertificate].
  final String? clientKey;

  /// Connects with MQTT over WebSocket instead of TCP. Always true on web.
  final bool useWebSocket;

  /// The URL path of the broker's WebSocket endpoint. Most brokers use
  /// `/mqtt`.
  final String webSocketPath;

  /// How often the client pings the broker when idle. The broker uses this
  /// to notice a dead connection and publish the Last Will.
  final Duration keepAlive;

  /// Added in front of every topic, so `{deviceId}/telemetry` becomes
  /// `bedaya/a1/telemetry`.
  ///
  /// May contain `/` (like `acme/home`) but not wildcards, and must not
  /// start or end with `/`. Use `''` for no prefix. The firmware must use
  /// the same prefix.
  final String topicPrefix;

  /// How to retry after the connection drops. Use
  /// `ReconnectPolicy.never()` to turn reconnecting off.
  final ReconnectPolicy reconnect;

  /// Whether the first `connect()` also retries with [reconnect].
  ///
  /// `false` (default): the first `connect()` throws if the broker can't be
  /// reached, so a wrong host or port shows up at once.
  ///
  /// `true`: it keeps retrying and completes once connected. Good for
  /// device scripts that may start before the network is up, and for apps
  /// opened while offline. Has no effect with `ReconnectPolicy.never()`.
  final bool retryFirstConnect;

  /// How many messages to keep while offline, to send once connected.
  /// When full, the oldest is dropped. Use 0 to turn the queue off, so
  /// publishing while offline throws instead.
  final int maxQueueSize;

  /// Queued messages older than this are dropped instead of sent. `null`
  /// keeps them until they are sent or pushed out.
  final Duration? queueTtl;
}
