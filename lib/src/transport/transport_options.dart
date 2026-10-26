/// What `createTransport` needs to build a real connection.
///
/// A plain copy of the connection part of `ClientConfig`, so `transport/`
/// does not depend on `client/`. See `ClientConfig` for what each setting
/// means.
class TransportOptions {
  /// Creates the options. `BedayaClient` does this from its config.
  const TransportOptions({
    required this.host,
    required this.clientId,
    this.port,
    this.username,
    this.password,
    this.useTls = false,
    this.useWebSocket = false,
    this.webSocketPath = '/mqtt',
    this.keepAlive = const Duration(seconds: 30),
    this.caCertificate,
    this.clientCertificate,
    this.clientKey,
  });

  /// The broker's host name.
  final String host;

  /// The broker's port, or `null` for the usual port of the connection
  /// type.
  final int? port;

  /// The MQTT client id. Must be unique on the broker.
  final String clientId;

  /// The login username, if the broker needs one.
  final String? username;

  /// The login password.
  final String? password;

  /// Whether to encrypt with TLS.
  final bool useTls;

  /// Whether to use MQTT over WebSocket instead of TCP (native only; the
  /// browser always uses WebSocket).
  final bool useWebSocket;

  /// The URL path of the broker's WebSocket endpoint, like `/mqtt`.
  final String webSocketPath;

  /// How often to ping the broker when idle.
  final Duration keepAlive;

  /// PEM text of an extra CA to trust, for brokers with a self-signed or
  /// private certificate.
  final String? caCertificate;

  /// PEM text of this client's certificate chain, for brokers that ask
  /// clients to prove who they are (mutual TLS). Needs [clientKey].
  final String? clientCertificate;

  /// PEM text of the private key for [clientCertificate].
  final String? clientKey;

  /// Whether any custom certificate is set.
  bool get hasCustomCertificates =>
      caCertificate != null || clientCertificate != null || clientKey != null;
}
