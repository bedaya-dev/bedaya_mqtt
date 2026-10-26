/// What `createTransport` needs to build a real connection.
///
/// A plain copy of the connection part of `ClientConfig`, so `transport/`
/// does not depend on `client/`.
class TransportOptions {
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

  final String host;
  final int? port;
  final String clientId;
  final String? username;
  final String? password;
  final bool useTls;
  final bool useWebSocket;
  final String webSocketPath;
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
