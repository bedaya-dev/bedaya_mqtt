import 'dart:convert';
import 'dart:io';

import 'package:mqtt_client/mqtt_server_client.dart';

import 'mqtt_client_transport.dart';
import 'mqtt_transport.dart';
import 'transport_options.dart';

/// Creates an [MqttTransport] over TCP, TLS or WebSocket, for Flutter
/// mobile/desktop, CLI scripts and servers.
MqttTransport createTransport(TransportOptions o) {
  final MqttServerClient client;
  if (o.useWebSocket) {
    final scheme = o.useTls ? 'wss' : 'ws';
    client = MqttServerClient.withPort(
      '$scheme://${o.host}${o.webSocketPath}',
      o.clientId,
      o.port ?? (o.useTls ? 443 : 80),
      maxConnectionAttempts: 1,
    )..useWebSocket = true;
    if (o.useTls && o.hasCustomCertificates) {
      // The default WebSocket (dart:io WebSocket.connect) only trusts the
      // system's CAs. The alternate one opens the TLS socket itself, so it
      // can use our SecurityContext.
      client
        ..useAlternateWebSocketImplementation = true
        ..securityContext = _securityContext(o);
    }
  } else {
    client = MqttServerClient.withPort(
      o.host,
      o.clientId,
      o.port ?? (o.useTls ? 8883 : 1883),
      maxConnectionAttempts: 1,
    )..secure = o.useTls;
    if (o.useTls) client.securityContext = _securityContext(o);
  }
  client.keepAlivePeriod = o.keepAlive.inSeconds;
  return MqttClientTransport(
    client,
    username: o.username,
    password: o.password,
  );
}

/// The system's CAs, plus the custom certificates if any.
SecurityContext _securityContext(TransportOptions o) {
  final context = SecurityContext(withTrustedRoots: true);
  final ca = o.caCertificate;
  final cert = o.clientCertificate;
  final key = o.clientKey;
  if (ca != null) context.setTrustedCertificatesBytes(utf8.encode(ca));
  if (cert != null) context.useCertificateChainBytes(utf8.encode(cert));
  if (key != null) context.usePrivateKeyBytes(utf8.encode(key));
  return context;
}
