import 'package:mqtt_client/mqtt_browser_client.dart';

import 'mqtt_client_transport.dart';
import 'mqtt_transport.dart';
import 'transport_options.dart';

/// Creates an [MqttTransport] for the browser. Browsers can't open raw
/// TCP sockets, so this is always MQTT over WebSocket, whatever
/// `useWebSocket` says.
///
/// Throws an [ArgumentError] if custom certificates are set: the browser
/// decides which certificates to trust, and a page can't change that.
MqttTransport createTransport(TransportOptions o) {
  if (o.hasCustomCertificates) {
    throw ArgumentError(
      'Custom certificates are not supported in the browser. '
      'The broker needs a certificate the browser already trusts.',
    );
  }
  final scheme = o.useTls ? 'wss' : 'ws';
  final client = MqttBrowserClient.withPort(
    '$scheme://${o.host}${o.webSocketPath}',
    o.clientId,
    o.port ?? (o.useTls ? 443 : 80),
    maxConnectionAttempts: 1,
  )..keepAlivePeriod = o.keepAlive.inSeconds;
  return MqttClientTransport(
    client,
    username: o.username,
    password: o.password,
  );
}
