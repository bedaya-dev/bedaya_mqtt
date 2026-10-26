import 'mqtt_transport.dart';
import 'transport_options.dart';

/// Creates the platform's real [MqttTransport].
///
/// This version is used only on platforms with neither `dart:io` nor a
/// browser.
MqttTransport createTransport(TransportOptions options) =>
    throw UnsupportedError('No MQTT transport for this platform');
