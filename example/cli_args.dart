import 'dart:io';

/// Reads a `host` or `host:port` argument for the examples.
///
/// No argument means the public broker `broker.hivemq.com`, default port.
(String, int?) hostAndPort(String? arg) {
  if (arg == null || arg.isEmpty) return ('broker.hivemq.com', null);
  final colon = arg.lastIndexOf(':');
  if (colon == -1) return (arg, null);
  return (arg.substring(0, colon), int.parse(arg.substring(colon + 1)));
}

/// The topic prefix when none is given: `bedaya-<your user name>`.
///
/// The public broker is shared by everyone. A personal prefix keeps your
/// devices apart from other people's, while the device and the controller
/// you start on the same machine still pick the same one.
String defaultPrefix() {
  final user =
      Platform.environment['USER'] ?? Platform.environment['USERNAME'] ?? '';
  final safe = user.toLowerCase().replaceAll(RegExp(r'[^a-z0-9_-]'), '');
  return safe.isEmpty ? 'bedaya-demo' : 'bedaya-$safe';
}
