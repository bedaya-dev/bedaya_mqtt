/// Reads a `host` or `host:port` argument for the examples.
///
/// No argument means the public broker `broker.hivemq.com`, default port.
(String, int?) hostAndPort(String? arg) {
  if (arg == null || arg.isEmpty) return ('broker.hivemq.com', null);
  final colon = arg.lastIndexOf(':');
  if (colon == -1) return (arg, null);
  return (arg.substring(0, colon), int.parse(arg.substring(colon + 1)));
}
