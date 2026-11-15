import 'dart:async';
import 'dart:io';
import 'dart:typed_data';

/// A tiny raw MQTT 3.1.1 server for tests, enough to drive the real
/// `MqttClientTransport` without Docker.
///
/// It answers CONNECT with CONNACK (accepted), SUBSCRIBE with SUBACK,
/// UNSUBSCRIBE with UNSUBACK, PINGREQ with PINGRESP and a QoS 1 PUBLISH
/// with PUBACK. It does not route messages. Each filter that [rejects]
/// gets return code 0x80 (failure) in the SUBACK; the others get the QoS
/// they asked for.
class StubMqttServer {
  StubMqttServer._(this._server, this.rejects) {
    _server.listen(_onClient);
  }

  /// Starts on a free port of the loopback interface.
  static Future<StubMqttServer> start({
    required bool Function(String filter) rejects,
  }) async => StubMqttServer._(
    await ServerSocket.bind(InternetAddress.loopbackIPv4, 0),
    rejects,
  );

  final ServerSocket _server;

  /// Which filters get SUBACK 0x80.
  final bool Function(String filter) rejects;

  final List<Socket> _clients = [];

  /// Every filter subscribed to, in order, with the return code sent.
  final List<(String filter, int code)> subscriptions = [];

  /// The port to connect to.
  int get port => _server.port;

  /// Stops listening and closes every connection.
  Future<void> close() async {
    for (final c in _clients) {
      c.destroy();
    }
    await _server.close();
  }

  void _onClient(Socket socket) {
    _clients.add(socket);
    final buffer = BytesBuilder();
    socket.listen(
      (data) {
        buffer.add(data);
        final rest = _handle(socket, buffer.takeBytes());
        buffer.add(rest);
      },
      onError: (Object _) => socket.destroy(),
      onDone: socket.destroy,
    );
  }

  /// Handles every complete packet in [bytes]; returns what is left.
  List<int> _handle(Socket socket, Uint8List bytes) {
    var offset = 0;
    while (true) {
      if (bytes.length - offset < 2) break;
      // Remaining length: up to 4 bytes, 7 bits each.
      var length = 0;
      var multiplier = 1;
      var i = offset + 1;
      var complete = false;
      while (i < bytes.length && i < offset + 5) {
        final b = bytes[i++];
        length += (b & 0x7f) * multiplier;
        multiplier *= 128;
        if (b & 0x80 == 0) {
          complete = true;
          break;
        }
      }
      if (!complete || bytes.length < i + length) break;
      _packet(socket, bytes[offset], bytes.sublist(i, i + length));
      offset = i + length;
    }
    return bytes.sublist(offset);
  }

  void _packet(Socket socket, int header, Uint8List body) {
    switch (header >> 4) {
      case 1: // CONNECT → CONNACK, accepted
        socket.add([0x20, 0x02, 0x00, 0x00]);
      case 3: // PUBLISH: QoS 1 → PUBACK (the packet id follows the topic)
        if ((header >> 1) & 0x03 == 1) {
          final p = 2 + ((body[0] << 8) | body[1]);
          socket.add([0x40, 0x02, body[p], body[p + 1]]);
        }
      case 8: // SUBSCRIBE → SUBACK
        final codes = <int>[];
        var p = 2; // after the packet id
        while (p < body.length) {
          final len = (body[p] << 8) | body[p + 1];
          final filter = String.fromCharCodes(body.sublist(p + 2, p + 2 + len));
          final qos = body[p + 2 + len];
          final code = rejects(filter) ? 0x80 : qos;
          subscriptions.add((filter, code));
          codes.add(code);
          p += 3 + len;
        }
        socket.add([0x90, 2 + codes.length, body[0], body[1], ...codes]);
      case 10: // UNSUBSCRIBE → UNSUBACK
        socket.add([0xb0, 0x02, body[0], body[1]]);
      case 12: // PINGREQ → PINGRESP
        socket.add([0xd0, 0x00]);
      case 14: // DISCONNECT
        socket.destroy();
    }
  }
}
