// Helper for mosquitto_test.dart: connects as a device and waits to be
// killed, so the broker publishes its Last Will.
//
//   dart run test/integration/will_device.dart <port> <prefix> <deviceId>

import 'dart:async';

import 'package:bedaya_mqtt/bedaya_mqtt.dart';

Future<void> main(List<String> args) async {
  final client = BedayaClient(
    ClientConfig(
      host: 'localhost',
      port: int.parse(args[0]),
      topicPrefix: args[1],
      keepAlive: const Duration(seconds: 2),
    ),
  );
  client.asDevice(args[2]);
  await client.connect();
  print('ready');
  await Completer<void>().future; // wait forever
}
