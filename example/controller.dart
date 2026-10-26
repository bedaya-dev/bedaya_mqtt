// A controller: watches a device and toggles its relay every 5 seconds.
//
//   dart run example/controller.dart [deviceId] [topicPrefix] [host[:port]]
//
// Start example/simulated_device.dart first, with the same id, prefix and
// host.

import 'dart:async';
import 'dart:io';

import 'package:bedaya_mqtt/bedaya_mqtt.dart';

import 'host_arg.dart';

Future<void> main(List<String> args) async {
  final deviceId = args.isNotEmpty ? args[0] : 'a1';
  final prefix = args.length > 1 ? args[1] : 'bedaya';
  final (host, port) = hostAndPort(args.length > 2 ? args[2] : null);

  final client = BedayaClient(
    ClientConfig(
      host: host,
      port: port,
      topicPrefix: prefix,
      retryFirstConnect: true,
    ),
  );
  client.connectionStates.listen((s) => print('[state] $s'));
  client.errors.listen((e) => print('[error] $e'));
  await client.connect();

  final device = client.device(deviceId);
  print('Watching "$prefix/$deviceId". Ctrl+C to stop.');

  device.status.listen(
    (s) => print('[status] ${s.online ? 'online' : 'offline'}'),
  );
  device.on(BedayaTopics.telemetry).listen((t) {
    print('[telemetry] temp=${t['temp']} relay=${t['relay']}');
  });

  var on = true;
  final timer = Timer.periodic(const Duration(seconds: 5), (_) async {
    final command = Command.relay(on: on);
    on = !on;
    try {
      final ack = await device.send(
        command,
        timeout: const Duration(seconds: 3),
      );
      print(
        '[ack] relay ${command.params['on']} → '
        '${ack.ok ? 'ok' : 'failed: ${ack.error}'}',
      );
    } on CommandTimeoutException {
      print('[ack] no answer (is the device running?)');
    } on ConnectionException catch (e) {
      print('[ack] not sent: ${e.message}');
    }
  });

  await ProcessSignal.sigint.watch().first;
  timer.cancel();
  await client.close();
  print('Bye.');
  exit(0);
}
