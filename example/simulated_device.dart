// A fake device: publishes telemetry every 2 seconds and answers commands.
//
//   dart run example/simulated_device.dart [deviceId] [topicPrefix] [host[:port]]
//
// Run example/controller.dart in another terminal to talk to it.
//
// The default broker (broker.hivemq.com) is shared with everyone, so the
// default prefix is personal: bedaya-<your user name>. Both programs pick
// the same one on your machine; strangers get a different one.
// For a local broker: dart run example/simulated_device.dart a1 bedaya localhost:1883

import 'dart:async';
import 'dart:io';
import 'dart:math';

import 'package:bedaya_mqtt/bedaya_mqtt.dart';

import 'cli_args.dart';

Future<void> main(List<String> args) async {
  final deviceId = args.isNotEmpty ? args[0] : 'a1';
  final prefix = args.length > 1 ? args[1] : defaultPrefix();
  final (host, port) = hostAndPort(args.length > 2 ? args[2] : null);

  final client = BedayaClient(
    ClientConfig(
      host: host,
      port: port,
      topicPrefix: prefix,
      // Keep trying if the network or the broker isn't ready yet.
      retryFirstConnect: true,
    ),
  );
  client.connectionStates.listen((s) => print('[state] $s'));
  client.errors.listen((e) => print('[error] $e'));

  // Before connect, so the Last Will (offline) is set.
  final me = client.asDevice(deviceId);
  await client.connect();
  print('Device "$deviceId" online under "$prefix/". Ctrl+C to stop.');

  var relayOn = false;
  me.commands.listen((command) async {
    print('[command] $command');
    if (command.name == 'relay' && command.params['on'] is bool) {
      relayOn = command.params['on'] as bool;
      print('  relay is now ${relayOn ? 'ON' : 'OFF'}');
      await me.ack(command);
    } else {
      await me.ack(command, error: 'unknown command "${command.name}"');
    }
  });

  final random = Random();
  final timer = Timer.periodic(const Duration(seconds: 2), (_) async {
    final telemetry = Telemetry({
      'temp': double.parse((22 + random.nextDouble() * 6).toStringAsFixed(1)),
      'humidity': 40 + random.nextInt(20),
      'relay': relayOn,
    });
    try {
      await me.publish(BedayaTopics.telemetry, telemetry);
      print('[telemetry] $telemetry');
    } on ConnectionException catch (e) {
      print('[telemetry] not sent: ${e.message}');
    }
  });

  // Ctrl+C: say goodbye properly, so the status becomes offline right away.
  await ProcessSignal.sigint.watch().first;
  timer.cancel();
  await client.close();
  print('Bye.');
  exit(0);
}
