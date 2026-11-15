// A device and a controller in one program, on a public broker.
//
//   dart run example/example.dart
//
// For two separate programs, see simulated_device.dart and controller.dart.

import 'dart:math';

import 'package:bedaya_mqtt/bedaya_mqtt.dart';

Future<void> main() async {
  // The public broker is shared, so use a prefix nobody else uses.
  final prefix = 'bedaya-example-${Random().nextInt(1 << 32)}';
  ClientConfig config() => ClientConfig(
    host: 'broker.hivemq.com',
    topicPrefix: prefix,
    retryFirstConnect: true,
  );

  // --- Device: publishes telemetry and answers commands. ---
  final fanClient = BedayaClient(config());
  // asDevice before connect: it sets the Last Will.
  final livingRoomFan = fanClient.asDevice('living-room-fan');
  await fanClient.connect();

  livingRoomFan.commands.listen((command) {
    print('device got: ${command.name} ${command.params}');
    livingRoomFan.ack(command);
  });

  // --- Controller: watches the device and sends it a command. ---
  final controllerClient = BedayaClient(config());
  await controllerClient.connect();
  final livingRoomFanRemote = controllerClient.device('living-room-fan');

  livingRoomFanRemote.status.listen(
    (s) => print('status: ${s.online ? 'online' : 'offline'}'),
  );
  final firstReading = livingRoomFanRemote.on(BedayaTopics.telemetry).first;

  await Future<void>.delayed(const Duration(milliseconds: 500));
  await livingRoomFan.publish(
    BedayaTopics.telemetry,
    const Telemetry({'temp': 24.5}),
  );
  print('controller got: ${await firstReading}');

  final ack = await livingRoomFanRemote.send(Command.relay(on: true));
  print('ack: ${ack.ok ? 'ok' : ack.error}');

  await controllerClient.close();
  await fanClient.close();
}
