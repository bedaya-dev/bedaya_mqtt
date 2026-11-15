import 'package:bedaya_mqtt/bedaya_mqtt.dart';
import 'package:bedaya_mqtt/testing.dart';
import 'package:test/test.dart';

/// A controller and a device talking through one in-memory broker.
void main() {
  late MemoryBroker broker;
  late MemoryTransport deviceTransport;
  late BedayaClient controllerClient;
  late BedayaClient deviceClient;
  late DeviceEndpoint me;

  setUp(() async {
    broker = MemoryBroker();
    deviceTransport = MemoryTransport(broker);
    controllerClient = BedayaClient(
      const ClientConfig(host: 'memory'),
      transport: MemoryTransport(broker),
    );
    deviceClient = BedayaClient(
      const ClientConfig(host: 'memory'),
      transport: deviceTransport,
    );
    me = deviceClient.asDevice('a1');
    await controllerClient.connect();
    await deviceClient.connect();
  });

  group('device role', () {
    test('telemetry reaches the controller', () async {
      final next = controllerClient
          .device('a1')
          .on(BedayaTopics.telemetry)
          .first;
      await me.publish(BedayaTopics.telemetry, const Telemetry({'temp': 24.5}));

      final t = await next;
      print('  controller got: $t');
      expect(t['temp'], 24.5);
    });

    test('publishes online when it connects (retained)', () async {
      final status = await controllerClient.device('a1').status.first;
      print('  status: $status');
      expect(status.online, isTrue);
    });

    test('publishes offline on disconnect', () async {
      final statuses = <bool>[];
      controllerClient
          .device('a1')
          .status
          .listen((s) => statuses.add(s.online));
      await pumpEventQueue();

      await deviceClient.disconnect();
      await pumpEventQueue();

      print('  statuses: $statuses');
      expect(statuses, [true, false]);
    });

    test('the Last Will says offline when the connection drops', () async {
      final statuses = <bool>[];
      controllerClient
          .device('a1')
          .status
          .listen((s) => statuses.add(s.online));
      await pumpEventQueue();

      deviceTransport.drop();
      await pumpEventQueue();

      print('  statuses: $statuses');
      expect(statuses, [true, false]);
    });

    test('asDevice after connect throws', () {
      final other = BedayaClient(
        const ClientConfig(host: 'memory'),
        transport: MemoryTransport(broker),
      );
      return other.connect().then((_) {
        expect(() => other.asDevice('b1'), throwsStateError);
      });
    });

    test('asDevice twice: same id is fine, another id throws', () {
      expect(deviceClient.asDevice('a1'), same(me));
      expect(() => deviceClient.asDevice('a2'), throwsStateError);
    });
  });

  group('commands', () {
    test('send gets the ack back', () async {
      me.commands.listen((c) => me.ack(c));
      await pumpEventQueue();

      final command = Command.relay(on: true);
      final ack = await controllerClient.device('a1').send(command);

      print('  sent: $command');
      print('  ack:  $ack');
      expect(ack.commandId, command.id);
      expect(ack.ok, isTrue);
    });

    test('a failed command comes back with its error', () async {
      me.commands.listen((c) => me.ack(c, error: 'relay is busy'));
      await pumpEventQueue();

      final ack = await controllerClient
          .device('a1')
          .send(Command.relay(on: true));
      expect(ack.ok, isFalse);
      expect(ack.error, 'relay is busy');
    });

    test('the device receives the command params', () async {
      final received = me.commands.first;
      await pumpEventQueue();

      controllerClient
          .device('a1')
          .send(Command.relay(on: false), timeout: Duration(milliseconds: 50))
          .ignore();
      final c = await received;
      expect(c.name, 'relay');
      expect(c.params, {'on': false});
    });

    test('times out when nobody answers', () async {
      await expectLater(
        controllerClient
            .device('a1')
            .send(Command('ping'), timeout: Duration(milliseconds: 50)),
        throwsA(isA<CommandTimeoutException>()),
      );
    });

    test('ignores acks for other commands', () async {
      me.commands.listen((c) async {
        await me.ack(Command('other'));
        await me.ack(c);
      });
      await pumpEventQueue();

      final command = Command('ping');
      final ack = await controllerClient.device('a1').send(command);
      expect(ack.commandId, command.id);
    });

    test('commands for another device are not received', () async {
      final got = <Command>[];
      me.commands.listen(got.add);
      await pumpEventQueue();

      await controllerClient.publish(
        BedayaTopics.command,
        Command('ping'),
        params: {'deviceId': 'a2'},
      );
      await pumpEventQueue();
      expect(got, isEmpty);
    });
  });

  group('commands while the controller is offline', () {
    late BedayaClient offline;
    late List<Command> got;

    setUp(() async {
      offline = BedayaClient(
        const ClientConfig(
          host: 'memory',
          reconnect: ReconnectPolicy(
            initialDelay: Duration(milliseconds: 10),
            maxDelay: Duration(milliseconds: 40),
            jitter: 0,
          ),
        ),
        transport: MemoryTransport(broker),
      );
      got = [];
      me.commands.listen((c) {
        got.add(c);
        me.ack(c);
      });
      await pumpEventQueue();
    });

    test('a command that timed out is never sent later', () async {
      await expectLater(
        offline
            .device('a1')
            .send(Command.relay(on: true), timeout: Duration(milliseconds: 50)),
        throwsA(isA<CommandTimeoutException>()),
      );
      print('  queued after the timeout: ${offline.queuedMessages}');
      expect(offline.queuedMessages, 0);

      await offline.connect();
      await Future<void>.delayed(const Duration(milliseconds: 50));
      print('  the device got: $got');
      expect(got, isEmpty);
    });

    test('the same after a drop: the reconnect does not send it', () async {
      final transport = MemoryTransport(broker);
      final dropping = BedayaClient(
        const ClientConfig(
          host: 'memory',
          reconnect: ReconnectPolicy(
            initialDelay: Duration(milliseconds: 200),
            maxDelay: Duration(milliseconds: 200),
            jitter: 0,
          ),
        ),
        transport: transport,
      );
      await dropping.connect();
      transport.drop();
      await pumpEventQueue();

      await expectLater(
        dropping
            .device('a1')
            .send(Command.relay(on: true), timeout: Duration(milliseconds: 50)),
        throwsA(isA<CommandTimeoutException>()),
      );
      await dropping.connectionStates.firstWhere((s) => s is Connected);
      await Future<void>.delayed(const Duration(milliseconds: 50));
      expect(got, isEmpty);
    });

    test('is sent if the connection comes back within the timeout', () async {
      final command = Command.relay(on: true);
      final sending = offline
          .device('a1')
          .send(command, timeout: const Duration(seconds: 2));
      await Future<void>.delayed(const Duration(milliseconds: 30));
      expect(got, isEmpty);

      await offline.connect();
      final ack = await sending;
      expect(ack.commandId, command.id);
      expect(got.map((c) => c.id), [command.id]);
    });

    test('close() while waiting throws a ConnectionException', () async {
      final sending = offline
          .device('a1')
          .send(Command('ping'), timeout: const Duration(seconds: 2));
      final result = expectLater(
        sending,
        throwsA(
          allOf(
            isA<ConnectionException>(),
            isNot(isA<CommandTimeoutException>()),
          ),
        ),
      );
      await pumpEventQueue();
      await offline.close();
      await result;
    });

    test('publishing on BedayaTopics.command directly is not queued '
        'either', () async {
      await expectLater(
        offline.publish(
          BedayaTopics.command,
          Command('ping'),
          params: {'deviceId': 'a1'},
        ),
        throwsA(isA<ConnectionException>()),
      );
      expect(offline.queuedMessages, 0);
    });
  });
}
