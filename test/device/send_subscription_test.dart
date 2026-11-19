import 'dart:async';

import 'package:bedaya_mqtt/bedaya_mqtt.dart';
import 'package:bedaya_mqtt/testing.dart';
import 'package:test/test.dart';

/// A [MemoryTransport] whose subscribes can be held, like a broker slow to
/// send its SUBACK. Counts the subscribe requests.
class SubackGate implements MqttTransport {
  SubackGate(this.inner);

  final MemoryTransport inner;

  /// While set, new subscribes wait for it. The end of the connection
  /// releases them, and they then fail like MqttClientTransport's do.
  Completer<void>? gate;

  final List<String> requested = [];

  @override
  TransportState get state => inner.state;
  @override
  Stream<TransportState> get states => inner.states;
  @override
  Stream<RawMessage> get messages => inner.messages;
  @override
  Future<void> connect({RawMessage? lastWill}) =>
      inner.connect(lastWill: lastWill);
  @override
  Future<void> disconnect() => inner.disconnect();
  @override
  Future<void> unsubscribe(String filter) => inner.unsubscribe(filter);
  @override
  Future<void> publish(RawMessage message) => inner.publish(message);

  @override
  Future<void> subscribe(String filter, Qos qos) async {
    requested.add(filter);
    final g = gate;
    if (g != null) {
      final ended = inner.states.firstWhere(
        (s) => s == TransportState.disconnected,
      );
      await Future.any([g.future, ended]);
    }
    await inner.subscribe(filter, qos);
  }
}

const fast = ReconnectPolicy(
  initialDelay: Duration(milliseconds: 10),
  maxDelay: Duration(milliseconds: 40),
  jitter: 0,
);

const ackFilter = 'bedaya/a1/command/ack';

void main() {
  late MemoryBroker broker;
  late SubackGate transport;
  late BedayaClient controller;
  late BedayaClient device;
  late List<Command> received;
  late List<BedayaException> errors;

  /// Whether the controller's ack subscription was confirmed when each
  /// command reached the device.
  late List<bool> ackSubscribedOnArrival;

  setUp(() async {
    broker = MemoryBroker();
    transport = SubackGate(MemoryTransport(broker));
    controller = BedayaClient(
      const ClientConfig(host: 'memory', reconnect: fast),
      transport: transport,
    );
    errors = [];
    controller.errors.listen(errors.add);

    device = BedayaClient(
      const ClientConfig(host: 'memory'),
      transport: MemoryTransport(broker),
    );
    final me = device.asDevice('a1');
    await device.connect();
    received = [];
    ackSubscribedOnArrival = [];
    me.commands.listen((c) {
      received.add(c);
      ackSubscribedOnArrival.add(
        transport.inner.subscriptions.containsKey(ackFilter),
      );
      me.ack(c);
    });

    await controller.connect();
  });

  tearDown(() async {
    final g = transport.gate;
    if (g != null && !g.isCompleted) g.complete();
    await controller.close();
    await device.close();
  });

  test('send() waits for the SUBACK of its ack subscription', () async {
    transport.gate = Completer(); // only new subscriptions are held
    final sending = controller
        .device('a1')
        .send(Command.relay(on: true), timeout: const Duration(seconds: 2));
    await Future<void>.delayed(const Duration(milliseconds: 50));

    print('  before the SUBACK: device got ${received.length} commands');
    expect(received, isEmpty, reason: 'not published before the SUBACK');
    expect(transport.inner.subscriptions, isNot(contains(ackFilter)));

    transport.gate!.complete(); // the broker answers
    final ack = await sending;
    print('  ack: $ack, subscribed on arrival: $ackSubscribedOnArrival');
    expect(transport.inner.subscriptions, contains(ackFilter));
    expect(ack.ok, isTrue);
    expect(received, hasLength(1));
    expect(ackSubscribedOnArrival, [true]);
  });

  test('a second send() does not wait for another SUBACK', () async {
    final remote = controller.device('a1');
    await remote.send(Command.relay(on: true));
    final requests = transport.requested.length;

    transport.gate = Completer(); // a new subscribe would hang
    final ack = await remote.send(
      Command.relay(on: false),
      timeout: const Duration(seconds: 1),
    );
    print('  subscribe requests: ${transport.requested}');
    expect(ack.ok, isTrue);
    expect(transport.requested, hasLength(requests), reason: 'no new request');
    expect(received, hasLength(2));

    // Also through a new DeviceRemote for the same device.
    final again = await controller
        .device('a1')
        .send(Command.relay(on: true), timeout: const Duration(seconds: 1));
    expect(again.ok, isTrue);
  });

  test('ack subscription refused: send() throws SubscriptionException and '
      'publishes nothing', () async {
    broker.rejectSubscription = (f) => f == ackFilter;
    final remote = controller.device('a1');

    final e = await remote
        .send(Command.relay(on: true))
        .then<Object?>((_) => null, onError: (Object e) => e);
    print('  send threw: $e');
    expect(e, isA<SubscriptionException>());
    expect((e! as SubscriptionException).filter, ackFilter);

    // Fails fast again, without a new request or a timeout.
    await expectLater(
      remote.send(
        Command.relay(on: false),
        timeout: const Duration(seconds: 5),
      ),
      throwsA(isA<SubscriptionException>()),
    );
    await pumpEventQueue();
    expect(received, isEmpty, reason: 'the command was never published');
    expect(
      errors.whereType<SubscriptionException>().single.context,
      SubscriptionContext.newListener,
      reason: 'reported once on errors too',
    );
  });

  test('a drop while waiting for the SUBACK ends the wait; the command goes '
      'out after the reconnect', () async {
    transport.gate = Completer(); // never answered on this connection
    final sending = controller
        .device('a1')
        .send(Command.relay(on: true), timeout: const Duration(seconds: 2));
    await Future<void>.delayed(const Duration(milliseconds: 50));
    expect(received, isEmpty);

    transport.gate = null;
    transport.inner.drop();

    final ack = await sending;
    print('  ack after the reconnect: $ack');
    expect(ack.ok, isTrue);
    expect(received, hasLength(1));
    expect(ackSubscribedOnArrival, [true]);
    expect(errors.whereType<SubscriptionException>(), isEmpty);
  });
}
