import 'dart:convert';

import 'package:bedaya_mqtt/bedaya_mqtt.dart';
import 'package:bedaya_mqtt/testing.dart';
import 'package:test/test.dart';

RawMessage text(
  String topic,
  String body, {
  Qos qos = Qos.atLeastOnce,
  bool retained = false,
}) => RawMessage(topic, utf8.encode(body), qos: qos, retained: retained);

void main() {
  late MemoryBroker broker;
  late MemoryTransport controller;
  late MemoryTransport device;

  setUp(() async {
    broker = MemoryBroker();
    controller = MemoryTransport(broker);
    device = MemoryTransport(broker);
  });

  group('connection', () {
    test('goes connecting → connected → disconnected', () async {
      final states = <TransportState>[];
      controller.states.listen(states.add);

      await controller.connect();
      await controller.disconnect();
      await pumpEventQueue();

      print('  states: $states');
      expect(states, [
        TransportState.connecting,
        TransportState.connected,
        TransportState.disconnected,
      ]);
    });

    test('connect fails while the broker is stopped', () async {
      broker.stop();
      await expectLater(
        controller.connect(),
        throwsA(isA<ConnectionException>()),
      );
      expect(controller.state, TransportState.disconnected);

      broker.start();
      await controller.connect();
      expect(controller.state, TransportState.connected);
    });

    test('stopping the broker disconnects every client', () async {
      await controller.connect();
      await device.connect();
      broker.stop();
      expect(controller.state, TransportState.disconnected);
      expect(device.state, TransportState.disconnected);
      expect(broker.clientCount, 0);
    });

    test('publish, subscribe and unsubscribe need a connection', () async {
      expect(
        () => controller.publish(text('a', 'x')),
        throwsA(isA<ConnectionException>()),
      );
      expect(
        () => controller.subscribe('a', Qos.atMostOnce),
        throwsA(isA<ConnectionException>()),
      );
      expect(
        () => controller.unsubscribe('a'),
        throwsA(isA<ConnectionException>()),
      );
    });
  });

  group('messages', () {
    setUp(() async {
      await controller.connect();
      await device.connect();
    });

    test('a subscriber gets what another client publishes', () async {
      await controller.subscribe('bedaya/+/telemetry', Qos.atMostOnce);
      final next = controller.messages.first;

      await device.publish(text('bedaya/a1/telemetry', '{"temp":24.5}'));
      final m = await next;

      print('  got: $m  →  ${utf8.decode(m.payload)}');
      expect(m.topic, 'bedaya/a1/telemetry');
      expect(utf8.decode(m.payload), '{"temp":24.5}');
    });

    test('non-matching topics are not delivered', () async {
      await controller.subscribe('bedaya/a1/#', Qos.atLeastOnce);
      final got = <String>[];
      controller.messages.listen((m) => got.add(m.topic));

      await device.publish(text('bedaya/a2/telemetry', 'x'));
      await device.publish(text('bedaya/a1/telemetry', 'x'));
      await pumpEventQueue();

      expect(got, ['bedaya/a1/telemetry']);
    });

    test('overlapping filters deliver once', () async {
      await controller.subscribe('bedaya/#', Qos.atMostOnce);
      await controller.subscribe('bedaya/+/telemetry', Qos.atMostOnce);
      final got = <RawMessage>[];
      controller.messages.listen(got.add);

      await device.publish(text('bedaya/a1/telemetry', 'x'));
      await pumpEventQueue();

      expect(got, hasLength(1));
    });

    test('delivered QoS is the lower of publish and subscribe', () async {
      await controller.subscribe('t', Qos.atMostOnce);
      final next = controller.messages.first;
      await device.publish(text('t', 'x', qos: Qos.exactlyOnce));
      expect((await next).qos, Qos.atMostOnce);
    });

    test('unsubscribe stops delivery', () async {
      await controller.subscribe('t', Qos.atMostOnce);
      await controller.unsubscribe('t');
      final got = <RawMessage>[];
      controller.messages.listen(got.add);

      await device.publish(text('t', 'x'));
      await pumpEventQueue();

      expect(got, isEmpty);
    });

    test('publish rejects a topic with wildcards', () {
      expect(
        () => device.publish(text('bedaya/+/x', 'x')),
        throwsArgumentError,
      );
    });
  });

  group('retained', () {
    setUp(() async {
      await controller.connect();
      await device.connect();
    });

    test('a new subscriber gets the retained message', () async {
      await device.publish(
        text('bedaya/a1/status', '{"online":true}', retained: true),
      );
      await controller.subscribe('bedaya/+/status', Qos.atLeastOnce);
      final m = await controller.messages.first;

      print('  got: $m');
      expect(utf8.decode(m.payload), '{"online":true}');
      expect(m.retained, isTrue);
    });

    test('live messages arrive with retained = false', () async {
      await controller.subscribe('bedaya/+/status', Qos.atLeastOnce);
      final next = controller.messages.first;
      await device.publish(text('bedaya/a1/status', 'x', retained: true));
      expect((await next).retained, isFalse);
    });

    test('only the last retained message is kept', () async {
      await device.publish(text('s', 'first', retained: true));
      await device.publish(text('s', 'second', retained: true));
      expect(utf8.decode(broker.retained['s']!.payload), 'second');
    });

    test('an empty retained payload clears the topic', () async {
      await device.publish(text('s', 'x', retained: true));
      await device.publish(text('s', '', retained: true));
      expect(broker.retained, isEmpty);
    });
  });

  group('last will', () {
    final will = text('bedaya/a1/status', '{"online":false}', retained: true);

    setUp(() async {
      await controller.connect();
      await controller.subscribe('bedaya/+/status', Qos.atLeastOnce);
      await device.connect(lastWill: will);
    });

    test('is published when the connection drops', () async {
      final next = controller.messages.first;
      device.drop();
      final m = await next;

      print('  will: ${utf8.decode(m.payload)}');
      expect(utf8.decode(m.payload), '{"online":false}');
      expect(device.state, TransportState.disconnected);
      expect(broker.retained.containsKey('bedaya/a1/status'), isTrue);
    });

    test('is not published on a normal disconnect', () async {
      final got = <RawMessage>[];
      controller.messages.listen(got.add);
      await device.disconnect();
      await pumpEventQueue();
      expect(got, isEmpty);
    });

    test('is not published when the broker stops', () async {
      broker.stop();
      expect(broker.retained, isEmpty);
    });
  });

  test('subscriptions are forgotten after a drop', () async {
    await controller.connect();
    await controller.subscribe('t', Qos.atMostOnce);
    controller.drop();
    print('  subscriptions after drop: ${controller.subscriptions}');
    expect(controller.subscriptions, isEmpty);

    await controller.connect();
    expect(controller.subscriptions, isEmpty);
  });
}
