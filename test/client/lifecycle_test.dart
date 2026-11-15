import 'dart:async';
import 'dart:convert';

import 'package:bedaya_mqtt/bedaya_mqtt.dart';
import 'package:bedaya_mqtt/testing.dart';
import 'package:test/test.dart';

/// A [MemoryTransport] whose connect can be held open, to call
/// `disconnect()` or `connect()` while a connect is on the wire.
class GatedTransport implements MqttTransport {
  GatedTransport(this.inner);

  final MemoryTransport inner;

  /// While set, `connect` waits for it before connecting.
  Completer<void>? gate;

  /// While set, `subscribe` waits for it, to hold a restore halfway.
  Completer<void>? subscribeGate;

  /// While set, `disconnect` waits for it, to leave the socket open after
  /// the client has sent its last message.
  Completer<void>? disconnectGate;

  int connectCalls = 0;

  @override
  TransportState get state => inner.state;

  @override
  Stream<TransportState> get states => inner.states;

  @override
  Stream<RawMessage> get messages => inner.messages;

  @override
  Future<void> connect({RawMessage? lastWill}) async {
    connectCalls++;
    final g = gate;
    if (g != null) await g.future;
    await inner.connect(lastWill: lastWill);
  }

  @override
  Future<void> disconnect() async {
    final g = disconnectGate;
    if (g != null) await g.future;
    await inner.disconnect();
  }

  @override
  Future<void> subscribe(String filter, Qos qos) async {
    final g = subscribeGate;
    if (g != null) await g.future;
    await inner.subscribe(filter, qos);
  }

  @override
  Future<void> unsubscribe(String filter) => inner.unsubscribe(filter);

  @override
  Future<void> publish(RawMessage message) => inner.publish(message);
}

const fast = ReconnectPolicy(
  initialDelay: Duration(milliseconds: 10),
  maxDelay: Duration(milliseconds: 40),
  jitter: 0,
);

void main() {
  late MemoryBroker broker;
  late GatedTransport transport;
  late BedayaClient client;
  late List<MqttConnectionState> states;

  setUp(() {
    broker = MemoryBroker();
    transport = GatedTransport(MemoryTransport(broker));
    client = BedayaClient(
      const ClientConfig(host: 'memory', reconnect: fast),
      transport: transport,
    );
    states = [];
    client.connectionStates.listen(states.add);
  });

  group('disconnect() while connecting', () {
    test('cancels the connect: it throws, and the client stays '
        'disconnected', () async {
      transport.gate = Completer();
      final connecting = client.connect();
      final connectResult = expectLater(
        connecting,
        throwsA(isA<ConnectionException>()),
      );
      await pumpEventQueue();
      expect(client.connectionState, isA<Connecting>());

      final disconnecting = client.disconnect();
      transport.gate!.complete(); // the transport connects anyway
      await disconnecting;
      await connectResult;
      await pumpEventQueue();

      print('  states: $states');
      expect(client.connectionState, isA<Disconnected>());
      expect(states.whereType<Connected>(), isEmpty);
      expect(transport.state, TransportState.disconnected);
      expect(broker.clientCount, 0);
    });

    test('a device does not go online', () async {
      final device = BedayaClient(
        const ClientConfig(host: 'memory', reconnect: fast),
        transport: transport,
      );
      device.asDevice('a1');
      transport.gate = Completer();
      final connecting = device.connect().then((_) {}, onError: (_) {});
      await pumpEventQueue();

      final disconnecting = device.disconnect();
      transport.gate!.complete();
      await disconnecting;
      await connecting;

      print('  retained: ${broker.retained.keys}');
      expect(broker.retained, isEmpty);
    });

    test('connect() works again afterwards', () async {
      transport.gate = Completer();
      final first = client.connect().then((_) {}, onError: (_) {});
      await pumpEventQueue();
      final disconnecting = client.disconnect();
      transport.gate!.complete();
      await disconnecting;
      await first;

      transport.gate = null;
      await client.connect();
      expect(client.connectionState, isA<Connected>());
      expect(broker.clientCount, 1);
    });
  });

  group('connect() called twice', () {
    test('both wait for the same attempt', () async {
      transport.gate = Completer();
      var firstDone = false;
      var secondDone = false;
      final first = client.connect().then((_) => firstDone = true);
      final second = client.connect().then((_) => secondDone = true);
      await pumpEventQueue();

      // Neither completes while the connect is still on the wire.
      expect(firstDone, isFalse);
      expect(secondDone, isFalse);

      transport.gate!.complete();
      await Future.wait([first, second]);
      expect(client.connectionState, isA<Connected>());
      expect(transport.connectCalls, 1);
    });

    test('both get the error when it fails', () async {
      broker.stop();
      final first = client.connect();
      final second = client.connect();
      await expectLater(first, throwsA(isA<ConnectionException>()));
      await expectLater(second, throwsA(isA<ConnectionException>()));
      expect(transport.connectCalls, 1);
    });
  });

  test('disconnect() during a reconnect attempt: nothing comes back '
      'later', () async {
    await client.connect();
    transport.gate = Completer();
    transport.inner.drop();
    await client.connectionStates.firstWhere((s) => s is Reconnecting);
    // Let the retry timer fire, so an attempt is on the wire.
    await Future<void>.delayed(const Duration(milliseconds: 30));
    expect(transport.connectCalls, 2);

    final disconnecting = client.disconnect();
    transport.gate!.complete(); // that attempt succeeds after disconnect()
    await disconnecting;
    await Future<void>.delayed(const Duration(milliseconds: 100));

    print('  states: $states');
    expect(client.connectionState, isA<Disconnected>());
    expect(transport.state, TransportState.disconnected);
    expect(broker.clientCount, 0);
  });

  test('disconnect() while a reconnect is restoring: the device ends '
      'offline, not online', () async {
    final device = BedayaClient(
      const ClientConfig(host: 'memory', reconnect: fast),
      transport: transport,
    );
    device.asDevice('a1');
    device.subscribe(BedayaTopics.command).listen((_) {});
    await device.connect();

    // Hold the next restore halfway: subscriptions not back, no "online".
    transport.subscribeGate = Completer();
    transport.inner.drop();
    await device.connectionStates.firstWhere((s) => s is Connected);

    // Without the fix: "offline" goes out now, then the restore's
    // "online" while the socket is still open, and "online" sticks.
    transport.disconnectGate = Completer();
    final disconnecting = device.disconnect();
    await Future<void>.delayed(const Duration(milliseconds: 20));
    transport.subscribeGate!.complete(); // the restore goes on: "online"
    await Future<void>.delayed(const Duration(milliseconds: 20));
    transport.disconnectGate!.complete();
    await disconnecting;

    final status = broker.retained['bedaya/a1/status'];
    final text = status == null ? null : utf8.decode(status.payload);
    print('  retained status: $text');
    expect(text, contains('"online":false'));
  });
}
