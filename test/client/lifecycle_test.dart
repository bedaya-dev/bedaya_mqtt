import 'dart:async';
import 'dart:convert';

import 'package:bedaya_mqtt/bedaya_mqtt.dart';
import 'package:bedaya_mqtt/testing.dart';
import 'package:test/test.dart';

/// A [MemoryTransport] whose connect can be held open, to call
/// `disconnect()` or `connect()` while a connect is on the wire.
class GatedTransport implements MqttTransport {
  GatedTransport(this.inner) {
    // Like MqttClientTransport: when the connection ends, by a drop or a
    // disconnect, publishes the broker never confirmed fail at once.
    inner.states.listen((s) {
      final p = publishGate;
      if (s == TransportState.disconnected && p != null && !p.isCompleted) {
        p.complete();
      }
    });
  }

  final MemoryTransport inner;

  /// While set, `connect` waits for it before connecting.
  Completer<void>? gate;

  /// While set, `subscribe` waits for it, to hold a restore halfway.
  Completer<void>? subscribeGate;

  /// While set, `disconnect` waits for it, to leave the socket open after
  /// the client has sent its last message.
  Completer<void>? disconnectGate;

  /// While set, `publish` waits for it, to hold a queue drain halfway, like
  /// a broker that is slow to confirm. The end of the connection releases
  /// it (see the constructor); the tests never open it by hand.
  Completer<void>? publishGate;

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
  Future<void> publish(RawMessage message) async {
    final g = publishGate;
    if (g != null) await g.future;
    await inner.publish(message);
  }
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

  group('disconnect() while the first connect sends the queue', () {
    Future<void> queueOne(BedayaClient c) => c.publish(
      BedayaTopics.commandAck, // OfflinePolicy.queue
      CommandAck.success(Command('ping')),
      params: {'deviceId': 'a1'},
    );

    test('a drop meanwhile does not bring the client back', () async {
      await queueOne(client);
      transport.publishGate = Completer(); // the broker is slow to confirm
      var connectDone = false;
      final connecting = client.connect().whenComplete(() {
        connectDone = true;
      });
      await client.connectionStates.firstWhere((s) => s is Connected);
      await pumpEventQueue(); // the drain is now waiting on the broker

      final disconnecting = client.disconnect();
      transport.inner.drop(); // and the network dies while we wait
      await disconnecting;
      expect(
        connectDone,
        isTrue,
        reason: 'connect() over when disconnect returns',
      );
      await connecting; // and it completed without an error
      // Longer than the reconnect delay (10ms): a re-armed timer would
      // have fired by now.
      await Future<void>.delayed(const Duration(milliseconds: 100));

      print('  states: $states');
      expect(client.connectionState, isA<Disconnected>());
      expect(states.whereType<Reconnecting>(), isEmpty);
      expect(broker.clientCount, 0);
    });

    test('it does not wait for a stalled queue', () async {
      await queueOne(client);
      transport.publishGate = Completer(); // never confirmed
      var connectDone = false;
      final connecting = client.connect().whenComplete(() {
        connectDone = true;
      });
      await client.connectionStates.firstWhere((s) => s is Connected);
      await pumpEventQueue();

      final started = DateTime.now();
      await client.disconnect().timeout(const Duration(seconds: 1));
      print('  disconnect took ${DateTime.now().difference(started)}');
      expect(
        connectDone,
        isTrue,
        reason: 'connect() over when disconnect returns',
      );
      await connecting; // and it completed without an error

      expect(client.connectionState, isA<Disconnected>());
      expect(client.queuedMessages, 1, reason: 'kept for the next connect');
    });

    test('the next connect() starts fresh and sends what was left', () async {
      await queueOne(client);
      transport.publishGate = Completer();
      var firstDone = false;
      final first = client.connect().whenComplete(() {
        firstDone = true;
      });
      await client.connectionStates.firstWhere((s) => s is Connected);
      await pumpEventQueue();
      await client.disconnect();
      expect(firstDone, isTrue, reason: 'no old connect() left behind');
      await first;

      transport.publishGate = null;
      await client.connect();
      expect(client.connectionState, isA<Connected>());
      expect(transport.connectCalls, 2);
      expect(client.queuedMessages, 0);
    });
    test(
      'a drop alone releases a stalled drain; the reconnect sends it',
      () async {
        await queueOne(client);
        transport.publishGate = Completer(); // never confirmed by hand
        final connecting = client.connect();
        await client.connectionStates.firstWhere((s) => s is Connected);
        await pumpEventQueue();

        transport.inner.drop();
        // connect() completes (the drain failed), and the client reconnects.
        await connecting.timeout(const Duration(seconds: 1));
        transport.publishGate = null;
        await client.connectionStates.firstWhere((s) => s is Connected);
        await pumpEventQueue();
        expect(client.queuedMessages, 0, reason: 'sent after the reconnect');
        await client.disconnect();
      },
    );
  });
}
