import 'dart:convert';

import 'package:bedaya_mqtt/bedaya_mqtt.dart';
import 'package:test/test.dart';

void main() {
  group('BedayaTopics', () {
    test('patterns have no prefix', () {
      final topics = [
        BedayaTopics.telemetry,
        BedayaTopics.status,
        BedayaTopics.command,
        BedayaTopics.commandAck,
      ];
      for (final t in topics) {
        print('  ${t.pattern.pattern}');
        expect(t.pattern.pattern, startsWith('{deviceId}/'));
      }
    });

    test('telemetry is QoS 0 and not retained', () {
      expect(BedayaTopics.telemetry.qos, Qos.atMostOnce);
      expect(BedayaTopics.telemetry.retained, isFalse);
    });

    test('status is retained', () {
      expect(BedayaTopics.status.retained, isTrue);
    });

    test('command and commandAck are QoS 1', () {
      expect(BedayaTopics.command.qos, Qos.atLeastOnce);
      expect(BedayaTopics.commandAck.qos, Qos.atLeastOnce);
    });

    test('resolve and match work with a deviceId', () {
      final p = BedayaTopics.commandAck.pattern;
      print("  resolve  →  ${p.resolve({'deviceId': 'a1'})}");
      expect(p.resolve({'deviceId': 'a1'}), 'a1/command/ack');
      expect(p.filter(), '+/command/ack');
      expect(p.match('a1/command/ack'), {'deviceId': 'a1'});
      expect(p.match('a1/command'), isNull);
    });

    test('codecs encode and decode the messages', () {
      final bytes = BedayaTopics.command.codec.encode(
        Command('ping', id: 'x1'),
      );
      print('  command payload  →  ${utf8.decode(bytes)}');
      expect(utf8.decode(bytes), '{"id":"x1","name":"ping"}');
      expect(BedayaTopics.command.codec.decode(bytes).id, 'x1');
    });

    test('a bad payload throws DecodeException', () {
      final bytes = utf8.encode('{"temp": 1}');
      expect(
        () => BedayaTopics.status.codec.decode(bytes),
        throwsA(isA<DecodeException>()),
      );
    });
  });
}
