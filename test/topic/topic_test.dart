import 'dart:convert';

import 'package:bedaya_mqtt/src/topic/codec.dart';
import 'package:bedaya_mqtt/src/topic/topic.dart';
import 'package:bedaya_mqtt/src/transport/qos.dart';
import 'package:test/test.dart';

/// A small message used only by these tests.
class Reading {
  const Reading(this.value);

  factory Reading.fromJson(Map<String, dynamic> json) =>
      Reading(json['value'] as num);

  final num value;

  Map<String, dynamic> toJson() => {'value': value};
}

void main() {
  group('Topic.json', () {
    final topic = Topic.json(
      'bedaya/{deviceId}/reading',
      fromJson: Reading.fromJson,
      toJson: (r) => r.toJson(),
    );

    test('builds the pattern from the string', () {
      print('  pattern:  ${topic.pattern.pattern}');
      print('  filter(): ${topic.pattern.filter()}');
      expect(topic.pattern.pattern, 'bedaya/{deviceId}/reading');
      expect(topic.pattern.filter(), 'bedaya/+/reading');
    });

    test('uses a JSON codec', () {
      final bytes = topic.codec.encode(const Reading(24.5));
      print('  codec:  ${topic.codec.runtimeType}');
      print('  encode(Reading(24.5))  →  ${utf8.decode(bytes)}');
      expect(topic.codec, isA<JsonMessageCodec<Reading>>());
      expect(utf8.decode(bytes), '{"value":24.5}');
      expect(topic.codec.decode(bytes).value, 24.5);
    });

    test('defaults to atLeastOnce and not retained', () {
      print('  qos:      ${topic.qos}');
      print('  retained: ${topic.retained}');
      expect(topic.qos, Qos.atLeastOnce);
      expect(topic.retained, isFalse);
    });

    test('accepts qos and retained', () {
      final status = Topic.json(
        'bedaya/{deviceId}/status',
        fromJson: Reading.fromJson,
        toJson: (r) => r.toJson(),
        qos: Qos.exactlyOnce,
        retained: true,
      );
      print('  qos:      ${status.qos}');
      print('  retained: ${status.retained}');
      expect(status.qos, Qos.exactlyOnce);
      expect(status.retained, isTrue);
    });
  });

  group('Topic', () {
    test('uses the codec it is given', () {
      const codec = _TextCodec();
      final topic = Topic<String>('bedaya/{deviceId}/log', codec: codec);
      print('  codec: ${topic.codec.runtimeType}');
      expect(topic.codec, same(codec));
    });

    test('throws ArgumentError on an invalid pattern', () {
      try {
        Topic<String>('bedaya/+/log', codec: const _TextCodec());
      } on ArgumentError catch (e) {
        print("  Topic('bedaya/+/log')  →  throws ArgumentError: ${e.message}");
      }
      expect(
        () => Topic<String>('bedaya/+/log', codec: const _TextCodec()),
        throwsArgumentError,
      );
    });
  });
}

/// A plain-text codec, to show that [Topic] works with any [MessageCodec].
class _TextCodec implements MessageCodec<String> {
  const _TextCodec();

  @override
  List<int> encode(String value) => utf8.encode(value);

  @override
  String decode(List<int> bytes) => utf8.decode(bytes);
}
