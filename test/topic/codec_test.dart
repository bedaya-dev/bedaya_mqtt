import 'dart:convert';

import 'package:bedaya_mqtt/src/errors.dart';
import 'package:bedaya_mqtt/src/topic/codec.dart';
import 'package:test/test.dart';

/// A small message used only by these tests.
class Reading {
  const Reading({required this.name, required this.value});

  factory Reading.fromJson(Map<String, dynamic> json) =>
      Reading(name: json['name'] as String, value: json['value'] as num);

  final String name;
  final num value;

  Map<String, dynamic> toJson() => {'name': name, 'value': value};

  @override
  String toString() => 'Reading(name: $name, value: $value)';
}

/// Runs [action] and prints what it returned, or the error it threw.
void show(String label, Object? Function() action) {
  try {
    print('  $label  →  ${action()}');
  } on DecodeException catch (e) {
    print('  $label  →  throws $e');
  }
}

void main() {
  const codec = JsonMessageCodec<Reading>(
    fromJson: Reading.fromJson,
    toJson: _readingToJson,
  );

  group('encode', () {
    test('turns a message into UTF-8 JSON bytes', () {
      const reading = Reading(name: 'temp', value: 24.5);
      final bytes = codec.encode(reading);
      print('  input:  $reading');
      print('  bytes:  $bytes');
      print('  as text: ${utf8.decode(bytes)}');
      expect(utf8.decode(bytes), '{"name":"temp","value":24.5}');
    });

    test('encodes non-ASCII text as UTF-8', () {
      const reading = Reading(name: 'حرارة', value: 1);
      final bytes = codec.encode(reading);
      print('  input:  $reading');
      print('  as text: ${utf8.decode(bytes)}');
      expect(utf8.decode(bytes), '{"name":"حرارة","value":1}');
    });
  });

  group('decode', () {
    test('turns JSON bytes into a message', () {
      final bytes = utf8.encode('{"name":"temp","value":24.5}');
      show('decode(${utf8.decode(bytes)})', () => codec.decode(bytes));
      final reading = codec.decode(bytes);
      expect(reading.name, 'temp');
      expect(reading.value, 24.5);
    });

    test('is the reverse of encode', () {
      const original = Reading(name: 'حرارة', value: 24.5);
      final bytes = codec.encode(original);
      print('  step 1: encode($original)  →  ${utf8.decode(bytes)}');
      final back = codec.decode(bytes);
      print('  step 2: decode(...)  →  $back');
      expect(back.name, original.name);
      expect(back.value, original.value);
    });
  });

  group('decode errors', () {
    test('throws DecodeException on bytes that are not UTF-8', () {
      final bytes = [0xff, 0xfe];
      show('decode($bytes)', () => codec.decode(bytes));
      expect(() => codec.decode(bytes), throwsA(isA<DecodeException>()));
    });

    test('throws DecodeException on text that is not JSON', () {
      final bytes = utf8.encode('hello');
      show("decode('hello')", () => codec.decode(bytes));
      expect(() => codec.decode(bytes), throwsA(isA<DecodeException>()));
    });

    test('throws DecodeException on an empty payload', () {
      show('decode([])', () => codec.decode([]));
      expect(() => codec.decode([]), throwsA(isA<DecodeException>()));
    });

    test('throws DecodeException on JSON that is not an object', () {
      for (final text in ['[1,2]', '"text"', '42', 'null']) {
        final bytes = utf8.encode(text);
        show('decode($text)', () => codec.decode(bytes));
        expect(
          () => codec.decode(bytes),
          throwsA(isA<DecodeException>()),
          reason: 'payload: $text',
        );
      }
    });

    test('throws DecodeException when fromJson fails', () {
      for (final text in ['{"name":"temp"}', '{"name":1,"value":2}']) {
        final bytes = utf8.encode(text);
        show('decode($text)', () => codec.decode(bytes));
        expect(
          () => codec.decode(bytes),
          throwsA(isA<DecodeException>()),
          reason: 'payload: $text',
        );
      }
    });

    test('keeps the original error as the cause', () {
      final bytes = utf8.encode('hello');
      try {
        codec.decode(bytes);
        fail('decode should have thrown');
      } on DecodeException catch (e) {
        print('  message: ${e.message}');
        print('  cause:   ${e.cause.runtimeType}');
        expect(e.cause, isA<FormatException>());
      }
    });
  });
}

Map<String, dynamic> _readingToJson(Reading reading) => reading.toJson();
