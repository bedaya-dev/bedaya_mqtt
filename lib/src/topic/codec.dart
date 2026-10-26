import 'dart:convert';

import '../errors.dart';

/// Turns a value of type [T] into bytes and back.
///
/// MQTT payloads are raw bytes. A codec lets the rest of the package work
/// with typed messages instead.
abstract interface class MessageCodec<T> {
  /// Converts [value] into payload bytes.
  List<int> encode(T value);

  /// Converts payload [bytes] back into a [T].
  ///
  /// Throws a [DecodeException] if the bytes are not a valid [T].
  T decode(List<int> bytes);
}

/// A [MessageCodec] that sends [T] as a UTF-8 JSON object.
class JsonMessageCodec<T> implements MessageCodec<T> {
  const JsonMessageCodec({required this.fromJson, required this.toJson});

  /// Builds a [T] from a decoded JSON object.
  final T Function(Map<String, dynamic> json) fromJson;

  /// Turns a [T] into a JSON object.
  final Map<String, dynamic> Function(T value) toJson;

  @override
  List<int> encode(T value) => utf8.encode(jsonEncode(toJson(value)));

  @override
  T decode(List<int> bytes) {
    final Object? decoded;
    try {
      decoded = jsonDecode(utf8.decode(bytes));
    } on FormatException catch (e) {
      throw DecodeException('Payload is not valid UTF-8 JSON', e);
    }

    if (decoded is! Map<String, dynamic>) {
      throw DecodeException(
        'Expected a JSON object, got ${decoded.runtimeType}',
      );
    }

    try {
      return fromJson(decoded);
    } catch (e) {
      throw DecodeException('JSON does not match the expected message', e);
    }
  }
}
