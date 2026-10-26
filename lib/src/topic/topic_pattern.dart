/// A topic template with named params, like `{deviceId}/telemetry`.
///
/// The pattern is split on `/`. A level written as `{name}` is a param; the
/// others are fixed text. One pattern does three jobs:
///
/// ```dart
/// final p = TopicPattern('{deviceId}/telemetry');
///
/// p.resolve({'deviceId': 'a1'}); // 'a1/telemetry'   → where to publish
/// p.filter();                    // '+/telemetry'    → what to subscribe to
/// p.filter({'deviceId': 'a1'});  // 'a1/telemetry'   → subscribe to one device
/// p.match('a1/telemetry');       // {deviceId: a1}   → who sent this?
/// p.match('a1/status');          // null             → not this topic
/// ```
///
/// Patterns don't include the topic prefix (`bedaya/`); the client adds it.
class TopicPattern {
  /// Parses [pattern].
  ///
  /// Throws an [ArgumentError] if it is empty, contains the MQTT wildcards
  /// `+` or `#` (use a `{param}` instead), or uses the same param twice.
  TopicPattern(this.pattern) : _segments = pattern.split('/') {
    if (pattern.isEmpty) {
      throw ArgumentError('Pattern must not be empty');
    }
    if (_segments.any((s) => s.contains('+') || s.contains('#'))) {
      throw ArgumentError('Pattern must not contain wildcards: "$pattern"');
    }
    final seen = <String>{};
    for (final segment in _segments) {
      final name = _parameterName(segment);
      if (name != null && !seen.add(name)) {
        throw ArgumentError('Duplicate param "{$name}" in "$pattern"');
      }
    }
  }

  /// The pattern as written, like `{deviceId}/telemetry`.
  final String pattern;

  /// The levels of [pattern], split on `/`.
  final List<String> _segments;

  /// `'{deviceId}'` → `'deviceId'`; fixed text → `null`.
  static String? _parameterName(String segment) {
    if (segment.length > 2 &&
        segment.startsWith('{') &&
        segment.endsWith('}')) {
      return segment.substring(1, segment.length - 1);
    }
    return null;
  }

  /// Throws if [value] would break the topic structure.
  static void _checkValue(String name, String value) {
    if (value.isEmpty) {
      throw ArgumentError('Param "$name" must not be empty');
    }
    if (value.contains('/') || value.contains('+') || value.contains('#')) {
      throw ArgumentError(
        'Param "$name" must not contain "/", "+" or "#": "$value"',
      );
    }
  }

  /// The concrete topic to publish to, with every param filled in from
  /// [params].
  ///
  /// `{deviceId}/telemetry` with `{'deviceId': 'a1'}` → `a1/telemetry`.
  ///
  /// Throws an [ArgumentError] if a param is missing, empty, or contains
  /// `/`, `+` or `#` (any of them would change which topic it means).
  String resolve(Map<String, String> params) {
    final out = <String>[];
    for (final segment in _segments) {
      final name = _parameterName(segment);
      if (name == null) {
        out.add(segment);
      } else {
        final value = params[name];
        if (value == null) {
          throw ArgumentError('Missing param "$name" for "$pattern"');
        }
        _checkValue(name, value);
        out.add(value);
      }
    }
    return out.join('/');
  }

  /// The filter to subscribe with. Params given in [params] are fixed; the
  /// others become the MQTT wildcard `+` (any value for that level).
  ///
  /// `{deviceId}/telemetry` → `+/telemetry` (all devices), or
  /// `a1/telemetry` with `{'deviceId': 'a1'}` (one device).
  ///
  /// Throws an [ArgumentError] for an empty value, or one that contains
  /// `/`, `+` or `#`.
  String filter([Map<String, String> params = const {}]) {
    final out = <String>[];
    for (final segment in _segments) {
      final name = _parameterName(segment);
      if (name == null) {
        out.add(segment);
      } else {
        final value = params[name];
        if (value == null) {
          out.add('+');
        } else {
          _checkValue(name, value);
          out.add(value);
        }
      }
    }
    return out.join('/');
  }

  /// The params of [topic] if it fits this pattern, or `null` if it
  /// doesn't.
  ///
  /// `a1/telemetry` → `{deviceId: a1}`. `a1/status` or `a1/x/telemetry`
  /// → `null`. [topic] must not include the prefix.
  Map<String, String>? match(String topic) {
    final topicSegments = topic.split('/');
    if (topicSegments.length != _segments.length) return null;

    final result = <String, String>{};
    for (var i = 0; i < _segments.length; i++) {
      final name = _parameterName(_segments[i]);
      final value = topicSegments[i];
      if (name == null) {
        if (value != _segments[i]) return null;
      } else {
        if (value.isEmpty) return null;
        result[name] = value;
      }
    }
    return result;
  }
}
