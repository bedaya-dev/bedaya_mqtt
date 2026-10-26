class TopicPattern {
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

  final String pattern;
  final List<String> _segments;

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
