/// Rules for MQTT topic names and filters, with `+` and `#` wildcards.
library;

/// Whether [topic] matches the subscribe [filter].
///
/// `+` matches exactly one level. `#` matches any number of levels,
/// including none, so `a/#` matches `a`, `a/b` and `a/b/c`.
bool topicMatchesFilter(String filter, String topic) {
  final f = filter.split('/');
  final t = topic.split('/');
  for (var i = 0; i < f.length; i++) {
    if (f[i] == '#') return true;
    if (i >= t.length) return false;
    if (f[i] != '+' && f[i] != t[i]) return false;
  }
  return f.length == t.length;
}

/// Throws an [ArgumentError] if [topic] can't be published to.
///
/// A topic name must not be empty and must not contain wildcards.
void checkTopicName(String topic) {
  if (topic.isEmpty) {
    throw ArgumentError('Topic must not be empty');
  }
  if (topic.contains('+') || topic.contains('#')) {
    throw ArgumentError('Topic must not contain wildcards: "$topic"');
  }
}

/// Throws an [ArgumentError] if [filter] is not a valid subscribe filter.
///
/// `+` must be a whole level. `#` must be a whole level and the last one.
void checkTopicFilter(String filter) {
  if (filter.isEmpty) {
    throw ArgumentError('Filter must not be empty');
  }
  final segments = filter.split('/');
  for (var i = 0; i < segments.length; i++) {
    final s = segments[i];
    if (s.contains('+') && s != '+') {
      throw ArgumentError('"+" must be a whole level: "$filter"');
    }
    if (s.contains('#') && (s != '#' || i != segments.length - 1)) {
      throw ArgumentError('"#" must be the whole last level: "$filter"');
    }
  }
}
