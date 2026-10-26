import '../transport/qos.dart';

/// Remembers which filters are subscribed, and how many listeners each has.
///
/// Several streams can listen to the same filter. The transport should
/// subscribe once, when the first listener comes, and unsubscribe when the
/// last one leaves. After a reconnect, [filters] says what to subscribe to
/// again.
class SubscriptionRegistry {
  final Map<String, _Entry> _entries = {};

  /// Every filter with at least one listener, and its QoS.
  Map<String, Qos> get filters => {
    for (final MapEntry(:key, :value) in _entries.entries) key: value.qos,
  };

  /// Adds a listener for [filter].
  ///
  /// Returns the QoS to subscribe with if the transport must subscribe:
  /// the filter is new, or [qos] is higher than before. Returns `null` if
  /// the current subscription already covers it.
  Qos? add(String filter, Qos qos) {
    final entry = _entries[filter];
    if (entry == null) {
      _entries[filter] = _Entry(qos);
      return qos;
    }
    entry.count++;
    if (qos.index > entry.qos.index) {
      entry.qos = qos;
      return qos;
    }
    return null;
  }

  /// Removes a listener for [filter].
  ///
  /// Returns `true` if it was the last one, so the transport should
  /// unsubscribe.
  bool remove(String filter) {
    final entry = _entries[filter];
    if (entry == null) return false;
    entry.count--;
    if (entry.count > 0) return false;
    _entries.remove(filter);
    return true;
  }
}

class _Entry {
  _Entry(this.qos);

  Qos qos;
  int count = 1;
}
