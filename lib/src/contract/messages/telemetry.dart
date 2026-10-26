/// Readings that a device publishes, by name.
///
/// On the wire this is a flat JSON object, which is easy to build on
/// firmware:
///
/// ```json
/// {"temp": 24.5, "battery": 87, "door": "open"}
/// ```
///
/// The names are up to the device. For a fixed, typed shape, define your
/// own `Topic<T>` with your own message class instead.
class Telemetry {
  const Telemetry(this.values);

  factory Telemetry.fromJson(Map<String, dynamic> json) => Telemetry(json);

  /// Every reading by name. Values are JSON values: numbers, strings,
  /// booleans, lists or maps.
  final Map<String, Object?> values;

  /// The reading called [name], or `null` if there is none.
  Object? operator [](String name) => values[name];

  /// The reading called [name] as a number, or `null` if it is missing or
  /// not a number.
  num? number(String name) {
    final value = values[name];
    return value is num ? value : null;
  }

  Map<String, dynamic> toJson() => {...values};

  @override
  String toString() => 'Telemetry($values)';
}
