/// Readings that a device publishes, by name.
///
/// On the wire, on `{prefix}/{deviceId}/telemetry`, a flat JSON object
/// (easy to build on firmware):
///
/// ```json
/// {"temp": 24.5, "battery": 87, "door": "open"}
/// ```
///
/// ```dart
/// // Device:
/// await me.publish(BedayaTopics.telemetry, Telemetry({'temp': 24.5}));
/// // Controller:
/// client.device('a1').on(BedayaTopics.telemetry).listen((t) {
///   final temp = t.number('temp'); // 24.5, or null
/// });
/// ```
///
/// The names are up to the device. For a fixed, typed shape, define your
/// own `Topic<T>` with your own message class instead.
class Telemetry {
  /// Creates telemetry from readings by name, like `{'temp': 24.5}`.
  const Telemetry(this.values);

  /// Reads telemetry from its JSON object. Every key becomes a reading.
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

  /// The JSON object for this telemetry: a copy of [values].
  Map<String, dynamic> toJson() => {...values};

  @override
  String toString() => 'Telemetry($values)';
}
