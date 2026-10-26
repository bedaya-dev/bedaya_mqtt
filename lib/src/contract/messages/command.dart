import 'dart:math';

/// An instruction from a controller to a device, like "turn the relay on".
///
/// On the wire, on `{prefix}/{deviceId}/command`:
///
/// ```json
/// {"id": "9f1c2a7b04de8361", "name": "relay", "params": {"on": true}}
/// ```
///
/// The device answers with a `CommandAck` that carries the same [id], so
/// the controller knows which command it answers.
///
/// ```dart
/// // Controller:
/// final ack = await client.device('a1').send(Command.relay(on: true));
/// // Your own command:
/// await client.device('a1').send(Command('blink', params: {'times': 3}));
/// ```
class Command {
  /// Creates a command called [name], with optional [params].
  ///
  /// A random [id] is made if none is given. Pass one only when you need
  /// to repeat a command with the same id.
  Command(this.name, {this.params = const {}, String? id})
    : id = id ?? _newId();

  /// Turns the device's relay on or off. Every Bedaya board understands it.
  factory Command.relay({required bool on}) =>
      Command('relay', params: {'on': on});

  /// Reads a command from its JSON object. A missing `params` becomes empty.
  ///
  /// Throws a `TypeError` if `id` or `name` is missing or not a string.
  factory Command.fromJson(Map<String, dynamic> json) => Command(
    json['name'] as String,
    params: (json['params'] as Map<String, dynamic>?) ?? const {},
    id: json['id'] as String,
  );

  /// Unique per command: 16 random hex characters. The ack carries it back.
  final String id;

  /// What to do, like `relay`.
  final String name;

  /// Extra input for the command, like `{"on": true}`.
  final Map<String, dynamic> params;

  /// The JSON object for this command. `params` is left out when empty.
  Map<String, dynamic> toJson() => {
    'id': id,
    'name': name,
    if (params.isNotEmpty) 'params': params,
  };

  @override
  String toString() => 'Command(${toJson()})';

  static final Random _random = Random.secure();

  /// 16 random hex characters.
  static String _newId() => List.generate(
    8,
    (_) => _random.nextInt(256).toRadixString(16).padLeft(2, '0'),
  ).join();
}
