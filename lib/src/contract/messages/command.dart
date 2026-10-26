import 'dart:math';

/// An instruction sent from a controller to a device.
///
/// ```json
/// {"id": "9f1c2a7b04de8361", "name": "relay", "params": {"on": true}}
/// ```
///
/// The device answers with a `CommandAck` that carries the same [id], so
/// the controller knows which command it answers.
class Command {
  /// Creates a command. A random [id] is generated if none is given.
  Command(this.name, {this.params = const {}, String? id})
    : id = id ?? _newId();

  /// Turns the device's relay on or off.
  factory Command.relay({required bool on}) =>
      Command('relay', params: {'on': on});

  factory Command.fromJson(Map<String, dynamic> json) => Command(
    json['name'] as String,
    params: (json['params'] as Map<String, dynamic>?) ?? const {},
    id: json['id'] as String,
  );

  /// Matches this command with its ack.
  final String id;

  /// What to do, like `relay`.
  final String name;

  /// Extra input for the command, like `{"on": true}`.
  final Map<String, dynamic> params;

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
