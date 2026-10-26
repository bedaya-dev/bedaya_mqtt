import 'command.dart';

/// A device's answer to a [Command].
///
/// ```json
/// {"id": "9f1c2a7b04de8361", "ok": false, "error": "relay is busy"}
/// ```
///
/// The `id` is the id of the command being answered.
class CommandAck {
  const CommandAck({required this.commandId, required this.ok, this.error});

  /// The command ran.
  CommandAck.success(Command command) : this(commandId: command.id, ok: true);

  /// The command failed, because of [error].
  CommandAck.failure(Command command, String error)
    : this(commandId: command.id, ok: false, error: error);

  factory CommandAck.fromJson(Map<String, dynamic> json) => CommandAck(
    commandId: json['id'] as String,
    ok: json['ok'] as bool,
    error: json['error'] as String?,
  );

  /// The [Command.id] this ack answers.
  final String commandId;

  /// Whether the command ran.
  final bool ok;

  /// Why the command failed, when [ok] is false.
  final String? error;

  Map<String, dynamic> toJson() => {
    'id': commandId,
    'ok': ok,
    if (error != null) 'error': error,
  };

  @override
  String toString() => 'CommandAck(${toJson()})';
}
