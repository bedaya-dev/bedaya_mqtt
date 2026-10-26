import 'command.dart';

/// A device's answer to a [Command]: did it work?
///
/// On the wire, on `{prefix}/{deviceId}/command/ack`:
///
/// ```json
/// {"id": "9f1c2a7b04de8361", "ok": true}
/// {"id": "9f1c2a7b04de8361", "ok": false, "error": "relay is busy"}
/// ```
///
/// `id` is the id of the command being answered, so the controller knows
/// which `send()` to complete.
///
/// On the device side, `me.ack(command)` builds and sends one. On the
/// controller side, `device.send(command)` returns it.
class CommandAck {
  /// Creates an ack for the command with id [commandId].
  const CommandAck({required this.commandId, required this.ok, this.error});

  /// An ack saying [command] ran.
  CommandAck.success(Command command) : this(commandId: command.id, ok: true);

  /// An ack saying [command] failed, because of [error].
  CommandAck.failure(Command command, String error)
    : this(commandId: command.id, ok: false, error: error);

  /// Reads an ack from its JSON object.
  ///
  /// Throws a `TypeError` if `id` or `ok` is missing or has the wrong type.
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

  /// The JSON object for this ack. `error` is left out when `null`.
  Map<String, dynamic> toJson() => {
    'id': commandId,
    'ok': ok,
    if (error != null) 'error': error,
  };

  @override
  String toString() => 'CommandAck(${toJson()})';
}
