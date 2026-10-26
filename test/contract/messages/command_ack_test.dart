import 'package:bedaya_mqtt/src/contract/messages/command.dart';
import 'package:bedaya_mqtt/src/contract/messages/command_ack.dart';
import 'package:test/test.dart';

void main() {
  final command = Command.relay(on: true);

  group('CommandAck', () {
    test('success carries the command id', () {
      final ack = CommandAck.success(command);
      print('  success  →  ${ack.toJson()}');
      expect(ack.commandId, command.id);
      expect(ack.ok, isTrue);
      expect(ack.error, isNull);
      expect(ack.toJson().containsKey('error'), isFalse);
    });

    test('failure carries the error', () {
      final ack = CommandAck.failure(command, 'relay is busy');
      print('  failure  →  ${ack.toJson()}');
      expect(ack.commandId, command.id);
      expect(ack.ok, isFalse);
      expect(ack.error, 'relay is busy');
    });

    test('round-trips through JSON', () {
      final ack = CommandAck.failure(command, 'relay is busy');
      final back = CommandAck.fromJson(ack.toJson());
      expect(back.commandId, ack.commandId);
      expect(back.ok, ack.ok);
      expect(back.error, ack.error);
    });
  });
}
