import 'package:bedaya_mqtt/src/contract/messages/command.dart';
import 'package:test/test.dart';

void main() {
  group('Command', () {
    test('relay builds a relay command', () {
      final c = Command.relay(on: true);
      print('  Command.relay(on: true)  →  ${c.toJson()}');
      expect(c.name, 'relay');
      expect(c.params, {'on': true});
    });

    test('generates a different 16-char hex id each time', () {
      final a = Command('ping');
      final b = Command('ping');
      print('  ids: ${a.id}, ${b.id}');
      expect(a.id, matches(RegExp(r'^[0-9a-f]{16}$')));
      expect(a.id, isNot(b.id));
    });

    test('keeps the id it is given', () {
      expect(Command('ping', id: 'x1').id, 'x1');
    });

    test('round-trips through JSON', () {
      final c = Command.relay(on: false);
      final back = Command.fromJson(c.toJson());
      expect(back.id, c.id);
      expect(back.name, 'relay');
      expect(back.params, {'on': false});
    });

    test('leaves out empty params', () {
      final json = Command('ping', id: 'x1').toJson();
      print('  toJson()  →  $json');
      expect(json, {'id': 'x1', 'name': 'ping'});
      expect(Command.fromJson(json).params, isEmpty);
    });
  });
}
