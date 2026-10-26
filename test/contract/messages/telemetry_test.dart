import 'package:bedaya_mqtt/src/contract/messages/telemetry.dart';
import 'package:test/test.dart';

void main() {
  group('Telemetry', () {
    test('round-trips through JSON', () {
      const t = Telemetry({'temp': 24.5, 'battery': 87, 'door': 'open'});
      final back = Telemetry.fromJson(t.toJson());
      print('  toJson()  →  ${t.toJson()}');
      expect(back.values, {'temp': 24.5, 'battery': 87, 'door': 'open'});
    });

    test('[] returns a reading or null', () {
      const t = Telemetry({'door': 'open'});
      expect(t['door'], 'open');
      expect(t['missing'], isNull);
    });

    test('number() returns only numbers', () {
      const t = Telemetry({'temp': 24.5, 'door': 'open'});
      print("  number('temp') → ${t.number('temp')}");
      print("  number('door') → ${t.number('door')}");
      expect(t.number('temp'), 24.5);
      expect(t.number('door'), isNull);
      expect(t.number('missing'), isNull);
    });

    test('toJson returns a copy', () {
      final values = <String, Object?>{'temp': 1};
      final json = Telemetry(values).toJson()..['temp'] = 2;
      expect(values['temp'], 1);
      expect(json['temp'], 2);
    });
  });
}
