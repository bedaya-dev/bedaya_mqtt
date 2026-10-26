import 'package:bedaya_mqtt/src/contract/messages/device_status.dart';
import 'package:test/test.dart';

void main() {
  group('DeviceStatus', () {
    test('round-trips through JSON', () {
      for (final status in [
        DeviceStatus.onlineStatus,
        DeviceStatus.offlineStatus,
      ]) {
        print('  toJson()  →  ${status.toJson()}');
        expect(DeviceStatus.fromJson(status.toJson()).online, status.online);
      }
    });

    test('fromJson throws when online is missing', () {
      expect(() => DeviceStatus.fromJson({}), throwsA(isA<TypeError>()));
    });
  });
}
