import 'package:bedaya_mqtt/src/subscriptions/subscription_registry.dart';
import 'package:bedaya_mqtt/src/transport/qos.dart';
import 'package:test/test.dart';

void main() {
  late SubscriptionRegistry registry;

  setUp(() => registry = SubscriptionRegistry());

  test('the first listener needs a subscribe', () {
    expect(registry.add('a/+', Qos.atMostOnce), Qos.atMostOnce);
    expect(registry.filters, {'a/+': Qos.atMostOnce});
  });

  test('a second listener with the same QoS does not', () {
    registry.add('a/+', Qos.atLeastOnce);
    expect(registry.add('a/+', Qos.atLeastOnce), isNull);
    expect(registry.add('a/+', Qos.atMostOnce), isNull);
  });

  test('a higher QoS needs a new subscribe', () {
    registry.add('a/+', Qos.atMostOnce);
    expect(registry.add('a/+', Qos.exactlyOnce), Qos.exactlyOnce);
    expect(registry.filters['a/+'], Qos.exactlyOnce);
  });

  test('only the last listener leaving needs an unsubscribe', () {
    registry.add('a/+', Qos.atMostOnce);
    registry.add('a/+', Qos.atMostOnce);
    print('  remove #1 → ${registry.remove('a/+')}');
    expect(registry.filters, isNotEmpty);
    expect(registry.remove('a/+'), isTrue);
    expect(registry.filters, isEmpty);
  });

  test('removing an unknown filter does nothing', () {
    expect(registry.remove('nope'), isFalse);
  });
}
