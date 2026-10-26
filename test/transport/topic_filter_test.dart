import 'package:bedaya_mqtt/src/transport/topic_filter.dart';
import 'package:test/test.dart';

void main() {
  group('topicMatchesFilter', () {
    final cases = <(String, String, bool)>[
      ('bedaya/a1/telemetry', 'bedaya/a1/telemetry', true),
      ('bedaya/a1/telemetry', 'bedaya/a2/telemetry', false),
      ('bedaya/+/telemetry', 'bedaya/a1/telemetry', true),
      ('bedaya/+/telemetry', 'bedaya/a1/x/telemetry', false),
      ('bedaya/+', 'bedaya', false),
      ('bedaya/#', 'bedaya/a1/command/ack', true),
      ('bedaya/#', 'bedaya', true),
      ('#', 'anything/at/all', true),
      ('bedaya/a1', 'bedaya/a1/telemetry', false),
      ('bedaya/a1/telemetry', 'bedaya/a1', false),
    ];
    for (final (filter, topic, expected) in cases) {
      test('$filter ~ $topic  →  $expected', () {
        expect(topicMatchesFilter(filter, topic), expected);
      });
    }
  });

  group('checkTopicName', () {
    test('accepts a normal topic', () {
      checkTopicName('bedaya/a1/telemetry');
    });

    for (final bad in ['', 'bedaya/+/x', 'bedaya/#']) {
      test('rejects "$bad"', () {
        expect(() => checkTopicName(bad), throwsArgumentError);
      });
    }
  });

  group('checkTopicFilter', () {
    for (final good in ['bedaya/+/telemetry', 'bedaya/#', '#', '+']) {
      test('accepts "$good"', () => checkTopicFilter(good));
    }

    for (final bad in ['', 'bedaya/a+', 'bedaya/#/x', 'bedaya/x#']) {
      test('rejects "$bad"', () {
        try {
          checkTopicFilter(bad);
        } on ArgumentError catch (e) {
          print('  "$bad"  →  ${e.message}');
        }
        expect(() => checkTopicFilter(bad), throwsArgumentError);
      });
    }
  });
}
