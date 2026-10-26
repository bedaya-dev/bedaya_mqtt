import 'package:bedaya_mqtt/src/topic/topic_pattern.dart';
import 'package:test/test.dart';

/// Runs [action] and prints what it returned, or the error it threw.
void show(String label, Object? Function() action) {
  try {
    print('  $label  →  ${action()}');
  } on ArgumentError catch (e) {
    print('  $label  →  throws ArgumentError: ${e.message}');
  }
}

void main() {
  final telemetry = TopicPattern('bedaya/{deviceId}/telemetry');

  group('constructor', () {
    test('accepts a normal pattern', () {
      show(
        "TopicPattern('bedaya/{deviceId}/telemetry')",
        () => TopicPattern('bedaya/{deviceId}/telemetry').pattern,
      );
      expect(
        () => TopicPattern('bedaya/{deviceId}/telemetry'),
        returnsNormally,
      );
    });

    test('accepts a pattern with no params', () {
      show(
        "TopicPattern('bedaya/broadcast')",
        () => TopicPattern('bedaya/broadcast').pattern,
      );
      expect(() => TopicPattern('bedaya/broadcast'), returnsNormally);
    });

    test('rejects an empty pattern', () {
      show("TopicPattern('')", () => TopicPattern(''));
      expect(() => TopicPattern(''), throwsArgumentError);
    });

    test('rejects wildcards', () {
      for (final bad in ['bedaya/+/telemetry', 'bedaya/#']) {
        show("TopicPattern('$bad')", () => TopicPattern(bad));
        expect(() => TopicPattern(bad), throwsArgumentError);
      }
    });

    test('rejects wildcards inside a segment', () {
      for (final bad in ['bedaya/x+y', 'bedaya/x#']) {
        show("TopicPattern('$bad')", () => TopicPattern(bad));
        expect(() => TopicPattern(bad), throwsArgumentError);
      }
    });

    test('rejects duplicate params', () {
      show("TopicPattern('{id}/x/{id}')", () => TopicPattern('{id}/x/{id}'));
      expect(() => TopicPattern('{id}/x/{id}'), throwsArgumentError);
    });
  });

  group('resolve', () {
    test('fills in the params', () {
      final params = {'deviceId': 'a1'};
      show('resolve($params)', () => telemetry.resolve(params));
      expect(telemetry.resolve(params), 'bedaya/a1/telemetry');
    });

    test('fills in several params', () {
      final p = TopicPattern('{site}/{deviceId}/status');
      final params = {'site': 'home', 'deviceId': 'a1'};
      print('  pattern: ${p.pattern}');
      show('resolve($params)', () => p.resolve(params));
      expect(p.resolve(params), 'home/a1/status');
    });

    test('returns a pattern with no params as is', () {
      final p = TopicPattern('bedaya/broadcast');
      print('  pattern: ${p.pattern}');
      show('resolve({})', () => p.resolve({}));
      expect(p.resolve({}), 'bedaya/broadcast');
    });

    test('ignores extra params', () {
      final params = {'deviceId': 'a1', 'other': 'x'};
      show('resolve($params)', () => telemetry.resolve(params));
      expect(telemetry.resolve(params), 'bedaya/a1/telemetry');
    });

    test('throws on a missing param', () {
      show('resolve({})', () => telemetry.resolve({}));
      expect(() => telemetry.resolve({}), throwsArgumentError);
    });

    test('throws on an empty value', () {
      final params = {'deviceId': ''};
      show('resolve($params)', () => telemetry.resolve(params));
      expect(() => telemetry.resolve(params), throwsArgumentError);
    });

    test('throws on a value with "/", "+" or "#"', () {
      for (final bad in ['a1/x', '+', '#', 'a+b']) {
        final params = {'deviceId': bad};
        show('resolve($params)', () => telemetry.resolve(params));
        expect(
          () => telemetry.resolve(params),
          throwsArgumentError,
          reason: 'value: "$bad"',
        );
      }
    });
  });

  group('filter', () {
    test('uses "+" for every missing param', () {
      show('filter()', () => telemetry.filter());
      expect(telemetry.filter(), 'bedaya/+/telemetry');
    });

    test('uses the value when given', () {
      final params = {'deviceId': 'a1'};
      show('filter($params)', () => telemetry.filter(params));
      expect(telemetry.filter(params), 'bedaya/a1/telemetry');
    });

    test('mixes given params and "+"', () {
      final p = TopicPattern('{site}/{deviceId}/status');
      final params = {'site': 'home'};
      print('  pattern: ${p.pattern}');
      show('filter($params)', () => p.filter(params));
      expect(p.filter(params), 'home/+/status');
    });

    test('throws on an empty value', () {
      final params = {'deviceId': ''};
      show('filter($params)', () => telemetry.filter(params));
      expect(() => telemetry.filter(params), throwsArgumentError);
    });

    test('throws on a value with "/", "+" or "#"', () {
      for (final bad in ['a1/x', '+', '#']) {
        final params = {'deviceId': bad};
        show('filter($params)', () => telemetry.filter(params));
        expect(
          () => telemetry.filter(params),
          throwsArgumentError,
          reason: 'value: "$bad"',
        );
      }
    });
  });

  group('match', () {
    test('returns the params of a matching topic', () {
      const topic = 'bedaya/a1/telemetry';
      show("match('$topic')", () => telemetry.match(topic));
      expect(telemetry.match(topic), {'deviceId': 'a1'});
    });

    test('returns several params', () {
      final p = TopicPattern('{site}/{deviceId}/status');
      const topic = 'home/a1/status';
      print('  pattern: ${p.pattern}');
      show("match('$topic')", () => p.match(topic));
      expect(p.match(topic), {'site': 'home', 'deviceId': 'a1'});
    });

    test('returns an empty map for a pattern with no params', () {
      final p = TopicPattern('bedaya/broadcast');
      const topic = 'bedaya/broadcast';
      print('  pattern: ${p.pattern}');
      show("match('$topic')", () => p.match(topic));
      expect(p.match(topic), <String, String>{});
    });

    test('returns null when a fixed segment differs', () {
      for (final topic in ['other/a1/telemetry', 'bedaya/a1/status']) {
        show("match('$topic')", () => telemetry.match(topic));
        expect(telemetry.match(topic), isNull);
      }
    });

    test('returns null when the number of segments differs', () {
      for (final topic in ['bedaya/a1', 'bedaya/a1/x/telemetry']) {
        show("match('$topic')", () => telemetry.match(topic));
        expect(telemetry.match(topic), isNull);
      }
    });

    test('returns null when a param is empty', () {
      const topic = 'bedaya//telemetry';
      show("match('$topic')", () => telemetry.match(topic));
      expect(telemetry.match(topic), isNull);
    });

    test('is the reverse of resolve', () {
      final params = {'deviceId': 'a1'};
      final topic = telemetry.resolve(params);
      print('  step 1: resolve($params)  →  $topic');
      final back = telemetry.match(topic);
      print("  step 2: match('$topic')  →  $back");
      expect(back, params);
    });
  });
}
