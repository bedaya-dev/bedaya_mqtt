/// Tracing: what the client is doing, step by step.
///
/// Off by default. Turn it on with [ClientConfig.logger]:
///
/// ```dart
/// BedayaClient(ClientConfig(host: 'broker.hivemq.com', logger: print));
/// ```
///
/// prints lines like:
///
/// ```text
/// 12:00:01.204 [info] connecting to broker.hivemq.com
/// 12:00:01.391 [info] connected
/// 12:00:01.392 [debug] subscribe bedaya/+/telemetry (atMostOnce)
/// 12:00:03.410 [debug] received bedaya/a1/telemetry {"temp":24.5}
/// 12:00:09.001 [warning] connection lost
/// 12:00:09.002 [info] reconnect #1 in 0:00:01.000000
/// ```
library;

import 'client_config.dart';

/// How important a [BedayaLogRecord] is.
enum BedayaLogLevel {
  /// Every message in and out, every subscribe. Very chatty.
  debug,

  /// Connection changes: connecting, connected, reconnecting, disconnected.
  info,

  /// Something went wrong but the client handles it: a lost connection, a
  /// bad payload, a full queue.
  warning,

  /// Something failed and needs your attention: a connect that failed, or
  /// reconnecting that gave up.
  error,
}

/// One step the client took. Passed to [ClientConfig.logger].
class BedayaLogRecord {
  /// Creates a record. The client does this; you only read them.
  BedayaLogRecord(this.level, this.message, {this.error, DateTime? time})
    : time = time ?? DateTime.now();

  /// How important this is.
  final BedayaLogLevel level;

  /// What happened, in words.
  final String message;

  /// The error behind it, if any.
  final Object? error;

  /// When it happened.
  final DateTime time;

  /// One line: time, level, message and error.
  ///
  /// `12:00:01.204 [info] connected`
  @override
  String toString() {
    final t = time.toIso8601String().substring(11, 23);
    final e = error == null ? '' : ' ($error)';
    return '$t [${level.name}] $message$e';
  }
}

/// Receives the client's [BedayaLogRecord]s. `print` works as one.
typedef BedayaLogger = void Function(BedayaLogRecord record);
