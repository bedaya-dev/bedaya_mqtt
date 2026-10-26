/// Base class for every exception thrown by this package.
///
/// Catch this to handle any bedaya_mqtt failure in one place:
///
/// ```dart
/// try {
///   await client.connect();
/// } on BedayaException catch (e) {
///   print(e.message);  // what went wrong
///   print(e.cause);    // the lower-level error, if any
/// }
/// ```
abstract class BedayaException implements Exception {
  /// Creates an exception with a [message] and the error that caused it,
  /// if any.
  const BedayaException(this.message, [this.cause]);

  /// What went wrong, in words.
  final String message;

  /// The original error that caused this one, if any. For example a
  /// `SocketException` or a `HandshakeException` (TLS) behind a
  /// [ConnectionException].
  final Object? cause;

  @override
  String toString() {
    final name = runtimeType.toString();
    return cause == null ? '$name: $message' : '$name: $message ($cause)';
  }
}

/// The broker can't be reached, refused the connection, or the client is
/// not connected.
///
/// Common messages:
/// - `Could not connect to host:port`: wrong host or port, no network, or a
///   TLS problem (see [cause]).
/// - `Broker refused the connection: notAuthorized`: wrong username or
///   password.
/// - `Not connected`: publishing while offline with the queue turned off.
class ConnectionException extends BedayaException {
  /// Creates the exception.
  const ConnectionException(super.message, [super.cause]);
}

/// The offline queue was full, so its oldest message was thrown away.
///
/// Reported on `client.errors`, not thrown. Raise
/// `ClientConfig.maxQueueSize` if this happens often.
class MessageDroppedException extends BedayaException {
  /// Creates the exception.
  const MessageDroppedException(super.message, [super.cause]);
}

/// A device did not answer a command in time.
///
/// Thrown by `device.send()`. The device may be offline, or its code may
/// not call `ack`.
class CommandTimeoutException extends BedayaException {
  /// Creates the exception.
  const CommandTimeoutException(super.message, [super.cause]);
}

/// A payload could not be turned into the expected message type.
///
/// Happens when the bytes are not valid UTF-8 JSON, are not a JSON object,
/// or do not fit the message's `fromJson`. Reported on `client.errors`;
/// the message is skipped.
class DecodeException extends BedayaException {
  /// Creates the exception.
  const DecodeException(super.message, [super.cause]);
}
