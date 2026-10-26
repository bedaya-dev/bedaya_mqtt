/// Base class for every exception thrown by this package.
///
/// Catch this to handle any bedaya_mqtt failure in one place.
abstract class BedayaException implements Exception {
  const BedayaException(this.message, [this.cause]);

  /// What went wrong, in words.
  final String message;

  /// The original error that caused this one, if any.
  final Object? cause;

  @override
  String toString() {
    final name = runtimeType.toString();
    return cause == null ? '$name: $message' : '$name: $message ($cause)';
  }
}

/// The broker can't be reached, refused the connection, or the client is
/// not connected.
class ConnectionException extends BedayaException {
  const ConnectionException(super.message, [super.cause]);
}

/// The offline queue was full, so its oldest message was thrown away.
class MessageDroppedException extends BedayaException {
  const MessageDroppedException(super.message, [super.cause]);
}

/// A device did not answer a command in time.
class CommandTimeoutException extends BedayaException {
  const CommandTimeoutException(super.message, [super.cause]);
}

/// A payload could not be turned into the expected message type.
///
/// Thrown when the bytes are not valid UTF-8 JSON, are not a JSON object,
/// or do not fit the message's `fromJson`.
class DecodeException extends BedayaException {
  const DecodeException(super.message, [super.cause]);
}
