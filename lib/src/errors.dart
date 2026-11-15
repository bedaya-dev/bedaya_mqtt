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

/// A message given a deadline (`publish(..., deadline:)`) could not be
/// published before it, because the client was not connected.
///
/// The message was never queued, so it will not be sent later.
/// `device.send()` turns this into a [CommandTimeoutException].
class PublishDeadlineException extends ConnectionException {
  /// Creates the exception.
  const PublishDeadlineException(super.message, [super.cause]);
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

/// The broker did not accept a subscription, or it could not be requested.
///
/// Reported on `client.errors`, never on the stream that listens to the
/// topic: that stream simply gets no messages for it. `DeviceRemote.send`
/// also throws it when its ack subscription fails, and then does not send
/// the command.
///
/// Two cases, told apart by [reason] (and [cause]):
///
/// - **The broker refused it**: it answered with a failure code (SUBACK
///   0x80). [reason] is `"rejected by the broker"` and [cause] is null.
///   Usually the broker's access control.
/// - **The request could not be sent**: `mqtt_client` failed to send the
///   SUBSCRIBE. [reason] is `"the request could not be sent"` and [cause]
///   holds the underlying error, if there was one.
///
/// A connection that ends before the broker answers is neither: that is a
/// `ConnectionException`, and the client simply asks again after the
/// reconnect.
///
/// Not every broker answers an ACL refusal with a failure code: Mosquitto
/// (checked with 2.1.2) accepts the subscription and silently drops its
/// messages, which the client can't see.
///
/// The client keeps the subscription and asks again after every
/// reconnect, so fixing the ACL on the broker is enough; there is no need
/// to listen again.
class SubscriptionException extends BedayaException {
  /// Creates the exception. Transports leave [context] out; the client
  /// fills it in before reporting it on `client.errors`.
  SubscriptionException(this.filter, this.reason, {this.context, Object? cause})
    : super(_describe(filter, reason, context), cause);

  /// The full topic filter, with the prefix, like `bedaya/a1/command`.
  final String filter;

  /// Why, in words: "rejected by the broker", or why the request could not
  /// be sent.
  final String reason;

  /// When the subscription was being made. Null when thrown by a
  /// transport, before the client adds it.
  final SubscriptionContext? context;

  /// The same exception, with [context] filled in.
  SubscriptionException withContext(SubscriptionContext context) =>
      SubscriptionException(filter, reason, context: context, cause: cause);

  static String _describe(
    String filter,
    String reason,
    SubscriptionContext? context,
  ) {
    final when = switch (context) {
      null => '',
      SubscriptionContext.firstConnect => ' (first connect)',
      SubscriptionContext.reconnect => ' (after a reconnect)',
      SubscriptionContext.newListener => ' (new listener)',
    };
    return 'Subscription to "$filter" failed$when: $reason';
  }
}

/// When a [SubscriptionException] happened.
enum SubscriptionContext {
  /// Restoring the subscriptions of streams that were listened to before
  /// the first successful connect.
  firstConnect,

  /// Restoring every subscription after the connection came back.
  reconnect,

  /// A stream started listening while connected.
  newListener,
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
