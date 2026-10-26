/// Whether a device is online.
///
/// On the wire, on `{prefix}/{deviceId}/status`, always retained (the
/// broker keeps the last one for anyone who subscribes later):
///
/// ```json
/// {"online": true}
/// ```
///
/// Who sends it:
///
/// - The device, `{"online": true}`, right after it connects.
/// - The device, `{"online": false}`, when it disconnects on purpose.
/// - The **broker**, `{"online": false}`, when the device drops off without
///   saying goodbye (power cut, lost WiFi). The device registers this as
///   its "Last Will" when connecting.
///
/// The client does all of this for you in the device role
/// (`client.asDevice(id)`). Read it with `client.device(id).status`.
///
/// "Last seen" is not in the payload. `client.devices` tracks it from when
/// messages arrive.
class DeviceStatus {
  /// Creates a status. Usually you use [onlineStatus] or [offlineStatus].
  const DeviceStatus({required this.online});

  /// Reads a status from its JSON object.
  ///
  /// Throws a `TypeError` if `online` is missing or not a boolean.
  factory DeviceStatus.fromJson(Map<String, dynamic> json) =>
      DeviceStatus(online: json['online'] as bool);

  /// `{"online": true}`.
  static const DeviceStatus onlineStatus = DeviceStatus(online: true);

  /// `{"online": false}`.
  static const DeviceStatus offlineStatus = DeviceStatus(online: false);

  /// Whether the device is connected to the broker.
  final bool online;

  /// The JSON object for this status.
  Map<String, dynamic> toJson() => {'online': online};

  @override
  String toString() => 'DeviceStatus(online: $online)';
}
