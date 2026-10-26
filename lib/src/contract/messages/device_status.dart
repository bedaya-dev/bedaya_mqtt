/// Whether a device is online.
///
/// A device publishes `{"online": true}` (retained) when it connects, and
/// sets `{"online": false}` as its Last Will, so the broker publishes it if
/// the device drops off without saying goodbye.
///
/// "Last seen" is not in the payload. The controller side tracks it from
/// when messages arrive.
class DeviceStatus {
  const DeviceStatus({required this.online});

  factory DeviceStatus.fromJson(Map<String, dynamic> json) =>
      DeviceStatus(online: json['online'] as bool);

  static const DeviceStatus onlineStatus = DeviceStatus(online: true);
  static const DeviceStatus offlineStatus = DeviceStatus(online: false);

  final bool online;

  Map<String, dynamic> toJson() => {'online': online};

  @override
  String toString() => 'DeviceStatus(online: $online)';
}
