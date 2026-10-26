/// Picks the right `createTransport` for the platform at compile time:
/// TCP/TLS on native platforms, WebSocket in the browser.
library;

export 'transport_factory_stub.dart'
    if (dart.library.io) 'transport_factory_io.dart'
    if (dart.library.js_interop) 'transport_factory_web.dart';
