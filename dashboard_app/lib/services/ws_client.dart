import 'dart:async';
import 'dart:convert';
import 'package:web_socket_channel/web_socket_channel.dart';
import '../models/models.dart';

/// WsMetricsClient connects to the PulseFlow WebSocket metrics stream,
/// deserializes MetricsEvent messages, and broadcasts them to listeners.
class WsMetricsClient {
  final String wsUrl;
  final Duration reconnectDelay;
  final Duration maxReconnectDelay;
  final int maxRetries;

  WsMetricsClient({
    required this.wsUrl,
    this.reconnectDelay = const Duration(seconds: 2),
    this.maxReconnectDelay = const Duration(seconds: 30),
    this.maxRetries = 1000,
  });

  WebSocketChannel? _channel;
  StreamController<MetricsEvent>? _controller;
  int _retryCount = 0;
  Timer? _reconnectTimer;
  bool _manuallyClosed = false;

  /// Stream of live metrics events.
  Stream<MetricsEvent> get stream => _controller?.stream ?? const Stream.empty();

  /// Connects to the WebSocket and begins streaming metrics.
  void connect() {
    _manuallyClosed = false;
    _controller ??= StreamController<MetricsEvent>.broadcast();
    _attemptConnection();
  }

  void _attemptConnection() {
    if (_manuallyClosed) return;

    try {
      _channel = WebSocketChannel.connect(Uri.parse(wsUrl));
      _retryCount = 0;

      _channel!.stream.listen(
        (data) {
          try {
            final json = jsonDecode(data as String) as Map<String, dynamic>;
            // Heartbeat events are emitted on connect; skip them in the
            // metrics stream (they don't carry RPS data).
            if (json['type'] == 'connected') return;
            _controller?.add(MetricsEvent.fromJson(json));
          } catch (e) {
            // Ignore malformed messages.
          }
        },
        onError: (error) {
          _scheduleReconnect();
        },
        onDone: () {
          _scheduleReconnect();
        },
      );
    } catch (e) {
      _scheduleReconnect();
    }
  }

  void _scheduleReconnect() {
    if (_manuallyClosed) return;
    _retryCount++;

    if (_retryCount > maxRetries) {
      _controller?.addError('Max reconnect attempts exceeded');
      return;
    }

    var delay = reconnectDelay * _retryCount;
    if (delay > maxReconnectDelay) {
      delay = maxReconnectDelay;
    }

    _reconnectTimer = Timer(delay, () {
      _channel = null;
      _attemptConnection();
    });
  }

  /// Gracefully disconnects and stops reconnection attempts.
  void disconnect() {
    _manuallyClosed = true;
    _reconnectTimer?.cancel();
    _channel?.sink?.close();
    _channel = null;
    _controller?.close();
    _controller = null;
  }

  /// Whether the client is currently connected.
  bool get isConnected => _channel != null;
}
