import 'dart:async';
import 'package:flutter/material.dart';
import 'package:intl/intl.dart';
import '../models/models.dart';
import '../services/api_client.dart';
import '../services/ws_client.dart';

/// CircuitBreakerPanel provides a 1-click manual reset for circuit breakers
/// in the pipeline (Redis streaming layer). Subscribes to live metrics via
/// WebSocket to display real-time breaker state.
class CircuitBreakerPanel extends StatefulWidget {
  final ApiClient apiClient;
  final WsMetricsClient? wsClient;

  const CircuitBreakerPanel({super.key, required this.apiClient, this.wsClient});

  @override
  State<CircuitBreakerPanel> createState() => _CircuitBreakerPanelState();
}

class _CircuitBreakerPanelState extends State<CircuitBreakerPanel> {
  late Future<CircuitBreakerInfo> _cbFuture;
  StreamSubscription<MetricsEvent>? _sub;
  CircuitBreakerInfo? _live;
  bool _isResetting = false;
  final List<String> _resetHistory = [];

  @override
  void initState() {
    super.initState();
    _fetch();
    _sub = widget.wsClient?.stream.listen((event) {
      setState(() {
        _live = CircuitBreakerInfo(
          name: 'redis-stream',
          state: event.circuitBreakerState,
          failing: 0, // not provided in WS metrics
          totalCalls: event.activeConsumers,
        );
      });
    });
  }

  @override
  void dispose() {
    _sub?.cancel();
    super.dispose();
  }

  void _fetch() {
    _cbFuture = widget.apiClient.getCircuitBreakerState();
  }

  Future<void> _reset() async {
    setState(() => _isResetting = true);

    try {
      final result = await widget.apiClient.resetCircuitBreaker();
      final now = DateFormat.jm().format(DateTime.now());
      setState(() {
        _resetHistory.insert(0, '$now — ${result['status']} (${result['name']})');
        if (_resetHistory.length > 50) _resetHistory.removeLast();
        _live = CircuitBreakerInfo(
          name: _live?.name ?? 'redis-stream',
          state: 'closed', // after reset, breaker is closed
          failing: 0,
          totalCalls: _live?.totalCalls ?? 0,
        );
      });
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: const Row(
            children: [
              Icon(Icons.check_circle, color: Colors.green),
              SizedBox(width: 8),
              Text('Circuit breaker reset successfully'),
            ],
          ),
          backgroundColor: Colors.green[900],
        ),
      );
      _fetch();
    } catch (e) {
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('Reset failed: $e'),
          backgroundColor: Colors.red[900],
        ),
      );
    } finally {
      if (mounted) setState(() => _isResetting = false);
    }
  }

  @override
  Widget build(BuildContext context) {
    return Scaffold(
      appBar: AppBar(
        title: const Text('Circuit Breaker Control'),
        actions: [
          IconButton(icon: const Icon(Icons.refresh), onPressed: () => setState(() => _fetch())),
        ],
      ),
      body: FutureBuilder<CircuitBreakerInfo>(
        future: _cbFuture,
        builder: (context, snapshot) {
          if (snapshot.connectionState != ConnectionState.done) {
            return const Center(child: CircularProgressIndicator());
          }
          if (snapshot.hasError) {
            return Center(child: Text('Error: ${snapshot.error}'));
          }

          final info = snapshot.data!;
          final liveState = _live?.state ?? info.state;
          final displayInfo = _live ?? info;

          final stateColor = _stateColor(displayInfo.state);

          return SingleChildScrollView(
            padding: const EdgeInsets.all(24),
            child: Column(
              crossAxisAlignment: CrossAxisAlignment.start,
              children: [
                // Current state card
                Card(
                  color: stateColor.withValues(alpha: 0.1),
                  child: Container(
                    decoration: BoxDecoration(
                      border: Border.all(color: stateColor, width: 2),
                      borderRadius: BorderRadius.circular(12),
                    ),
                    child: Padding(
                      padding: const EdgeInsets.all(24),
                      child: Column(
                        children: [
                          Icon(
                            Icons.power,
                            size: 48,
                            color: stateColor,
                          ),
                          const SizedBox(height: 12),
                          Text(
                            displayInfo.state.toUpperCase(),
                            style: TextStyle(
                              fontSize: 24,
                              fontWeight: FontWeight.bold,
                              color: stateColor,
                            ),
                          ),
                          const SizedBox(height: 8),
                          Text(
                            _stateDescription(displayInfo.state),
                            style: TextStyle(color: Colors.grey[400]),
                            textAlign: TextAlign.center,
                          ),
                          const SizedBox(height: 16),
                          Text(
                            'Consecutive Failures: ${displayInfo.failing}',
                            style: const TextStyle(fontSize: 16),
                          ),
                          Text(
                            'Total Calls: ${displayInfo.totalCalls}',
                            style: TextStyle(color: Colors.grey[400]),
                          ),
                        ],
                      ),
                    ),
                  ),
                ),
                const SizedBox(height: 24),

                // Reset button
                SizedBox(
                  width: double.infinity,
                  child: ElevatedButton.icon(
                    onPressed: _isResetting ? null : _reset,
                    icon: _isResetting
                        ? const SizedBox(
                            width: 18,
                            height: 18,
                            child: CircularProgressIndicator(strokeWidth: 2, color: Colors.white),
                          )
                        : const Icon(Icons.power),
                    label: Text(_isResetting ? 'Resetting...' : 'Reset Circuit Breaker'),
                    style: ElevatedButton.styleFrom(
                      backgroundColor: stateColor == Colors.red ? Colors.orange : Colors.blue,
                      padding: const EdgeInsets.symmetric(vertical: 16),
                      textStyle: const TextStyle(fontSize: 16, fontWeight: FontWeight.bold),
                    ),
                  ),
                ),
                const SizedBox(height: 24),

                // Warning
                Container(
                  padding: const EdgeInsets.all(16),
                  decoration: BoxDecoration(
                    color: Colors.orange.withValues(alpha: 0.1),
                    borderRadius: BorderRadius.circular(8),
                    border: Border.all(color: Colors.orange.withValues(alpha: 0.5)),
                  ),
                  child: const Row(
                    children: [
                      Icon(Icons.warning, color: Colors.orange),
                      SizedBox(width: 12),
                      Expanded(
                        child: Text(
                          'Resetting a circuit breaker forces it to closed state. '
                          'This may cause a surge of traffic to downstream services. '
                          'Use with caution.',
                          style: TextStyle(color: Colors.orange),
                        ),
                      ),
                    ],
                  ),
                ),
                const SizedBox(height: 24),

                // Reset history
                if (_resetHistory.isNotEmpty) ...[
                  const Text(
                    'Reset History',
                    style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold),
                  ),
                  const SizedBox(height: 8),
                  ListView.builder(
                    shrinkWrap: true,
                    physics: const NeverScrollableScrollPhysics(),
                    itemCount: _resetHistory.length,
                    itemBuilder: (context, i) => ListTile(
                      leading: const Icon(Icons.history, color: Colors.blue),
                      title: Text(_resetHistory[i]),
                    ),
                  ),
                ],
              ],
            ),
          );
        },
      ),
    );
  }

  Color _stateColor(String state) {
    switch (state) {
      case 'closed':
        return Colors.green;
      case 'open':
        return Colors.red;
      case 'half-open':
        return Colors.orange;
      default:
        return Colors.grey;
    }
  }

  String _stateDescription(String state) {
    switch (state) {
      case 'closed':
        return 'All requests are passing through normally.';
      case 'open':
        return 'Requests are failing. Circuit is open — calls are short-circuited.';
      case 'half-open':
        return 'Testing if the downstream service has recovered. Limited requests are allowed.';
      default:
        return 'State unknown.';
    }
  }
}
