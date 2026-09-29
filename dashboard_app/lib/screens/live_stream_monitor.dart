import 'dart:async';
import 'package:flutter/material.dart';
import 'package:intl/intl.dart';
import '../models/models.dart';
import '../services/api_client.dart';
import '../services/ws_client.dart';

/// LiveStreamMonitor displays real-time pipeline metrics via WebSocket
/// streaming, including ingestion RPS, processing RPS, DLQ count, active
/// consumers, failure rate, and circuit breaker state with live graphs.
class LiveStreamMonitor extends StatefulWidget {
  final WsMetricsClient wsClient;
  final ApiClient apiClient;
  final HealthResponse health;

  const LiveStreamMonitor({
    super.key,
    required this.wsClient,
    required this.apiClient,
    required this.health,
  });

  @override
  State<LiveStreamMonitor> createState() => _LiveStreamMonitorState();
}

class _LiveStreamMonitorState extends State<LiveStreamMonitor> {
  StreamSubscription<MetricsEvent>? _sub;
  final List<MetricsEvent> _history = [];
  static const int _maxHistory = 60;

  @override
  void initState() {
    super.initState();
    _sub = widget.wsClient.stream.listen((event) {
      setState(() {
        _history.add(event);
        if (_history.length > _maxHistory) {
          _history.removeAt(0);
        }
      });
    }, onError: (error) {
      if (mounted) {
        ScaffoldMessenger.of(context).showSnackBar(
          SnackBar(content: Text('WebSocket error: $error'), backgroundColor: Colors.red),
        );
      }
    });
  }

  @override
  void dispose() {
    _sub?.cancel();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    final latest = _history.isNotEmpty ? _history.last : null;
    final isDark = Theme.of(context).brightness == Brightness.dark;

    return Scaffold(
      body: CustomScrollView(
        slivers: [
          SliverAppBar(
            title: const Text('Live Stream Monitor'),
            actions: [
              Container(
                margin: const EdgeInsets.only(right: 16),
                padding: const EdgeInsets.symmetric(horizontal: 12, vertical: 4),
                decoration: BoxDecoration(
                  color: widget.wsClient.isConnected
                      ? Colors.green.withValues(alpha: 0.2)
                      : Colors.red.withValues(alpha: 0.2),
                  borderRadius: BorderRadius.circular(12),
                ),
                child: Row(
                  children: [
                    CircleAvatar(
                      radius: 4,
                      backgroundColor: widget.wsClient.isConnected ? Colors.green : Colors.red,
                    ),
                    const SizedBox(width: 4),
                    Text(
                      widget.wsClient.isConnected ? 'Connected' : 'Disconnected',
                      style: TextStyle(
                        color: widget.wsClient.isConnected ? Colors.green : Colors.red,
                        fontSize: 12,
                      ),
                    ),
                  ],
                ),
              ),
            ],
          ),
          SliverPadding(
            padding: const EdgeInsets.all(16),
            sliver: SliverList(
              delegate: SliverChildListDelegate([
                // Connection status banner
                _buildStatusBanner(context),
                const SizedBox(height: 24),

                // Real-time metric cards
                Wrap(
                  spacing: 16,
                  runSpacing: 16,
                  children: [
                    _MetricCard(
                      title: 'Ingress RPS',
                      value: latest?.ingressRps ?? 0.0,
                      unit: 'events/s',
                      icon: Icons.speed,
                      color: Colors.blue,
                      isDark: isDark,
                    ),
                    _MetricCard(
                      title: 'Processed RPS',
                      value: latest?.processedRps ?? 0.0,
                      unit: 'events/s',
                      icon: Icons.download,
                      color: Colors.green,
                      isDark: isDark,
                    ),
                    _MetricCard(
                      title: 'DLQ Count',
                      value: (latest?.dlqCount ?? 0).toDouble(),
                      unit: 'messages',
                      icon: Icons.error,
                      color: Colors.red,
                      isDark: isDark,
                    ),
                    _MetricCard(
                      title: 'Active Consumers',
                      value: (latest?.activeConsumers ?? 0).toDouble(),
                      unit: 'workers',
                      icon: Icons.groups,
                      color: Colors.purple,
                      isDark: isDark,
                    ),
                    _MetricCard(
                      title: 'Failure Rate',
                      value: (latest?.failureRate ?? 0.0) * 100,
                      unit: '%',
                      icon: Icons.warning,
                      color: Colors.orange,
                      isDark: isDark,
                    ),
                  ],
                ),
                const SizedBox(height: 24),

                // Circuit breaker status
                _buildCircuitBreakerStatus(latest, isDark),
                const SizedBox(height: 24),

                // Live graph of RPS over time
                _buildRpsChart(isDark),
                const SizedBox(height: 16),

                // Historical events summary
                _buildEventsSummary(isDark),
              ]),
            ),
          ),
        ],
      ),
    );
  }

  Widget _buildStatusBanner(BuildContext context) {
    final checks = widget.health.checks;
    final allHealthy = checks.values.every((v) => v == 'ok');
    return Container(
      padding: const EdgeInsets.all(16),
      decoration: BoxDecoration(
        color: allHealthy
            ? Colors.green.withValues(alpha: 0.1)
            : Colors.orange.withValues(alpha: 0.1),
        borderRadius: BorderRadius.circular(12),
        border: Border.all(
          color: allHealthy ? Colors.green : Colors.orange,
        ),
      ),
      child: Row(
        children: [
          Icon(
            allHealthy ? Icons.check_circle : Icons.warning,
            color: allHealthy ? Colors.green : Colors.orange,
          ),
          const SizedBox(width: 12),
          Expanded(
            child: Text(
              allHealthy
                  ? 'All systems operational'
                  : 'Some services degraded — check health endpoints',
              style: TextStyle(
                color: allHealthy ? Colors.green : Colors.orange,
              ),
            ),
          ),
          if (!allHealthy)
            TextButton(
              onPressed: () {
                widget.apiClient.health().then((health) {
                  setState(() {});
                });
              },
              child: const Text('Refresh'),
            ),
        ],
      ),
    );
  }

  Widget _buildCircuitBreakerStatus(MetricsEvent? latest, bool isDark) {
    final state = latest?.circuitBreakerState ?? 'unknown';
    final color = switch (state) {
      'closed' => Colors.green,
      'open' => Colors.red,
      'half-open' => Colors.orange,
      _ => Colors.grey,
    };

    return Container(
      padding: const EdgeInsets.all(16),
      decoration: BoxDecoration(
        color: color.withValues(alpha: 0.1),
        borderRadius: BorderRadius.circular(12),
        border: Border.all(color: color),
      ),
      child: Row(
        children: [
          Icon(Icons.power, color: color, size: 24),
          const SizedBox(width: 12),
          Text(
            'Circuit Breaker: ${state.toUpperCase()}',
            style: TextStyle(color: color, fontWeight: FontWeight.bold, fontSize: 16),
          ),
          const Spacer(),
          Text(
            _history.isNotEmpty
                ? 'Last update: ${DateFormat.Hm().format(_history.last.timestamp)}'
                : 'No data yet',
            style: TextStyle(color: color.withValues(alpha: 0.7), fontSize: 12),
          ),
        ],
      ),
    );
  }

  Widget _buildRpsChart(bool isDark) {
    if (_history.isEmpty) {
      return Container(
        height: 150,
        decoration: BoxDecoration(
          color: isDark ? Colors.grey[900] : Colors.grey[200],
          borderRadius: BorderRadius.circular(12),
        ),
        child: const Center(child: Text('Waiting for data...')),
      );
    }

    return Container(
      height: 180,
      padding: const EdgeInsets.all(16),
      decoration: BoxDecoration(
        color: isDark ? Colors.grey[900] : Colors.grey[100],
        borderRadius: BorderRadius.circular(12),
      ),
      child: CustomPaint(
        size: const Size(double.infinity, double.infinity),
        painter: RpsChartPainter(
          data: _history,
          ingressColor: Colors.blue,
          processedColor: Colors.green,
          isDark: isDark,
        ),
      ),
    );
  }

  Widget _buildEventsSummary(bool isDark) {
    final latest = _history.isNotEmpty ? _history.last : null;
    return Container(
      padding: const EdgeInsets.all(16),
      decoration: BoxDecoration(
        color: isDark ? Colors.grey[900] : Colors.grey[100],
        borderRadius: BorderRadius.circular(12),
      ),
      child: Column(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          const Text('Pipeline Summary', style: TextStyle(fontSize: 18, fontWeight: FontWeight.bold)),
          const SizedBox(height: 12),
          if (latest != null)
            Column(
              children: [
                _buildSummaryRow('Service Status', widget.health.status, isDark),
                _buildSummaryRow('Version', widget.health.version, isDark),
                _buildSummaryRow('Uptime', widget.health.uptime, isDark),
                _buildSummaryRow('Redis', widget.health.checks['redis'] ?? 'unknown', isDark),
                _buildSummaryRow('Postgres', widget.health.checks['postgres'] ?? 'unknown', isDark),
              ],
            )
          else
            const Text('No data available'),
        ],
      ),
    );
  }

  Widget _buildSummaryRow(String label, String value, bool isDark) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 4),
      child: Row(
        mainAxisAlignment: MainAxisAlignment.spaceBetween,
        children: [
          Text(label, style: TextStyle(color: isDark ? Colors.grey[400] : Colors.grey[700])),
          Text(value, style: TextStyle(color: isDark ? Colors.white : Colors.black87, fontWeight: FontWeight.w500)),
        ],
      ),
    );
  }
}

// --- Chart painter ---

class RpsChartPainter extends CustomPainter {
  final List<MetricsEvent> data;
  final Color ingressColor;
  final Color processedColor;
  final bool isDark;

  RpsChartPainter({
    required this.data,
    required this.ingressColor,
    required this.processedColor,
    required this.isDark,
  });

  @override
  void paint(Canvas canvas, Size size) {
    final paint = Paint()..isAntiAlias = true;

    // Axes
    final axisPaint = paint..color = (isDark ? Colors.grey[600] : Colors.grey[400])!..strokeWidth = 1;
    final axisColor = isDark ? Colors.grey[600]! : Colors.grey[400]!;

    // Find max value for scaling.
    double maxVal = 0;
    for (final e in data) {
      if (e.ingressRps > maxVal) maxVal = e.ingressRps;
      if (e.processedRps > maxVal) maxVal = e.processedRps;
    }
    maxVal = maxVal > 0 ? maxVal * 1.1 : 1;

    final padding = 12.0;
    final chartWidth = size.width - 2 * padding;
    final chartHeight = size.height - 2 * padding;

    // Draw grid lines.
    final gridPaint = axisPaint..color = axisColor.withValues(alpha: 0.3);
    for (var i = 0; i <= 4; i++) {
      final y = padding + (chartHeight / 4) * i;
      canvas.drawLine(Offset(padding, y), Offset(size.width - padding, y), gridPaint);
    }

    // Draw axes.
    canvas.drawLine(Offset(padding, padding), Offset(padding, size.height - padding), axisPaint);
    canvas.drawLine(Offset(padding, size.height - padding), Offset(size.width - padding, size.height - padding), axisPaint);

    if (data.length < 2) {
      // Draw a single dot.
      final x = size.width / 2;
      final y = size.height / 2;
      canvas.drawCircle(Offset(x, y), 4, paint..color = ingressColor);
      return;
    }

    final step = chartWidth / (data.length - 1);

    // Draw ingress RPS line.
    final path = Path();
    for (var i = 0; i < data.length; i++) {
      final x = padding + step * i;
      final y = size.height - padding - (data[i].ingressRps / maxVal) * chartHeight;
      if (i == 0) {
        path.moveTo(x, y);
      } else {
        path.lineTo(x, y);
      }
    }
    paint.color = ingressColor;
    paint.strokeWidth = 2;
    paint.style = PaintingStyle.stroke;
    canvas.drawPath(path, paint);

    // Draw processed RPS line.
    final path2 = Path();
    for (var i = 0; i < data.length; i++) {
      final x = padding + step * i;
      final y = size.height - padding - (data[i].processedRps / maxVal) * chartHeight;
      if (i == 0) {
        path2.moveTo(x, y);
      } else {
        path2.lineTo(x, y);
      }
    }
    paint.color = processedColor;
    paint.strokeWidth = 2;
    canvas.drawPath(path2, paint);

    // Draw data points.
    for (var i = 0; i < data.length; i++) {
      final x = padding + step * i;
      final y1 = size.height - padding - (data[i].ingressRps / maxVal) * chartHeight;
      final y2 = size.height - padding - (data[i].processedRps / maxVal) * chartHeight;
      canvas.drawCircle(Offset(x, y1), 2, paint..color = ingressColor);
      canvas.drawCircle(Offset(x, y2), 2, paint..color = processedColor);
    }
  }

  @override
  bool shouldRepaint(covariant RpsChartPainter old) => old.data != data;
}

// --- Metric card ---

class _MetricCard extends StatelessWidget {
  final String title;
  final double value;
  final String unit;
  final IconData icon;
  final Color color;
  final bool isDark;

  const _MetricCard({
    required this.title,
    required this.value,
    required this.unit,
    required this.icon,
    required this.color,
    required this.isDark,
  });

  @override
  Widget build(BuildContext context) {
    final screenWidth = MediaQuery.of(context).size.width;
    final isDesktop = screenWidth > 800;
    final cardWidth = isDesktop ? 200.0 : (screenWidth - 64) / 2;

    return SizedBox(
      width: cardWidth,
      child: Card(
        elevation: 4,
        child: Padding(
          padding: const EdgeInsets.all(16),
          child: Column(
            crossAxisAlignment: CrossAxisAlignment.start,
            children: [
              Row(
                mainAxisAlignment: MainAxisAlignment.spaceBetween,
                children: [
                  Text(title, style: TextStyle(color: Colors.grey[isDark ? 400 : 700])),
                  Icon(icon, color: color, size: 20),
                ],
              ),
              const SizedBox(height: 8),
              Text(
                value.toStringAsFixed(value >= 100 ? 0 : 1),
                style: TextStyle(
                  fontSize: 28,
                  fontWeight: FontWeight.bold,
                  color: color,
                ),
              ),
              Text(unit, style: TextStyle(color: Colors.grey[isDark ? 500 : 600], fontSize: 12)),
            ],
          ),
        ),
      ),
    );
  }
}
