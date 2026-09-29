import 'dart:async';

import 'package:flutter/material.dart';
import 'package:intl/intl.dart';
import 'models/models.dart';
import 'services/api_client.dart';
import 'services/ws_client.dart';
import 'screens/live_stream_monitor.dart';
import 'screens/dlq_operator.dart';
import 'screens/payload_inspector.dart';
import 'screens/circuit_breaker_panel.dart';

void main() {
  runApp(const PulseFlowApp());
}

class PulseFlowApp extends StatefulWidget {
  const PulseFlowApp({super.key});

  @override
  State<PulseFlowApp> createState() => _PulseFlowAppState();
}

class _PulseFlowAppState extends State<PulseFlowApp> {
  late Future<HealthResponse> _healthFuture;
  ApiClient? _apiClient;
  WsMetricsClient? _wsClient;

  @override
  void initState() {
    super.initState();
    _initClients();
  }

  void _initClients() {
    // The dashboard connects to the PulseFlow API. In production, the base URL
    // and API key are configured via environment variables or a settings page.
    const baseUrl = String.fromEnvironment('PULSEFLOW_API_URL', defaultValue: 'http://localhost:8080');
    const apiKey = String.fromEnvironment('PULSEFLOW_API_KEY', defaultValue: '');

    final key = apiKey.isNotEmpty ? apiKey : '';
    _apiClient = ApiClient(baseUrl: baseUrl, apiKey: key);

    _healthFuture = _checkHealth();

    // Don't wait for health check — start WebSocket immediately.
    final wsUrl = '${baseUrl.replaceFirst('http', 'ws')}/ws/metrics';
    _wsClient = WsMetricsClient(wsUrl: wsUrl);
    _wsClient?.connect();
  }

  Future<HealthResponse> _checkHealth() async {
    if (_apiClient == null) throw ApiException('API client not initialized');
    return _apiClient!.health();
  }

  @override
  void dispose() {
    _wsClient?.disconnect();
    _apiClient?.dispose();
    super.dispose();
  }

  @override
  Widget build(BuildContext context) {
    return MaterialApp(
      title: 'PulseFlow Control Plane',
      theme: ThemeData.dark().copyWith(
        colorScheme: ColorScheme.fromSeed(
          seedColor: Colors.blueAccent,
          brightness: Brightness.dark,
        ),
        useMaterial3: true,
        appBarTheme: const AppBarTheme(
          backgroundColor: Colors.transparent,
          elevation: 0,
        ),
      ),
      home: FutureBuilder<HealthResponse>(
        future: _healthFuture,
        builder: (context, snapshot) {
          if (snapshot.connectionState != ConnectionState.done) {
            return const Scaffold(
              body: Center(child: CircularProgressIndicator()),
            );
          }
          if (snapshot.hasError) {
            return Scaffold(
              body: Center(
                child: Column(
                  mainAxisSize: MainAxisSize.min,
                  children: [
                    const Icon(Icons.error_outline, size: 48, color: Colors.red),
                    const SizedBox(height: 16),
                    Text(
                      'Cannot connect to PulseFlow: ${snapshot.error}',
                      style: const TextStyle(fontSize: 16),
                      textAlign: TextAlign.center,
                    ),
                    const SizedBox(height: 24),
                    ElevatedButton(
                      onPressed: () {
                        setState(() {
                          _healthFuture = _checkHealth();
                        });
                      },
                      child: const Text('Retry'),
                    ),
                  ],
                ),
              ),
            );
          }
          return DashboardHome(
            apiClient: _apiClient!,
            wsClient: _wsClient!,
            health: snapshot.data!,
          );
        },
      ),
      debugShowCheckedModeBanner: false,
    );
  }
}

class DashboardHome extends StatefulWidget {
  final ApiClient apiClient;
  final WsMetricsClient wsClient;
  final HealthResponse health;

  const DashboardHome({
    super.key,
    required this.apiClient,
    required this.wsClient,
    required this.health,
  });

  @override
  State<DashboardHome> createState() => _DashboardHomeState();
}

class _DashboardHomeState extends State<DashboardHome> {
  int _selectedIndex = 0;

  @override
  void initState() {
    super.initState();
  }

  void _onItemTapped(int index) {
    setState(() => _selectedIndex = index);
  }

  @override
  Widget build(BuildContext context) {
    final screens = [
      LiveStreamMonitor(
        wsClient: widget.wsClient,
        apiClient: widget.apiClient,
        health: widget.health,
      ),
      DlQOperator(apiClient: widget.apiClient),
      PayloadInspector(apiClient: widget.apiClient),
      CircuitBreakerPanel(apiClient: widget.apiClient),
    ];

    return Scaffold(
      body: IndexedStack(
        index: _selectedIndex,
        children: screens,
      ),
      bottomNavigationBar: NavigationBar(
        selectedIndex: _selectedIndex,
        onDestinationSelected: _onItemTapped,
        indicatorColor: Colors.blueAccent.withValues(alpha: 0.2),
        destinations: const [
          NavigationDestination(
            icon: Icon(Icons.speed),
            label: 'Live Stream',
          ),
          NavigationDestination(
            icon: Icon(Icons.error),
            label: 'DLQ Operator',
          ),
          NavigationDestination(
            icon: Icon(Icons.insights),
            label: 'Payload Inspector',
          ),
          NavigationDestination(
            icon: Icon(Icons.power),
            label: 'Circuits',
          ),
        ],
      ),
    );
  }
}
