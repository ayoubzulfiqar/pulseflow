import 'dart:convert';

// MetricsEvent represents a real-time metrics update streamed over WebSocket.
class MetricsEvent {
  final double ingressRps;
  final double processedRps;
  final int dlqCount;
  final int activeConsumers;
  final double failureRate;
  final String circuitBreakerState;
  final DateTime timestamp;

  MetricsEvent({
    required this.ingressRps,
    required this.processedRps,
    required this.dlqCount,
    required this.activeConsumers,
    required this.failureRate,
    required this.circuitBreakerState,
    required this.timestamp,
  });

  factory MetricsEvent.fromJson(Map<String, dynamic> json) => MetricsEvent(
        ingressRps: (json['ingress_rps'] as num?)?.toDouble() ?? 0.0,
        processedRps: (json['processed_rps'] as num?)?.toDouble() ?? 0.0,
        dlqCount: json['dlq_count'] as int? ?? 0,
        activeConsumers: json['active_consumers'] as int? ?? 0,
        failureRate: (json['failure_rate'] as num?)?.toDouble() ?? 0.0,
        circuitBreakerState: json['circuit_breaker_state'] as String? ?? 'unknown',
        timestamp: DateTime.tryParse(json['timestamp'] as String? ?? '') ?? DateTime.now(),
      );
}

// DLQMessage represents a dead-lettered event in the control plane.
class DLQMessage {
  final String id;
  final Event? event;
  final String reason;
  final int retryCount;
  final DateTime failedAt;
  final String consumer;
  final String status;

  DLQMessage({
    required this.id,
    this.event,
    required this.reason,
    required this.retryCount,
    required this.failedAt,
    required this.consumer,
    required this.status,
  });

  factory DLQMessage.fromJson(Map<String, dynamic> json) => DLQMessage(
        id: json['id'] as String,
        event: json['event'] != null ? Event.fromJson(json['event'] as Map<String, dynamic>) : null,
        reason: json['reason'] as String? ?? '',
        retryCount: json['retry_count'] as int? ?? 0,
        failedAt: DateTime.tryParse(json['failed_at'] as String? ?? '') ?? DateTime.now(),
        consumer: json['consumer'] as String? ?? '',
        status: json['status'] as String? ?? 'pending',
      );
}

// Event represents an event payload in the pipeline.
class Event {
  final String id;
  final String source;
  final String type;
  final String subject;
  final Map<String, dynamic> data;
  final Map<String, dynamic>? metadata;
  final DateTime timestamp;

  Event({
    required this.id,
    required this.source,
    required this.type,
    required this.subject,
    required this.data,
    this.metadata,
    required this.timestamp,
  });

  factory Event.fromJson(Map<String, dynamic> json) => Event(
        id: json['id'] as String? ?? '',
        source: json['source'] as String? ?? '',
        type: json['type'] as String? ?? '',
        subject: json['subject'] as String? ?? '',
        data: Map<String, dynamic>.from(json['data'] as Map? ?? {}),
        metadata: json['metadata'] as Map<String, dynamic>?,
        timestamp: DateTime.tryParse(json['timestamp'] as String? ?? '') ?? DateTime.now(),
      );

  String get prettyData => const JsonEncoder.withIndent('  ').convert(data);
}

// CircuitBreakerState describes the runtime state of a circuit breaker.
class CircuitBreakerInfo {
  final String name;
  final String state;
  final int failing;
  final int totalCalls;

  CircuitBreakerInfo({
    required this.name,
    required this.state,
    required this.failing,
    required this.totalCalls,
  });

  factory CircuitBreakerInfo.fromJson(Map<String, dynamic> json) => CircuitBreakerInfo(
        name: json['name'] as String? ?? 'unknown',
        state: json['state'] as String? ?? 'unknown',
        failing: json['failing'] as int? ?? 0,
        totalCalls: json['total_calls'] as int? ?? 0,
      );
}

// HealthResponse is the health check response.
class HealthResponse {
  final String status;
  final String version;
  final String uptime;
  final Map<String, String> checks;

  HealthResponse({
    required this.status,
    required this.version,
    required this.uptime,
    required this.checks,
  });

  factory HealthResponse.fromJson(Map<String, dynamic> json) => HealthResponse(
        status: json['status'] as String? ?? 'unknown',
        version: json['version'] as String? ?? '',
        uptime: json['uptime'] as String? ?? '',
        checks: Map<String, String>.from(json['checks'] as Map? ?? {}),
      );
}
