import 'dart:convert';
import 'package:http/http.dart' as http;
import '../models/models.dart';

/// ApiClient is the REST client for all PulseFlow control-plane APIs.
class ApiClient {
  final String baseUrl;
  final String? apiKey;
  final http.Client _http;

  ApiClient({
    required this.baseUrl,
    this.apiKey,
    http.Client? httpClient,
  })  : _http = httpClient ?? http.Client(),
        assert(!baseUrl.endsWith('/'), 'baseUrl must not end with /');

  Map<String, String> get _headers {
    final h = {'Content-Type': 'application/json'};
    if (apiKey != null && apiKey!.isNotEmpty) {
      h['X-API-Key'] = apiKey!;
    }
    return h;
  }

  // --- Health ---
  Future<HealthResponse> health() async {
    final resp = await _http.get(Uri.parse('$baseUrl/health'), headers: _headers);
    if (resp.statusCode != 200) throw ApiException('Health check failed: ${resp.statusCode}');
    return HealthResponse.fromJson(jsonDecode(resp.body) as Map<String, dynamic>);
  }

  // --- Events ---
  Future<List<Event>> queryEvents({
    int limit = 100,
    int offset = 0,
    List<String>? types,
    List<String>? sources,
    List<String>? subjects,
  }) async {
    final params = <String, String>{
      'limit': limit.toString(),
      'offset': offset.toString(),
      if (types != null && types.isNotEmpty) 'types': types.join(','),
      if (sources != null && sources.isNotEmpty) 'sources': sources.join(','),
      if (subjects != null && subjects.isNotEmpty) 'subjects': subjects.join(','),
    };
    final uri = Uri.parse('$baseUrl/v1/events').replace(queryParameters: params);
    final resp = await _http.get(uri, headers: _headers);
    if (resp.statusCode != 200) throw ApiException('Query events failed: ${resp.statusCode}');
    final body = jsonDecode(resp.body) as Map<String, dynamic>;
    final data = body['data'] as List? ?? [];
    return data.map((e) => Event.fromJson(e as Map<String, dynamic>)).toList();
  }

  // --- DLQ ---
  Future<DLQListResponse> listDLQ({
    int limit = 100,
    int offset = 0,
    String? status,
    String? types,
    String? sources,
  }) async {
    final params = <String, String>{
      'limit': limit.toString(),
      'offset': offset.toString(),
      if (status != null) 'status': status,
      if (types != null) 'types': types,
      if (sources != null) 'sources': sources,
    };
    final uri = Uri.parse('$baseUrl/v1/admin/dlq').replace(queryParameters: params);
    final resp = await _http.get(uri, headers: _headers);
    if (resp.statusCode != 200) throw ApiException('List DLQ failed: ${resp.statusCode}');
    final body = jsonDecode(resp.body) as Map<String, dynamic>;
    final msgs = (body['data'] as List? ?? []).map((e) => DLQMessage.fromJson(e as Map<String, dynamic>)).toList();
    return DLQListResponse(
      data: msgs,
      count: body['count'] as int? ?? msgs.length,
      offset: body['offset'] as int? ?? 0,
      limit: body['limit'] as int? ?? limit,
    );
  }

  Future<DLQRetryResult> retryDLQ(List<String> ids) async {
    final resp = await _http.post(
      Uri.parse('$baseUrl/v1/admin/dlq/retry'),
      headers: _headers,
      body: jsonEncode({'ids': ids}),
    );
    if (resp.statusCode != 200) throw ApiException('Retry DLQ failed: ${resp.statusCode}');
    final body = jsonDecode(resp.body) as Map<String, dynamic>;
    return DLQRetryResult.fromJson(body);
  }

  Future<int> purgeDLQ({bool archive = true}) async {
    final resp = await _http.delete(
      Uri.parse('$baseUrl/v1/admin/dlq/purge?archive=$archive'),
      headers: _headers,
    );
    if (resp.statusCode != 200) throw ApiException('Purge DLQ failed: ${resp.statusCode}');
    final body = jsonDecode(resp.body) as Map<String, dynamic>;
    return body['deleted'] as int? ?? 0;
  }

  // --- Replay ---
  Future<ReplayResult> replay({
    required String from,
    required String to,
    List<String>? types,
    List<String>? sources,
    List<String>? subjects,
    int? maxEvents,
  }) async {
    final body = <String, dynamic>{
      'from': from,
      'to': to,
      if (types != null) 'types': types,
      if (sources != null) 'sources': sources,
      if (subjects != null) 'subjects': subjects,
      if (maxEvents != null) 'max_events': maxEvents,
    };
    final resp = await _http.post(
      Uri.parse('$baseUrl/v1/admin/replay'),
      headers: _headers,
      body: jsonEncode(body),
    );
    if (resp.statusCode != 200) throw ApiException('Replay failed: ${resp.statusCode}');
    final b = jsonDecode(resp.body) as Map<String, dynamic>;
    return ReplayResult.fromJson(b);
  }

  // --- Circuit Breaker ---
  Future<CircuitBreakerInfo> getCircuitBreakerState() async {
    final resp = await _http.get(
      Uri.parse('$baseUrl/v1/admin/circuit-breaker'),
      headers: _headers,
    );
    if (resp.statusCode != 200) throw ApiException('Circuit breaker query failed: ${resp.statusCode}');
    return CircuitBreakerInfo.fromJson(jsonDecode(resp.body) as Map<String, dynamic>);
  }

  Future<Map<String, dynamic>> resetCircuitBreaker() async {
    final resp = await _http.post(
      Uri.parse('$baseUrl/v1/admin/circuit-breaker/reset'),
      headers: _headers,
    );
    if (resp.statusCode != 200) throw ApiException('Circuit breaker reset failed: ${resp.statusCode}');
    return jsonDecode(resp.body) as Map<String, dynamic>;
  }

  void dispose() => _http.close();
}

// --- Response wrappers ---

class DLQListResponse {
  final List<DLQMessage> data;
  final int count;
  final int offset;
  final int limit;

  DLQListResponse({required this.data, required this.count, required this.offset, required this.limit});
}

class DLQRetryResult {
  final int requested;
  final int requeued;
  final int skipped;
  final List<DLQRetryFailure> failed;

  DLQRetryResult({required this.requested, required this.requeued, required this.skipped, required this.failed});

  factory DLQRetryResult.fromJson(Map<String, dynamic> json) => DLQRetryResult(
        requested: json['requested'] as int? ?? 0,
        requeued: json['requeued'] as int? ?? 0,
        skipped: json['skipped'] as int? ?? 0,
        failed: (json['failed'] as List? ?? []).map((e) => DLQRetryFailure.fromJson(e as Map<String, dynamic>)).toList(),
      );
}

class DLQRetryFailure {
  final String id;
  final String error;

  DLQRetryFailure({required this.id, required this.error});

  factory DLQRetryFailure.fromJson(Map<String, dynamic> json) => DLQRetryFailure(
        id: json['id'] as String? ?? '',
        error: json['error'] as String? ?? '',
      );
}

class ReplayResult {
  final int replayed;
  final int successful;
  final int failed;
  final List<String> errors;

  ReplayResult({required this.replayed, required this.successful, required this.failed, required this.errors});

  factory ReplayResult.fromJson(Map<String, dynamic> json) => ReplayResult(
        replayed: json['replayed'] as int? ?? 0,
        successful: json['successful'] as int? ?? 0,
        failed: json['failed'] as int? ?? 0,
        errors: (json['errors'] as List? ?? []).map((e) => e as String).toList(),
      );
}

class ApiException implements Exception {
  final String message;
  ApiException(this.message);
  @override
  String toString() => 'ApiException: $message';
}
