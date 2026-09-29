import 'dart:convert';
import 'package:flutter/material.dart';
import 'package:intl/intl.dart';
import '../models/models.dart';
import '../services/api_client.dart';

/// PayloadInspector allows operators to search and inspect individual
/// events stored in PostgreSQL, view their JSON payload and headers,
/// and trigger a one-click replay of any event back to the stream.
class PayloadInspector extends StatefulWidget {
  final ApiClient apiClient;

  const PayloadInspector({super.key, required this.apiClient});

  @override
  State<PayloadInspector> createState() => _PayloadInspectorState();
}

class _PayloadInspectorState extends State<PayloadInspector> {
  late Future<List<Event>> _eventsFuture;
  final _typeController = TextEditingController();
  final _sourceController = TextEditingController();
  final _subjectController = TextEditingController();
  final _limitController = TextEditingController(text: '50');
  int _limit = 50;

  @override
  void initState() {
    super.initState();
    _fetch();
  }

  void _fetch() {
    _eventsFuture = widget.apiClient.queryEvents(
      limit: _limit,
      types: _typeController.text.isNotEmpty ? [_typeController.text] : null,
      sources: _sourceController.text.isNotEmpty ? [_sourceController.text] : null,
      subjects: _subjectController.text.isNotEmpty ? [_subjectController.text] : null,
    );
  }

  Future<void> _replayEvent(Event event) async {
    final confirmed = await showDialog<bool>(
      context: context,
      builder: (ctx) => AlertDialog(
        title: const Text('Replay Event?'),
        content: Text('Event ${event.id} will be re-published to the stream for reprocessing.'),
        actions: [
          TextButton(onPressed: () => Navigator.pop(ctx, false), child: const Text('Cancel')),
          ElevatedButton.icon(
            onPressed: () => Navigator.pop(ctx, true),
            icon: const Icon(Icons.refresh),
            label: const Text('Replay'),
          ),
        ],
      ),
    );

    if (confirmed != true) return;

    // Replay by publishing the event's timestamp as the time window.
    final result = await widget.apiClient.replay(
      from: event.timestamp.toUtc().toIso8601String(),
      to: event.timestamp.toUtc().toIso8601String(),
      types: [event.type],
    );

    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(
        content: Text('Replayed: ${result.successful} successful, ${result.failed} failed'),
        backgroundColor: result.failed > 0 ? Colors.orange : Colors.green,
      ),
    );
  }

  void _showEventDetail(Event event) {
    showDialog(
      context: context,
      builder: (ctx) => AlertDialog(
        title: Text('Event: ${event.id}'),
        content: SizedBox(
          width: 600,
          child: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              _buildDetailRow('ID', event.id),
              _buildDetailRow('Type', event.type),
              _buildDetailRow('Source', event.source),
              _buildDetailRow('Subject', event.subject),
              _buildDetailRow('Timestamp', DateFormat.yMMMd().add_jm().format(event.timestamp)),
              if (event.metadata != null) ...[
                const SizedBox(height: 12),
                Align(
                  alignment: Alignment.centerLeft,
                  child: Text('Metadata', style: Theme.of(ctx).textTheme.titleSmall),
                ),
                SelectableText(
                  const JsonEncoder.withIndent('  ').convert(event.metadata),
                  style: const TextStyle(fontFamily: 'monospace', fontSize: 12),
                ),
              ],
              const SizedBox(height: 12),
              Align(
                alignment: Alignment.centerLeft,
                child: Text('Payload', style: Theme.of(ctx).textTheme.titleSmall),
              ),
              Expanded(
                child: SingleChildScrollView(
                  child: SelectableText(
                    event.prettyData,
                    style: const TextStyle(fontFamily: 'monospace', fontSize: 12),
                  ),
                ),
              ),
            ],
          ),
        ),
        actions: [
          TextButton(onPressed: () => Navigator.pop(ctx), child: const Text('Close')),
          ElevatedButton.icon(
            onPressed: () {
              Navigator.pop(ctx);
              _replayEvent(event);
            },
            icon: const Icon(Icons.refresh),
            label: const Text('Replay Event'),
          ),
        ],
      ),
    );
  }

  Widget _buildDetailRow(String label, String value) {
    return Padding(
      padding: const EdgeInsets.symmetric(vertical: 4),
      child: Row(
        crossAxisAlignment: CrossAxisAlignment.start,
        children: [
          SizedBox(width: 120, child: Text(label, style: const TextStyle(fontWeight: FontWeight.bold, color: Colors.grey))),
          Expanded(child: Text(value)),
        ],
      ),
    );
  }

  @override
  Widget build(BuildContext context) {
    final isDesktop = MediaQuery.of(context).size.width > 800;

    return Scaffold(
      appBar: AppBar(
        title: const Text('Payload Inspector'),
        actions: [
          IconButton(icon: const Icon(Icons.refresh), onPressed: () => setState(() => _fetch())),
        ],
      ),
      body: Column(
        children: [
          // Search bar
          Padding(
            padding: const EdgeInsets.all(16),
            child: Wrap(
              spacing: 12,
              runSpacing: 8,
              children: [
                if (isDesktop)
                  SizedBox(
                    width: 200,
                    child: TextField(
                      controller: _typeController,
                      decoration: const InputDecoration(labelText: 'Event Type', border: OutlineInputBorder()),
                    ),
                  ),
                if (isDesktop)
                  SizedBox(
                    width: 200,
                    child: TextField(
                      controller: _sourceController,
                      decoration: const InputDecoration(labelText: 'Source', border: OutlineInputBorder()),
                    ),
                  ),
                if (isDesktop)
                  SizedBox(
                    width: 200,
                    child: TextField(
                      controller: _subjectController,
                      decoration: const InputDecoration(labelText: 'Subject', border: OutlineInputBorder()),
                    ),
                  ),
                SizedBox(
                  width: isDesktop ? 120 : 80,
                  child: TextField(
                    decoration: const InputDecoration(labelText: 'Limit', border: OutlineInputBorder()),
                    keyboardType: TextInputType.number,
                    controller: _limitController,
                    onChanged: (v) {
                      final n = int.tryParse(v);
                      if (n != null && n > 0 && n <= 1000) _limit = n;
                    },
                  ),
                ),
              ],
            ),
          ),
          Padding(
            padding: const EdgeInsets.symmetric(horizontal: 16),
            child: Align(
              alignment: Alignment.centerLeft,
              child: ElevatedButton.icon(
                onPressed: () => setState(() => _fetch()),
                icon: const Icon(Icons.search),
                label: const Text('Search'),
              ),
            ),
          ),

          // Event list
          Expanded(
            child: FutureBuilder<List<Event>>(
              future: _eventsFuture,
              builder: (context, snapshot) {
                if (snapshot.connectionState != ConnectionState.done) {
                  return const Center(child: CircularProgressIndicator());
                }
                if (snapshot.hasError) {
                  return Center(child: Text('Error: ${snapshot.error}'));
                }

                final events = snapshot.data!;
                if (events.isEmpty) {
                  return const Center(child: Text('No events found. Adjust filters and try again.'));
                }

                return ListView.separated(
                  itemCount: events.length,
                  separatorBuilder: (_, __) => const Divider(height: 1),
                  itemBuilder: (context, i) {
                    final event = events[i];
                    final shortId = event.id.substring(event.id.length > 8 ? event.id.length - 8 : 0);
                    return ListTile(
                      leading: CircleAvatar(
                        backgroundColor: _typeColor(event.type),
                        child: Text(event.type.substring(0).toUpperCase(), style: const TextStyle(fontSize: 12)),
                      ),
                      title: Text('${event.type} — ${event.subject}'),
                      subtitle: Text('${event.source}  ·  ${DateFormat.Md().add_Hm().format(event.timestamp)}  ·  ${shortId}'),
                      trailing: IconButton(
                        icon: const Icon(Icons.refresh),
                        tooltip: 'Replay this event',
                        onPressed: () => _replayEvent(event),
                      ),
                      onTap: () => _showEventDetail(event),
                    );
                  },
                );
              },
            ),
          ),
        ],
      ),
    );
  }

  Color _typeColor(String type) {
    final hash = type.hashCode;
    return Colors.primaries[hash % Colors.primaries.length];
  }
}
