import 'package:flutter/material.dart';
import 'package:intl/intl.dart';
import '../models/models.dart';
import '../services/api_client.dart';

/// DlQOperator provides a full-screen interface for managing dead-lettered
/// messages: listing with filters, retrying individual or bulk messages,
/// and purging the DLQ.
class DlQOperator extends StatefulWidget {
  final ApiClient apiClient;

  const DlQOperator({super.key, required this.apiClient});

  @override
  State<DlQOperator> createState() => _DlQOperatorState();
}

class _DlQOperatorState extends State<DlQOperator> {
  late Future<DLQListResponse> _dlqFuture;
  String _statusFilter = '';
  String _typeFilter = '';
  String _sourceFilter = '';
  int _limit = 50;
  final Set<String> _selected = {};

  @override
  void initState() {
    super.initState();
    _fetch();
  }

  void _fetch() {
    _dlqFuture = widget.apiClient.listDLQ(
      limit: _limit,
      offset: 0,
      status: _statusFilter.isNotEmpty ? _statusFilter : null,
      types: _typeFilter.isNotEmpty ? _typeFilter : null,
      sources: _sourceFilter.isNotEmpty ? _sourceFilter : null,
    );
  }

  void _refresh() {
    setState(() {
      _selected.clear();
      _fetch();
    });
  }

  Future<void> _retrySelected() async {
    if (_selected.isEmpty) {
      ScaffoldMessenger.of(context).showSnackBar(
        const SnackBar(content: Text('Select messages to retry first')),
      );
      return;
    }

    final confirmed = await _showConfirmDialog(
      context,
      'Retry ${_selected.length} DLQ messages?',
      'These messages will be re-enqueued to the main stream for reprocessing.',
    );
    if (!confirmed) return;

    try {
      final result = await widget.apiClient.retryDLQ(_selected.toList());
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(
          content: Text('Requeued: ${result.requeued}, Skipped: ${result.skipped}, Failed: ${result.failed.length}'),
          backgroundColor: result.failed.isNotEmpty ? Colors.orange : Colors.green,
        ),
      );
      _refresh();
    } catch (e) {
      _showError(e.toString());
    }
  }

  Future<void> _purgeAll() async {
    final confirmed = await _showConfirmDialog(
      context,
      'Purge entire DLQ?',
      'This action is irreversible. All DLQ records will be permanently deleted.',
    );
    if (!confirmed) return;

    try {
      final count = await widget.apiClient.purgeDLQ(archive: true);
      ScaffoldMessenger.of(context).showSnackBar(
        SnackBar(content: Text('Purged $count DLQ records'), backgroundColor: Colors.green),
      );
      _refresh();
    } catch (e) {
      _showError(e.toString());
    }
  }

  void _showError(String message) {
    ScaffoldMessenger.of(context).showSnackBar(
      SnackBar(content: Text(message), backgroundColor: Colors.red),
    );
  }

  Future<bool> _showConfirmDialog(BuildContext ctx, String title, String content) async {
    final result = await showDialog<bool>(
      context: ctx,
      builder: (context) => AlertDialog(
        title: Text(title),
        content: Text(content),
        actions: [
          TextButton(onPressed: () => Navigator.pop(context, false), child: const Text('Cancel')),
          TextButton(
            onPressed: () => Navigator.pop(context, true),
            style: TextButton.styleFrom(foregroundColor: Colors.red),
            child: const Text('Confirm'),
          ),
        ],
      ),
    );
    return result ?? false;
  }

  @override
  Widget build(BuildContext context) {
    final isDesktop = MediaQuery.of(context).size.width > 800;

    return Scaffold(
      appBar: AppBar(
        title: const Text('DLQ Operator'),
        actions: [
          IconButton(icon: const Icon(Icons.refresh), onPressed: _refresh),
          if (isDesktop) ..._buildFilterActions(),
          IconButton(
            icon: const Icon(Icons.delete_sweep, color: Colors.red),
            onPressed: _purgeAll,
          ),
        ],
      ),
      body: Column(
        children: [
          // Filters bar
          if (!isDesktop)
            Padding(
              padding: const EdgeInsets.all(8),
              child: Row(children: _buildFilterActions()),
            ),

          // Selection actions
          if (_selected.isNotEmpty)
            Container(
              color: Colors.blue.withValues(alpha: 0.1),
              padding: const EdgeInsets.symmetric(horizontal: 16, vertical: 8),
              child: Row(
                children: [
                  Text('${_selected.length} selected'),
                  const Spacer(),
                  TextButton.icon(
                    onPressed: _retrySelected,
                    icon: const Icon(Icons.refresh, size: 16),
                    label: const Text('Retry Selected'),
                  ),
                ],
              ),
            ),

          // DLQ list
          Expanded(
            child: FutureBuilder<DLQListResponse>(
              future: _dlqFuture,
              builder: (context, snapshot) {
                if (snapshot.connectionState != ConnectionState.done) {
                  return const Center(child: CircularProgressIndicator());
                }
                if (snapshot.hasError) {
                  return Center(child: Text('Error: ${snapshot.error}', style: const TextStyle(color: Colors.red)));
                }

                final data = snapshot.data!;
                if (data.data.isEmpty) {
                  return const Center(child: Text('DLQ is empty'));
                }

                return SingleChildScrollView(
                  child: PaginatedDataTable(
                    source: _DLQDataTableSource(
                      data.data,
                      _selected,
                      (id) {
                        setState(() {
                          if (_selected.contains(id)) {
                            _selected.remove(id);
                          } else {
                            _selected.add(id);
                          }
                        });
                      },
                      (DLQMessage msg) {
                        _showDlqDetail(context, msg);
                      },
                    ),
                    columnSpacing: 20,
                    headingRowHeight: 40,
                    dataRowHeight: 52,
                    columns: const [
                      DataColumn(label: Text('Select')),
                      DataColumn(label: Text('ID')),
                      DataColumn(label: Text('Type')),
                      DataColumn(label: Text('Source')),
                      DataColumn(label: Text('Reason')),
                      DataColumn(label: Text('Retries')),
                      DataColumn(label: Text('Status')),
                      DataColumn(label: Text('Failed At')),
                      DataColumn(label: Text('Actions')),
                    ],
                  ),
                );
              },
            ),
          ),
        ],
      ),
      floatingActionButton: _selected.isNotEmpty
          ? FloatingActionButton.extended(
              onPressed: _retrySelected,
              icon: const Icon(Icons.refresh),
              label: const Text('Retry'),
              backgroundColor: Colors.orange,
            )
          : null,
    );
  }

  List<Widget> _buildFilterActions() => [
        Padding(
          padding: const EdgeInsets.symmetric(horizontal: 8),
          child: DropdownButton<String>(
            value: _statusFilter.isEmpty ? null : _statusFilter,
            hint: const Text('Status'),
            items: const [
              DropdownMenuItem(value: '', child: Text('All')),
              DropdownMenuItem(value: 'pending', child: Text('Pending')),
              DropdownMenuItem(value: 'locked', child: Text('Locked')),
              DropdownMenuItem(value: 'processing', child: Text('Processing')),
              DropdownMenuItem(value: 'resolved', child: Text('Resolved')),
            ],
            onChanged: (v) => setState(() {
              _statusFilter = v ?? '';
              _fetch();
            }),
          ),
        ),
        Padding(
          padding: const EdgeInsets.symmetric(horizontal: 8),
          child: SizedBox(
            width: 200,
            child: TextField(
              decoration: InputDecoration(
                hintText: 'Filter by type or source',
                border: const OutlineInputBorder(),
              ),
              onChanged: (v) {
                _typeFilter = v;
                // Debounced fetch not implemented in this prototype.
              },
              onSubmitted: (v) => setState(() { _typeFilter = v; _fetch(); }),
            ),
          ),
        ),
      ];

  void _showDlqDetail(BuildContext context, DLQMessage msg) {
    showDialog(
      context: context,
      builder: (ctx) => AlertDialog(
        title: Text('DLQ Message: ${msg.id}'),
        content: SizedBox(
          width: 600,
          child: Column(
            mainAxisSize: MainAxisSize.min,
            children: [
              _buildDetailRow('Event ID', msg.event?.id ?? 'N/A'),
              _buildDetailRow('Type', msg.event?.type ?? 'N/A'),
              _buildDetailRow('Source', msg.event?.source ?? 'N/A'),
              _buildDetailRow('Subject', msg.event?.subject ?? 'N/A'),
              _buildDetailRow('Reason', msg.reason),
              _buildDetailRow('Retry Count', msg.retryCount.toString()),
              _buildDetailRow('Status', msg.status),
              _buildDetailRow('Failed At', DateFormat.yMMMd().add_jm().format(msg.failedAt)),
              _buildDetailRow('Consumer', msg.consumer),
              if (msg.event != null) ...[
                const SizedBox(height: 12),
                Expanded(
                  child: SingleChildScrollView(
                    child: SelectableText(msg.event!.prettyData, style: const TextStyle(fontFamily: 'monospace', fontSize: 12)),
                  ),
                ),
              ],
            ],
          ),
        ),
        actions: [
          TextButton(onPressed: () => Navigator.pop(ctx), child: const Text('Close')),
          ElevatedButton.icon(
            onPressed: () async {
              Navigator.pop(ctx);
              final result = await widget.apiClient.retryDLQ([msg.id]);
              ScaffoldMessenger.of(context).showSnackBar(
                SnackBar(content: Text('Requeued: ${result.requeued}')),
              );
              _refresh();
            },
            icon: const Icon(Icons.refresh),
            label: const Text('Retry This Message'),
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
}

// --- Data table source ---

class _DLQDataTableSource extends DataTableSource {
  final List<DLQMessage> messages;
  final Set<String> selected;
  final Function(String id) onToggleSelection;
  final void Function(DLQMessage msg)? onView;

  _DLQDataTableSource(this.messages, this.selected, this.onToggleSelection, this.onView);

  @override
  DataRow getRow(int index) {
    final msg = messages[index];
    final isSelected = selected.contains(msg.id);

    final statusColor = switch (msg.status) {
      'locked' => Colors.red,
      'processing' => Colors.orange,
      'resolved' => Colors.blue,
      _ => Colors.grey,
    };

    return DataRow(
      selected: isSelected,
      onSelectChanged: (_) => onToggleSelection(msg.id),
      cells: [
        DataCell(
          Checkbox(
            value: isSelected,
            onChanged: (_) => onToggleSelection(msg.id),
          ),
        ),
        DataCell(SelectableText(msg.id.substring(msg.id.length > 12 ? msg.id.length - 12 : 0))),
        DataCell(Text(msg.event?.type ?? '-')),
        DataCell(Text(msg.event?.source ?? '-')),
        DataCell(
          Container(
            width: 200,
            child: Text(msg.reason, overflow: TextOverflow.ellipsis, maxLines: 2),
          ),
        ),
        DataCell(Text(msg.retryCount.toString())),
        DataCell(
          Container(
            padding: const EdgeInsets.symmetric(horizontal: 8, vertical: 2),
            decoration: BoxDecoration(
              color: statusColor.withValues(alpha: 0.2),
              borderRadius: BorderRadius.circular(8),
            ),
            child: Text(msg.status, style: TextStyle(color: statusColor, fontSize: 12)),
          ),
        ),
        DataCell(Text(DateFormat.Md().add_Hm().format(msg.failedAt))),
        DataCell(
          IconButton(
            icon: const Icon(Icons.visibility, size: 16),
            onPressed: onView != null ? () => onView!(msg) : null,
            tooltip: 'View details',
          ),
        ),
      ],
    );
  }

  @override
  int get rowCount => messages.length;

  @override
  bool get isRowCountApproximate => false;

  @override
  int get selectedRowCount => selected.length;
}
