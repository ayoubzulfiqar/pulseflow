package usecase

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/ayoubzulfiqar/pulseflow/internal/entity"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// mockMonitorRepo implements entity.MonitorRepository for testing.
type mockMonitorRepo struct {
	mu     sync.Mutex
	lastDT map[string]time.Time     // key: destID
	alerts map[string][]*entity.AlertPayload // key: destID
	hbs    []*entity.HeartbeatExpectation
}

func newMockMonitorRepo() *mockMonitorRepo {
	return &mockMonitorRepo{
		lastDT: make(map[string]time.Time),
		alerts: make(map[string][]*entity.AlertPayload),
		hbs:    make([]*entity.HeartbeatExpectation, 0),
	}
}

func (m *mockMonitorRepo) LastDeliveryTime(ctx context.Context, destID string, eventType string) (*time.Time, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	ts, ok := m.lastDT[destID]
	if !ok {
		return nil, nil
	}
	return &ts, nil
}

func (m *mockMonitorRepo) ListHeartbeatExpectations(ctx context.Context) ([]*entity.HeartbeatExpectation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Return copies to avoid mutation issues.
	result := make([]*entity.HeartbeatExpectation, len(m.hbs))
	copy(result, m.hbs)
	return result, nil
}

func (m *mockMonitorRepo) SaveHeartbeatExpectation(ctx context.Context, hb *entity.HeartbeatExpectation) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.hbs = append(m.hbs, hb)
	return nil
}

func (m *mockMonitorRepo) UpdateLastDelivery(ctx context.Context, destID string, eventType string, ts time.Time) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.lastDT[destID] = ts
	return nil
}

func (m *mockMonitorRepo) MarkAlerted(ctx context.Context, destID string, ts time.Time) error {
	return nil
}

// mockAlertSender records all sent alerts.
type mockAlertSender struct {
	mu     sync.Mutex
	alerts []*entity.AlertPayload
}

func (m *mockAlertSender) SendAlert(ctx context.Context, alert *entity.AlertPayload) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.alerts = append(m.alerts, alert)
	return nil
}

func (m *mockAlertSender) CountAlerts() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.alerts)
}

func TestMonitor_NoHeartbeatMissed(t *testing.T) {
	repo := newMockMonitorRepo()
	alerts := &mockAlertSender{}

	now := time.Now().UTC()
	repo.lastDT["dest_1"] = now.Add(-1 * time.Hour) // within 24h window

	hb := &entity.HeartbeatExpectation{
		DestinationID:    "dest_1",
		EventType:        "user.active",
		ExpectedInterval: 24 * time.Hour,
		Enabled:          true,
	}
	repo.hbs = append(repo.hbs, hb)

	monitor := NewMonitor(repo, alerts, 0, nil)
	monitor.checkExpectations()

	assert.Equal(t, 0, alerts.CountAlerts(), "should not alert when within expected interval")
}

func TestMonitor_HeartbeatMissed_AlertFired(t *testing.T) {
	repo := newMockMonitorRepo()
	alerts := &mockAlertSender{}

	now := time.Now().UTC()
	// Last delivery was 25 hours ago — exceeded 24h expectation.
	repo.lastDT["dest_1"] = now.Add(-25 * time.Hour)

	hb := &entity.HeartbeatExpectation{
		DestinationID:    "dest_1",
		EventType:        "user.active",
		ExpectedInterval: 24 * time.Hour,
		Enabled:          true,
	}
	repo.hbs = append(repo.hbs, hb)

	monitor := NewMonitor(repo, alerts, 0, nil)
	monitor.checkExpectations()

	assert.Equal(t, 1, alerts.CountAlerts(), "should fire alert when heartbeat exceeded")
	assert.Equal(t, "dest_1", alerts.alerts[0].DestinationID)
	assert.Equal(t, "user.active", alerts.alerts[0].EventType)
}

func TestMonitor_DisabledHeartbeat_NoAlert(t *testing.T) {
	repo := newMockMonitorRepo()
	alerts := &mockAlertSender{}

	now := time.Now().UTC()
	repo.lastDT["dest_1"] = now.Add(-48 * time.Hour)

	hb := &entity.HeartbeatExpectation{
		DestinationID:    "dest_1",
		EventType:        "user.active",
		ExpectedInterval: 24 * time.Hour,
		Enabled:          false, // disabled
	}
	repo.hbs = append(repo.hbs, hb)

	monitor := NewMonitor(repo, alerts, 0, nil)
	monitor.checkExpectations()

	assert.Equal(t, 0, alerts.CountAlerts(), "should not alert when heartbeat is disabled")
}

func TestMonitor_NoPriorDelivery_NoAlert(t *testing.T) {
	repo := newMockMonitorRepo()
	alerts := &mockAlertSender{}

	// No last delivery time — skip alerting.
	hb := &entity.HeartbeatExpectation{
		DestinationID:    "dest_1",
		EventType:        "user.active",
		ExpectedInterval: 24 * time.Hour,
		Enabled:          true,
	}
	repo.hbs = append(repo.hbs, hb)

	monitor := NewMonitor(repo, alerts, 0, nil)
	monitor.checkExpectations()

	assert.Equal(t, 0, alerts.CountAlerts(), "should not alert on first run without prior delivery")
}

func TestMonitor_PreventsDuplicateAlerts(t *testing.T) {
	repo := newMockMonitorRepo()
	alerts := &mockAlertSender{}

	now := time.Now().UTC()
	repo.lastDT["dest_1"] = now.Add(-26 * time.Hour)

	hb := &entity.HeartbeatExpectation{
		DestinationID:    "dest_1",
		EventType:        "user.active",
		ExpectedInterval: 24 * time.Hour,
		Enabled:          true,
		AlertedAt:        &now, // already alerted recently
	}
	repo.hbs = append(repo.hbs, hb)

	monitor := NewMonitor(repo, alerts, 0, nil)
	monitor.checkExpectations()

	assert.Equal(t, 0, alerts.CountAlerts(), "should not re-alert within the same gap window")
}

func TestMonitor_AlertPayloadContainsGapInfo(t *testing.T) {
	repo := newMockMonitorRepo()
	alerts := &mockAlertSender{}

	now := time.Now().UTC()
	lastDelivery := now.Add(-25 * time.Hour)
	repo.lastDT["dest_1"] = lastDelivery

	hb := &entity.HeartbeatExpectation{
		DestinationID:    "dest_1",
		EventType:        "user.active",
		ExpectedInterval: 24 * time.Hour,
		Enabled:          true,
	}
	repo.hbs = append(repo.hbs, hb)

	monitor := NewMonitor(repo, alerts, 0, nil)
	monitor.checkExpectations()

	require.Equal(t, 1, alerts.CountAlerts())
	alert := alerts.alerts[0]
	assert.Equal(t, "dest_1", alert.DestinationID)
	assert.Equal(t, "user.active", alert.EventType)
	assert.Contains(t, alert.Message, "dest_1")
	assert.Contains(t, alert.Message, "user.active")
}

func TestMonitor_FormatDuration(t *testing.T) {
	assert.Equal(t, "5 seconds", entity.FormatDuration(5*time.Second))
	assert.Equal(t, "5 minutes", entity.FormatDuration(5*time.Minute))
	assert.Equal(t, "24 hours", entity.FormatDuration(24*time.Hour))
}

func TestMonitor_StartAndStop(t *testing.T) {
	repo := newMockMonitorRepo()
	alerts := &mockAlertSender{}

	// Very short interval for testing.
	monitor := NewMonitor(repo, alerts, 1*time.Millisecond, nil)
	monitor.Start()
	time.Sleep(10 * time.Millisecond)
	monitor.Stop()

	// Should not block — monitor.Stop() should cleanly shut down.
}

func TestMonitor_AlertSender_ImplementsInterface(t *testing.T) {
	var _ entity.AlertSender = (*mockAlertSender)(nil)
}

func TestMonitor_Repo_ImplementsInterface(t *testing.T) {
	var _ entity.MonitorRepository = (*mockMonitorRepo)(nil)
}
