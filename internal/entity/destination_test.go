package entity

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestHasSecondarySecret_ActiveRotation(t *testing.T) {
	d := &Destination{
		PrimarySecret:     "primary-secret",
		SecondarySecret:   "secondary-secret",
		RotationExpiresAt: time.Now().Add(1 * time.Hour).UTC(),
	}

	assert.True(t, d.HasSecondarySecret(), "secondary should be active during rotation window")
}

func TestHasSecondarySecret_ExpiredRotation(t *testing.T) {
	d := &Destination{
		PrimarySecret:     "primary-secret",
		SecondarySecret:   "secondary-secret",
		RotationExpiresAt: time.Now().Add(-1 * time.Hour).UTC(),
	}

	assert.False(t, d.HasSecondarySecret(), "secondary should not be active after expiration")
}

func TestHasSecondarySecret_NoSecondarySecret(t *testing.T) {
	d := &Destination{
		PrimarySecret:   "primary-secret",
		SecondarySecret: "",
	}

	assert.False(t, d.HasSecondarySecret(), "no secondary secret configured")
}

func TestHasSecondarySecret_ZeroRotationTime(t *testing.T) {
	d := &Destination{
		PrimarySecret:     "primary-secret",
		SecondarySecret:   "secondary-secret",
		RotationExpiresAt: time.Time{},
	}

	assert.False(t, d.HasSecondarySecret(), "zero rotation time means no active rotation")
}

func TestIsDisabled_Active(t *testing.T) {
	d := &Destination{Status: DestinationActive}
	assert.False(t, d.IsDisabled())
}

func TestIsDisabled_Disabled(t *testing.T) {
	d := &Destination{Status: DestinationDisabled}
	assert.True(t, d.IsDisabled())
}

func TestDestinationStatus_Constants(t *testing.T) {
	assert.Equal(t, DestinationStatus("active"), DestinationActive)
	assert.Equal(t, DestinationStatus("disabled"), DestinationDisabled)
}
