// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver

import (
	"context"
	"testing"
	"time"

	"go.uber.org/zap"
)

func TestPersistenceManager(t *testing.T) {
	config := PersistenceConfig{
		Enabled:       true,
		RetryInterval: 1 * time.Second,
	}

	logger := zap.NewNop()
	pm := NewPersistenceManager(config, logger, nil)

	// Test storing a message
	err := pm.StoreMessage(context.Background(), []byte("test data"), "application/json", "traces")
	if err != nil {
		t.Fatalf("Failed to store message: %v", err)
	}

	// Test retrieving stored messages
	messages, err := pm.GetStoredMessages(context.Background())
	if err != nil {
		t.Fatalf("Failed to get stored messages: %v", err)
	}

	if len(messages) != 1 {
		t.Fatalf("Expected 1 message, got %d", len(messages))
	}

	if messages[0].SignalType != "traces" {
		t.Fatalf("Expected signal type 'traces', got '%s'", messages[0].SignalType)
	}

	// Test removing a message
	err = pm.RemoveMessage(context.Background(), messages[0].ID, "traces")
	if err != nil {
		t.Fatalf("Failed to remove message: %v", err)
	}

	// Verify message was removed
	messages, err = pm.GetStoredMessages(context.Background())
	if err != nil {
		t.Fatalf("Failed to get stored messages: %v", err)
	}

	if len(messages) != 0 {
		t.Fatalf("Expected 0 messages after removal, got %d", len(messages))
	}

	// Test shutdown
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	err = pm.Shutdown(ctx)
	if err != nil {
		t.Fatalf("Failed to shutdown persistence manager: %v", err)
	}
}
