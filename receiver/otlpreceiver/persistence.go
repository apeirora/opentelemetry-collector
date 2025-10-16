// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package otlpreceiver // import "go.opentelemetry.io/collector/receiver/otlpreceiver"

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/receiver/otlpreceiver/internal/logs"
)

// PersistenceManager handles message persistence and retry logic
type PersistenceManager struct {
	config PersistenceConfig
	logger *zap.Logger

	storedMessages map[string]PersistedMessage
	storageMutex   sync.RWMutex

	retryWorkerCtx    context.Context
	retryWorkerCancel context.CancelFunc
	retryWorkerWg     sync.WaitGroup

	logsReceiver *logs.Receiver
}

// PersistedMessage represents a message stored in persistence
type PersistedMessage struct {
	ID          string    `json:"id"`
	Data        []byte    `json:"data"`
	ContentType string    `json:"content_type"`
	SignalType  string    `json:"signal_type"`
	CreatedAt   time.Time `json:"created_at"`
	RetryCount  int       `json:"retry_count"`
	_           struct{}
}

// NewPersistenceManager creates a new persistence manager
func NewPersistenceManager(config PersistenceConfig, logger *zap.Logger, logsReceiver *logs.Receiver) *PersistenceManager {
	ctx, cancel := context.WithCancel(context.Background())
	pm := &PersistenceManager{
		config:            config,
		logger:            logger,
		storedMessages:    make(map[string]PersistedMessage),
		retryWorkerCtx:    ctx,
		retryWorkerCancel: cancel,
		logsReceiver:      logsReceiver,
	}

	pm.startRetryWorker()

	return pm
}

// StoreMessage stores a message in persistent storage (only for logs)
func (pm *PersistenceManager) StoreMessage(_ context.Context, data []byte, contentType, signalType string) (string, error) {
	if signalType != "logs" {
		pm.logger.Debug("Skipping persistence for non-log signal type",
			zap.String("signal_type", signalType))
		return "", nil
	}

	messageID := fmt.Sprintf("%s_%d", signalType, time.Now().UnixNano())

	message := PersistedMessage{
		ID:          messageID,
		Data:        data,
		ContentType: contentType,
		SignalType:  signalType,
		CreatedAt:   time.Now(),
		RetryCount:  0,
	}

	key := fmt.Sprintf("otlp_%s_%s", signalType, messageID)

	pm.storageMutex.Lock()
	pm.storedMessages[key] = message
	pm.storageMutex.Unlock()

	pm.logger.Debug("Log message stored for persistence",
		zap.String("message_id", messageID),
		zap.String("signal_type", signalType))

	return messageID, nil
}

// RemoveMessage removes a message from persistent storage
func (pm *PersistenceManager) RemoveMessage(_ context.Context, messageID, signalType string) error {
	key := fmt.Sprintf("otlp_%s_%s", signalType, messageID)

	pm.storageMutex.Lock()
	delete(pm.storedMessages, key)
	pm.storageMutex.Unlock()

	pm.logger.Debug("Message removed from persistence",
		zap.String("message_id", messageID),
		zap.String("signal_type", signalType))

	return nil
}

// ClearStoredMessages clears all stored messages for a specific signal type
func (pm *PersistenceManager) ClearStoredMessages(_ context.Context, signalType string) error {
	pm.storageMutex.Lock()
	defer pm.storageMutex.Unlock()

	// Remove all messages for the specified signal type
	keysToDelete := make([]string, 0)
	for key, message := range pm.storedMessages {
		if message.SignalType == signalType {
			keysToDelete = append(keysToDelete, key)
		}
	}

	// Delete the messages
	for _, key := range keysToDelete {
		delete(pm.storedMessages, key)
	}

	pm.logger.Info("Cleared stored messages",
		zap.String("signal_type", signalType),
		zap.Int("cleared_count", len(keysToDelete)))

	return nil
}

// DeleteStoredMessageByContent deletes a stored message by matching content
func (pm *PersistenceManager) DeleteStoredMessageByContent(_ context.Context, content []byte, contentType, signalType string) error {
	pm.storageMutex.Lock()
	defer pm.storageMutex.Unlock()

	// Find and delete the message with matching content
	for key, message := range pm.storedMessages {
		if message.SignalType == signalType &&
			message.ContentType == contentType &&
			len(message.Data) == len(content) {
			// Simple byte comparison for exact match
			match := true
			for i, b := range content {
				if message.Data[i] != b {
					match = false
					break
				}
			}
			if match {
				delete(pm.storedMessages, key)
				pm.logger.Info("Deleted stored message after successful processing",
					zap.String("message_id", message.ID),
					zap.String("signal_type", signalType))
				return nil
			}
		}
	}

	pm.logger.Debug("No matching stored message found to delete",
		zap.String("signal_type", signalType),
		zap.String("content_type", contentType),
		zap.Int("content_length", len(content)))

	return nil
}

// GetStoredMessages retrieves all stored messages for retry
func (pm *PersistenceManager) GetStoredMessages(_ context.Context) ([]PersistedMessage, error) {
	pm.storageMutex.RLock()
	defer pm.storageMutex.RUnlock()

	messages := make([]PersistedMessage, 0, len(pm.storedMessages))
	for _, message := range pm.storedMessages {
		messages = append(messages, message)
	}

	return messages, nil
}

// ProcessWithPersistence wraps an HTTP handler with persistence logic
func (pm *PersistenceManager) ProcessWithPersistence(
	handler func(http.ResponseWriter, *http.Request),
	signalType string,
) func(http.ResponseWriter, *http.Request) {
	return func(resp http.ResponseWriter, req *http.Request) {
		// Read the request body
		body, err := readRequestBody(req)
		if err != nil {
			http.Error(resp, "Failed to read request body", http.StatusBadRequest)
			return
		}

		// Get content type
		contentType := req.Header.Get("Content-Type")
		if contentType == "" {
			contentType = "application/json"
		}

		// Try to process the request
		success := false
		var processingErr error

		// Create a custom response writer to capture the response
		captureWriter := &responseCapture{ResponseWriter: resp}

		// Process the request
		handler(captureWriter, req)

		// Check if the request was successful (status code 200)
		if captureWriter.statusCode == http.StatusOK {
			success = true
		} else {
			processingErr = fmt.Errorf("request failed with status %d", captureWriter.statusCode)
		}

		if success {
			pm.logger.Debug("Request processed successfully",
				zap.String("signal_type", signalType))
		} else {
			messageID, err := pm.StoreMessage(req.Context(), body, contentType, signalType)
			if err != nil {
				pm.logger.Error("Failed to store message for retry",
					zap.String("signal_type", signalType),
					zap.Error(err))
			} else {
				pm.logger.Info("Message stored for retry",
					zap.String("message_id", messageID),
					zap.String("signal_type", signalType),
					zap.Error(processingErr))
			}
		}
	}
}

// responseCapture captures the response for analysis
type responseCapture struct {
	http.ResponseWriter
	statusCode int
}

func (rc *responseCapture) WriteHeader(code int) {
	rc.statusCode = code
	rc.ResponseWriter.WriteHeader(code)
}

// startRetryWorker starts the background retry worker
func (pm *PersistenceManager) startRetryWorker() {
	pm.retryWorkerWg.Add(1)
	go func() {
		defer pm.retryWorkerWg.Done()
		ticker := time.NewTicker(pm.config.RetryInterval)
		defer ticker.Stop()

		for {
			select {
			case <-pm.retryWorkerCtx.Done():
				pm.logger.Info("Retry worker shutting down")
				return
			case <-ticker.C:
				pm.processRetries(pm.retryWorkerCtx)
			}
		}
	}()
}

func (pm *PersistenceManager) processRetries(ctx context.Context) {
	pm.logger.Debug("Processing queued messages")

	messages, err := pm.GetStoredMessages(ctx)
	if err != nil {
		pm.logger.Error("Failed to get stored messages", zap.Error(err))
		return
	}

	now := time.Now()
	minAge := 10 * time.Millisecond

	logMessages := make([]PersistedMessage, 0)
	skippedNew := 0
	for _, message := range messages {
		if message.SignalType == "logs" {
			messageAge := now.Sub(message.CreatedAt)
			if messageAge < minAge {
				skippedNew++
				pm.logger.Debug("Skipping message - too new (ensuring write completion)",
					zap.String("message_id", message.ID),
					zap.Duration("age", messageAge))
				continue
			}
			logMessages = append(logMessages, message)
		}
	}

	if len(logMessages) == 0 {
		if skippedNew > 0 {
			pm.logger.Debug("No messages to process (waiting for write completion)",
				zap.Int("skipped_new", skippedNew))
		} else {
			pm.logger.Debug("Queue is empty - no messages to process")
		}
		return
	}

	pm.logger.Info("Processing queued messages",
		zap.Int("count", len(logMessages)),
		zap.Int("pending_writes", skippedNew))

	for _, message := range logMessages {
		pm.processQueuedMessage(ctx, message)
	}
}

func (pm *PersistenceManager) processQueuedMessage(ctx context.Context, message PersistedMessage) {
	if message.SignalType != "logs" {
		pm.logger.Debug("Skipping non-log message",
			zap.String("signal_type", message.SignalType))
		return
	}

	isRetry := message.RetryCount > 0
	messageAge := time.Since(message.CreatedAt)

	if isRetry {
		pm.logger.Info("Processing retry attempt",
			zap.String("message_id", message.ID),
			zap.Int("retry_count", message.RetryCount),
			zap.Duration("message_age", messageAge))
	} else {
		pm.logger.Debug("Processing queued message",
			zap.String("message_id", message.ID),
			zap.Duration("queue_time", messageAge))
	}

	success := pm.processStoredLogMessage(ctx, message)

	if success {
		if isRetry {
			pm.logger.Info("Retry successful, removing from queue",
				zap.String("message_id", message.ID),
				zap.Int("retry_count", message.RetryCount),
				zap.Duration("total_time", messageAge))
		} else {
			pm.logger.Debug("Message processed successfully, removing from queue",
				zap.String("message_id", message.ID),
				zap.Duration("queue_time", messageAge))
		}

		if err := pm.RemoveMessage(ctx, message.ID, message.SignalType); err != nil {
			pm.logger.Error("Failed to remove message after successful processing",
				zap.String("message_id", message.ID),
				zap.Error(err))
		}
	} else {
		message.RetryCount++
		key := fmt.Sprintf("otlp_%s_%s", message.SignalType, message.ID)

		pm.storageMutex.Lock()
		pm.storedMessages[key] = message
		pm.storageMutex.Unlock()

		pm.logger.Warn("Message processing failed, will retry",
			zap.String("message_id", message.ID),
			zap.Int("retry_count", message.RetryCount),
			zap.Duration("next_retry_in", pm.config.RetryInterval))
	}
}

// processStoredLogMessage processes a stored log message
func (pm *PersistenceManager) processStoredLogMessage(ctx context.Context, message PersistedMessage) bool {
	pm.logger.Debug("Processing stored log message",
		zap.String("message_id", message.ID),
		zap.String("content_type", message.ContentType),
		zap.Int("data_size", len(message.Data)))

	if pm.logsReceiver == nil {
		pm.logger.Error("No logs receiver available for processing stored message",
			zap.String("message_id", message.ID))
		return false
	}

	enc := getEncoderForContentType(message.ContentType)
	if enc == nil {
		pm.logger.Error("Unknown content type for stored message",
			zap.String("message_id", message.ID),
			zap.String("content_type", message.ContentType))
		return false
	}

	otlpReq, err := enc.unmarshalLogsRequest(message.Data)
	if err != nil {
		pm.logger.Error("Failed to unmarshal stored message",
			zap.String("message_id", message.ID),
			zap.Error(err))
		return false
	}

	otlpResp, err := pm.logsReceiver.Export(ctx, otlpReq)
	if err != nil {
		pm.logger.Error("Failed to process stored log message",
			zap.String("message_id", message.ID),
			zap.Error(err))
		return false
	}

	_, err = enc.marshalLogsResponse(otlpResp)
	if err != nil {
		pm.logger.Error("Failed to marshal response for stored message",
			zap.String("message_id", message.ID),
			zap.Error(err))
		return false
	}

	pm.logger.Info("Successfully processed stored log message",
		zap.String("message_id", message.ID),
		zap.String("content_type", message.ContentType))

	return true
}

// Shutdown stops the persistence manager
func (pm *PersistenceManager) Shutdown(ctx context.Context) error {
	pm.logger.Info("Shutting down persistence manager")

	pm.storageMutex.RLock()
	queueSize := len(pm.storedMessages)
	pm.storageMutex.RUnlock()

	if queueSize > 0 {
		pm.logger.Info("Processing remaining messages before shutdown",
			zap.Int("queue_size", queueSize))

		maxAttempts := 100
		for i := 0; i < maxAttempts; i++ {
			pm.processRetries(ctx)

			pm.storageMutex.RLock()
			remaining := len(pm.storedMessages)
			pm.storageMutex.RUnlock()

			if remaining == 0 {
				pm.logger.Info("All messages processed successfully before shutdown")
				break
			}

			pm.logger.Info("Still processing messages",
				zap.Int("remaining", remaining),
				zap.Int("attempt", i+1))

			select {
			case <-ctx.Done():
				pm.logger.Warn("Shutdown timeout - stopping with messages remaining",
					zap.Int("remaining", remaining))
				goto shutdown
			case <-time.After(50 * time.Millisecond):
			}
		}
	}

shutdown:
	pm.retryWorkerCancel()

	done := make(chan struct{})
	go func() {
		pm.retryWorkerWg.Wait()
		close(done)
	}()

	select {
	case <-done:
		pm.logger.Info("Retry worker stopped gracefully")
	case <-ctx.Done():
		pm.logger.Warn("Retry worker shutdown timed out")
	}

	pm.storageMutex.RLock()
	finalCount := len(pm.storedMessages)
	pm.storageMutex.RUnlock()

	if finalCount > 0 {
		pm.logger.Warn("Shutdown complete with unprocessed messages",
			zap.Int("unprocessed_count", finalCount))
	} else {
		pm.logger.Info("Shutdown complete - all messages processed")
	}

	return nil
}

// readRequestBody reads the request body
func readRequestBody(req *http.Request) ([]byte, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read request body: %w", err)
	}

	if err := req.Body.Close(); err != nil {
		return nil, fmt.Errorf("failed to close request body: %w", err)
	}

	return body, nil
}

// getEncoderForContentType returns the appropriate encoder for the content type
func getEncoderForContentType(contentType string) encoder {
	switch contentType {
	case "application/x-protobuf":
		return pbEncoder
	case "application/json":
		return jsEncoder
	default:
		return nil
	}
}
