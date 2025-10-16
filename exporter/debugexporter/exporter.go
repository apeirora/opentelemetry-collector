// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package debugexporter // import "go.opentelemetry.io/collector/exporter/debugexporter"

import (
	"context"
	"fmt"
	"os"
	"sync"
	"sync/atomic"

	"go.uber.org/zap"

	"go.opentelemetry.io/collector/config/configtelemetry"
	"go.opentelemetry.io/collector/exporter/debugexporter/internal/normal"
	"go.opentelemetry.io/collector/exporter/debugexporter/internal/otlptext"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pprofile"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

var failureCnt atomic.Int32

type debugExporter struct {
	verbosity         configtelemetry.Level
	logger            *zap.Logger
	logsMarshaler     plog.Marshaler
	metricsMarshaler  pmetric.Marshaler
	tracesMarshaler   ptrace.Marshaler
	profilesMarshaler pprofile.Marshaler
	receivedLogs      sync.Map

	totalLogsCount  atomic.Int64
	exportCallCount atomic.Int64
	expectedLogs    int64
	logFile         *os.File
	fileMutex       sync.Mutex
}

func newDebugExporter(logger *zap.Logger, verbosity configtelemetry.Level) *debugExporter {
	var logsMarshaler plog.Marshaler
	var metricsMarshaler pmetric.Marshaler
	var tracesMarshaler ptrace.Marshaler
	var profilesMarshaler pprofile.Marshaler
	if verbosity == configtelemetry.LevelDetailed {
		logsMarshaler = otlptext.NewTextLogsMarshaler()
		metricsMarshaler = otlptext.NewTextMetricsMarshaler()
		tracesMarshaler = otlptext.NewTextTracesMarshaler()
		profilesMarshaler = otlptext.NewTextProfilesMarshaler()
	} else {
		logsMarshaler = normal.NewNormalLogsMarshaler()
		metricsMarshaler = normal.NewNormalMetricsMarshaler()
		tracesMarshaler = normal.NewNormalTracesMarshaler()
		profilesMarshaler = normal.NewNormalProfilesMarshaler()
	}

	logFile, err := os.OpenFile("test-logs.txt", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		logger.Error("Failed to open test-logs.txt for writing", zap.Error(err))
	}

	return &debugExporter{
		verbosity:         verbosity,
		logger:            logger,
		logsMarshaler:     logsMarshaler,
		metricsMarshaler:  metricsMarshaler,
		tracesMarshaler:   tracesMarshaler,
		profilesMarshaler: profilesMarshaler,
		expectedLogs:      10000,
		logFile:           logFile,
	}
}

func (s *debugExporter) pushTraces(_ context.Context, td ptrace.Traces) error {
	s.logger.Info("Traces",
		zap.Int("resource spans", td.ResourceSpans().Len()),
		zap.Int("spans", td.SpanCount()))
	if s.verbosity == configtelemetry.LevelBasic {
		return nil
	}

	buf, err := s.tracesMarshaler.MarshalTraces(td)
	if err != nil {
		return err
	}
	s.logger.Info(string(buf))
	return nil
}

func (s *debugExporter) pushMetrics(_ context.Context, md pmetric.Metrics) error {
	s.logger.Info("Metrics",
		zap.Int("resource metrics", md.ResourceMetrics().Len()),
		zap.Int("metrics", md.MetricCount()),
		zap.Int("data points", md.DataPointCount()))
	if s.verbosity == configtelemetry.LevelBasic {
		return nil
	}

	buf, err := s.metricsMarshaler.MarshalMetrics(md)
	if err != nil {
		return err
	}
	s.logger.Info(string(buf))
	return nil
}

func (s *debugExporter) pushLogs(_ context.Context, ld plog.Logs) error {
	callNumber := s.exportCallCount.Add(1)

	if callNumber%10 == 0 {
		s.logger.Warn("Simulating export failure for 10th call", zap.Int64("call_number", callNumber))
		return fmt.Errorf("simulated export failure on call %d", callNumber)
	}

	logRecordCount := ld.LogRecordCount()

	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		resourceLogs := ld.ResourceLogs().At(i)
		for j := 0; j < resourceLogs.ScopeLogs().Len(); j++ {
			scopeLogs := resourceLogs.ScopeLogs().At(j)
			for k := 0; k < scopeLogs.LogRecords().Len(); k++ {
				logRecord := scopeLogs.LogRecords().At(k)
				if counter, exists := logRecord.Attributes().Get("test.log.counter"); exists {
					s.receivedLogs.Store(counter.Int(), true)
					s.writeLogToFile(logRecord, s.totalLogsCount.Load())
				}
			}
		}
	}

	currentCount := s.totalLogsCount.Add(int64(logRecordCount))
	s.logger.Info("Logs body", zap.String("body", ld.ResourceLogs().At(0).ScopeLogs().At(0).LogRecords().At(0).Body().AsString()))
	s.logger.Info("Logs",
		zap.Int("resource logs", ld.ResourceLogs().Len()),
		zap.Int("log records", logRecordCount),
		zap.Int64("total received", currentCount),
		zap.Int64("currentCount", currentCount),
	)

	if currentCount >= s.expectedLogs {
		uniqueCount := int64(0)
		var missingLogs []int64
		receivedCounters := make(map[int64]bool)

		s.receivedLogs.Range(func(key, value interface{}) bool {
			if counter, ok := key.(int64); ok {
				receivedCounters[counter] = true
				uniqueCount++
			}
			return true
		})

		for i := int64(1); i <= s.expectedLogs; i++ {
			if !receivedCounters[i] {
				missingLogs = append(missingLogs, i)
			}
		}

		successRate := (float64(uniqueCount) / float64(s.expectedLogs)) * 100

		s.logger.Info("SUCCESS RATE REPORT",
			zap.Int64("expected_logs", s.expectedLogs),
			zap.Int64("total_received", currentCount),
			zap.Int64("unique_logs", uniqueCount),
			zap.Int("missing_count", len(missingLogs)),
			zap.Float64("success_rate_percent", successRate))

		s.writeReportToFile(s.expectedLogs, currentCount, uniqueCount, int64(len(missingLogs)), successRate, missingLogs)

		if len(missingLogs) > 0 && len(missingLogs) <= 100 {
			s.logger.Warn("Missing log counters", zap.Int64s("missing", missingLogs))
		} else if len(missingLogs) > 100 {
			s.logger.Warn("Too many missing logs to list",
				zap.Int("missing_count", len(missingLogs)),
				zap.Int64("first_missing", missingLogs[0]),
				zap.Int64("last_missing", missingLogs[len(missingLogs)-1]))
		}
	}

	if s.verbosity == configtelemetry.LevelBasic {
		return nil
	}

	// buf, err := s.logsMarshaler.MarshalLogs(ld)
	// if err != nil {
	// 	return err
	// }
	// s.logger.Info(string(buf))
	return nil
}

func (s *debugExporter) pushProfiles(_ context.Context, pd pprofile.Profiles) error {
	s.logger.Info("Profiles",
		zap.Int("resource profiles", pd.ResourceProfiles().Len()),
		zap.Int("sample records", pd.SampleCount()))

	if s.verbosity == configtelemetry.LevelBasic {
		return nil
	}

	buf, err := s.profilesMarshaler.MarshalProfiles(pd)
	if err != nil {
		return err
	}
	s.logger.Info(string(buf))
	return nil
}

func (s *debugExporter) writeLogToFile(logRecord plog.LogRecord, counter int64) {
	if s.logFile == nil {
		return
	}

	s.fileMutex.Lock()
	defer s.fileMutex.Unlock()

	logLine := fmt.Sprintf("Counter: %d | Body: %s | Severity: %s | Timestamp: %s",
		counter,
		logRecord.Body().AsString(),
		logRecord.SeverityText(),
		logRecord.Timestamp().String())

	var attrs string
	logRecord.Attributes().Range(func(k string, v pcommon.Value) bool {
		attrs += fmt.Sprintf(" | %s: %s", k, v.AsString())
		return true
	})
	logLine += attrs + "\n"

	_, err := s.logFile.WriteString(logLine)
	if err != nil {
		s.logger.Error("Failed to write log to file", zap.Error(err))
	}
}

func (s *debugExporter) writeReportToFile(expectedLogs, totalReceived, uniqueLogs, missingCount int64, successRate float64, missingLogs []int64) {
	if s.logFile == nil {
		return
	}

	s.fileMutex.Lock()
	defer s.fileMutex.Unlock()

	report := fmt.Sprintf("\n"+
		"========================================\n"+
		"          SUCCESS RATE REPORT          \n"+
		"========================================\n"+
		"Expected Logs:        %d\n"+
		"Total Received:       %d\n"+
		"Unique Logs:          %d\n"+
		"Successfully Received: %d\n"+
		"Missing/Failed:       %d\n"+
		"Success Rate:         %.2f%%\n"+
		"========================================\n",
		expectedLogs, totalReceived, uniqueLogs, uniqueLogs, missingCount, successRate)

	if missingCount > 0 && missingCount <= 100 {
		report += fmt.Sprintf("\nMissing Log Counters:\n%v\n", missingLogs)
	} else if missingCount > 100 {
		report += fmt.Sprintf("\nMissing Logs Count: %d\n", missingCount)
		report += fmt.Sprintf("First Missing: %d\n", missingLogs[0])
		report += fmt.Sprintf("Last Missing: %d\n", missingLogs[len(missingLogs)-1])
	}

	report += "========================================\n\n"

	_, err := s.logFile.WriteString(report)
	if err != nil {
		s.logger.Error("Failed to write report to file", zap.Error(err))
	}
}

func (s *debugExporter) Shutdown(_ context.Context) error {
	if s.logFile != nil {
		s.fileMutex.Lock()
		defer s.fileMutex.Unlock()
		if err := s.logFile.Close(); err != nil {
			s.logger.Error("Failed to close log file", zap.Error(err))
			return err
		}
	}
	return nil
}
