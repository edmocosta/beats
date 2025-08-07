// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package internal

import (
	"context"

	"github.com/elastic/beats/v7/libbeat/publisher"
	"go.opentelemetry.io/collector/pdata/plog"
)

type LogBatchResult struct {
	Acked       bool
	Dropped     bool
	Retry       bool
	RetryEvents []publisher.Event
	SplitRetry  bool
	Cancelled   bool
}

type LogBatch struct {
	logs   plog.Logs
	events []publisher.Event
	result *LogBatchResult
}

func NewLogBatch(ctx context.Context, logs plog.Logs) (*LogBatch, error) {
	events, err := createEvents(ctx, &logs)
	if err != nil {
		return nil, err
	}
	return &LogBatch{
		logs:   logs,
		events: events,
		result: &LogBatchResult{},
	}, nil
}

func createEvents(ctx context.Context, logs *plog.Logs) ([]publisher.Event, error) {
	var events []publisher.Event
	for _, rl := range logs.ResourceLogs().All() {
		for _, sl := range rl.ScopeLogs().All() {
			for _, lr := range sl.LogRecords().All() {
				record, err := FromLogRecord(ctx, &lr)
				if err != nil {
					return nil, err
				}
				events = append(events, publisher.Event{Content: record})
			}
		}
	}
	return events, nil
}

func (b *LogBatch) Events() []publisher.Event {
	return b.events
}

func (b *LogBatch) ACK() {
	b.result.Acked = true
}

func (b *LogBatch) Drop() {
	b.result.Dropped = true
	b.result.Retry = false
}

func (b *LogBatch) Retry() {
	b.result.Retry = true
}

func (b *LogBatch) RetryEvents(events []publisher.Event) {
	b.result.RetryEvents = events
	b.result.Retry = true
}

func (b *LogBatch) SplitRetry() bool {
	b.result.SplitRetry = true
	b.result.Retry = false
	return true
}

func (b *LogBatch) Cancelled() {
	b.result.Cancelled = true
	b.result.Retry = false
}

func (b *LogBatch) Result() *LogBatchResult {
	return b.result
}
