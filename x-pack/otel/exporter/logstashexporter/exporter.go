// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package logstashexporter

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/elastic/beats/v7/libbeat/outputs"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/elastic/beats/v7/libbeat/otelbeat/otelctx"
	"github.com/elastic/beats/v7/libbeat/outputs/logstash"
	"github.com/elastic/beats/v7/x-pack/otel/exporter/logstashexporter/internal"
	"github.com/elastic/elastic-agent-libs/config"
	"github.com/elastic/elastic-agent-libs/logp"
	"github.com/elastic/elastic-agent-libs/transport"
)

const defaultDeadlockTimeout = 5 * time.Minute

type logstashExporter struct {
	config          *logstashOutputConfig
	rawConfig       *config.C
	logger          *logp.Logger
	workers         map[string][]internal.Worker
	workQueue       chan *internal.Work
	settings        exporter.Settings
	deadlockTimeout time.Duration
	observer        outputs.Observer
	mu              sync.RWMutex
}

func newLogstashExporter(settings exporter.Settings, cfg component.Config) (*logstashExporter, error) {
	rawConfig, logstashConfig, err := parseLogstashConfig(&cfg)
	if err != nil {
		return nil, err
	}

	logger, err := logp.ConfigureWithCoreLocal(logp.Config{}, settings.Logger.Core())
	if err != nil {
		return nil, err
	}

	return &logstashExporter{
		config:          logstashConfig,
		rawConfig:       rawConfig,
		logger:          logger,
		workers:         make(map[string][]internal.Worker, 1),
		workQueue:       make(chan *internal.Work, runtime.NumCPU()), // otelconsumer workers
		settings:        settings,
		deadlockTimeout: defaultDeadlockTimeout,
		observer:        outputs.NewNilObserver(),
	}, nil
}

func (*logstashExporter) Start(context.Context, component.Host) error {
	return nil
}

func (l *logstashExporter) Shutdown(context.Context) error {
	l.shutdownLogstashWorkers()
	return nil
}

func (l *logstashExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{MutatesData: false}
}

func (l *logstashExporter) ConsumeLogs(ctx context.Context, ld plog.Logs) error {
	err := l.makeLogstashWorkers(ctx)
	if err != nil {
		return err
	}

	batch, err := internal.NewLogBatch(ctx, ld)
	if err != nil {
		return err
	}

	work := internal.NewWork(batch)

	select {
	case l.workQueue <- work:
	case <-ctx.Done():
		return consumererror.NewLogs(l.handleContextDone(ctx), ld)
	}

	return l.processWorkResult(ctx, ld, work, batch)
}

func (l *logstashExporter) processWorkResult(
	ctx context.Context,
	ld plog.Logs,
	work *internal.Work,
	batch *internal.LogBatch,
) error {
	for {
		select {
		case <-ctx.Done():
			return consumererror.NewLogs(l.handleContextDone(ctx), ld)
		case workRes := <-work.Result():
			done, res := l.processBatchResult(ctx, workRes, ld, batch, work)
			if done {
				return res
			}
		case <-time.After(l.deadlockTimeout):
			// See logstash.deadlockListener for reasoning behind this log.
			l.logger.Infof("Logstash worker hasn't complete processing in the last %v.", l.deadlockTimeout)
			// TODO: should we react to this somehow?
		}
	}
}

func (l *logstashExporter) processBatchResult(
	ctx context.Context,
	workRes error,
	ld plog.Logs,
	batch *internal.LogBatch,
	work *internal.Work,
) (bool, error) {
	for {
		select {
		case <-ctx.Done():
			return true, l.handleContextDone(ctx)

		case batchRes := <-batch.Result():
			return l.handleBatchResult(batchRes, workRes, ld, batch, work)

		case <-time.After(l.deadlockTimeout):
			// See logstash.deadlockListener for reasoning behind this log.
			l.logger.Infof("Logstash batch hasn't complete processing in the last %v.", l.deadlockTimeout)
			// TODO: should we react to this somehow?
		}
	}
}

func (l *logstashExporter) handleBatchResult(
	batchRes internal.LogBatchResult,
	workRes error,
	ld plog.Logs,
	batch *internal.LogBatch,
	work *internal.Work,
) (bool, error) {
	switch batchRes {
	case internal.LogBatchResultACK:
		// Batch was acknowledged, processing complete
		return true, nil

	case internal.LogBatchResultDrop:
		// Batch was explicitly dropped, report permanent error
		return true, consumererror.NewPermanent(fmt.Errorf("batch was dropped: %w", workRes))

	case internal.LogBatchResultCancelled:
		// Batch was cancelled, requeue work
		l.workQueue <- work
		return false, nil

	case internal.LogBatchResultRetry:
		return l.handleRetry(workRes, ld, batch, work)

	default:
		return true, consumererror.NewPermanent(fmt.Errorf("unexpected batch result: %v", batchRes))
	}
}

func (l *logstashExporter) handleRetry(
	workRes error,
	ld plog.Logs,
	batch *internal.LogBatch,
	work *internal.Work,
) (bool, error) {
	// Connection errors don't count against retry limit as they aren't detected by the
	// workers. Clients might close the connection for different reasons, and workers
	// don't have access to the internal client's state to properly determine it.
	if workRes != nil && errors.Is(workRes, transport.ErrNotConnected) {
		batch.AddRetry(-1)
		l.workQueue <- work
		return false, nil
	}

	//nolint:gosec //G115: MaxRetries is positive.
	if l.config.MaxRetries > 0 && batch.NumRetries() >= uint64(l.config.MaxRetries) {
		return true, consumererror.NewLogs(
			fmt.Errorf("max number of retries exceeded: %d", l.config.MaxRetries),
			ld,
		)
	}

	l.logger.Infof("Attempt %d of %d to publish events", batch.NumRetries()+1, l.config.MaxRetries)
	l.workQueue <- work
	return false, nil
}

func (l *logstashExporter) handleContextDone(cxt context.Context) error {
	l.shutdownLogstashWorkers()
	return cxt.Err()
}

func (l *logstashExporter) makeLogstashWorkers(ctx context.Context) error {
	beatVersion := otelctx.GetBeatVersion(ctx)
	if l.hasWorkersFor(beatVersion) {
		return nil
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// Re-check after acquiring write lock
	if l.workers[beatVersion] != nil {
		return nil
	}

	beatIndexPrefix := otelctx.GetBeatIndexPrefix(ctx)
	group, err := logstash.MakeLogstashClients(beatVersion, l.logger, l.observer, l.rawConfig, beatIndexPrefix)
	if err != nil {
		return err
	}

	workers := make([]internal.Worker, 0, len(group.Clients))
	for _, cli := range group.Clients {
		workers = append(workers, internal.MakeClientWorker(l.workQueue, cli, *l.logger))
	}

	l.workers[beatVersion] = workers
	return nil
}

func (l *logstashExporter) hasWorkersFor(beatVersion string) bool {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.workers[beatVersion] != nil
}

func (l *logstashExporter) shutdownLogstashWorkers() {
	l.mu.Lock()
	closingWorkers := l.workers
	l.workers = make(map[string][]internal.Worker, 1)
	l.mu.Unlock()

	for _, c := range closingWorkers {
		for _, v := range c {
			err := v.Close()
			if err != nil {
				l.logger.Errorf("Failed to close logstash client: %v", err)
			}
		}
	}
}
