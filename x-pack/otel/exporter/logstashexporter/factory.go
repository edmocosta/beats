// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package logstashexporter

import (
	"context"
	"errors"
	"time"

	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/logstash"
	"github.com/elastic/beats/v7/x-pack/otel/exporter/logstashexporter/internal"
	"github.com/elastic/elastic-agent-libs/config"
	"github.com/elastic/elastic-agent-libs/logp"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
	"go.opentelemetry.io/collector/pdata/plog"
)

var (
	Type              = component.MustNewType("logstash")
	LogStabilityLevel = component.StabilityLevelBeta
)

type logstashBeatsConfig struct {
	outputs.HostWorkerCfg `config:",inline"`
	logstash.Config       `config:",inline"`
}

func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		Type,
		createDefaultConfig,
		exporter.WithLogs(createLogExporter, LogStabilityLevel),
	)
}

func createDefaultConfig() component.Config {
	return &Config{}
}

type logstashExporter struct {
	config     *logstashBeatsConfig
	logger     *logp.Logger
	workers    map[string][]internal.OutputWorker
	workerChan chan internal.Work
	settings   exporter.Settings
}

func (l *logstashExporter) Shutdown(_ context.Context) error {
	for _, c := range l.workers {
		for _, v := range c {
			_ = v.Close()
		}
	}
	return nil
}

func createLogExporter(ctx context.Context, settings exporter.Settings, cfg component.Config) (exporter.Logs, error) {
	parsedCfg, err := config.NewConfigFrom(&cfg)
	if err != nil {
		return nil, err
	}

	lsCfg := logstashBeatsConfig{}
	err = parsedCfg.Unpack(&lsCfg)
	if err != nil {
		return nil, err
	}

	logger, err := logp.ConfigureWithCoreLocal(logp.Config{}, settings.Logger.Core())
	if err != nil {
		return nil, err
	}

	exp := logstashExporter{
		config:     &lsCfg,
		logger:     logger,
		workers:    map[string][]internal.OutputWorker{},
		workerChan: make(chan internal.Work),
		settings:   settings,
	}

	// 	Beats (sync) <-> OTel batcher (async) <-> logstash exporter (sync)
	qs := exporterhelper.NewDefaultQueueConfig()
	qs.Enabled = false

	return exporterhelper.NewLogs(
		ctx,
		settings,
		cfg,
		exp.pushLogData,
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		exporterhelper.WithShutdown(exp.Shutdown),
		exporterhelper.WithQueueBatch(qs, exporterhelper.NewLogsQueueBatchSettings()),
		exporterhelper.WithTimeout(exporterhelper.TimeoutConfig{Timeout: lsCfg.Timeout}),
	)
}

func (l *logstashExporter) pushLogData(ctx context.Context, ld plog.Logs) error {
	err := l.makeLogstashWorkers(ctx)
	if err != nil {
		return err
	}

	batch, err := internal.NewLogBatch(ctx, ld)
	if err != nil {
		return err
	}

	resultChan := make(chan error, 1)
	work := internal.NewWork(batch, resultChan)
	l.workerChan <- work

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(l.config.Timeout):
			return consumererror.NewLogs(errors.New("timeout"), ld)
		case result := <-resultChan:
			if result != nil {
				// TODO: check if the error is really permanent
				return consumererror.NewLogs(result, ld)
			}
			res := batch.Result()
			switch {
			case res.Acked:
				return nil
			case res.Dropped:
				return consumererror.NewPermanent(result)
			case res.Cancelled:
				l.workerChan <- work
			case res.Retry:
				if l.config.MaxRetries > 0 && res.Retries >= l.config.MaxRetries {
					return consumererror.NewPermanent(errors.New("max retries exceeded"))
				}
				l.workerChan <- work
			case res.Split:
			// TODO: implement split logic if needed. Logstash clients does not call SplitRetry currently
			default:
				return consumererror.NewPermanent(result)
			}
		}
	}
}

func (l *logstashExporter) makeLogstashWorkers(ctx context.Context) error {
	beatVersion := internal.GetBeatVersion(ctx)
	if _, ok := l.workers[beatVersion]; ok {
		return nil
	}

	hostWorkerConfig, err := config.NewConfigFrom(l.config.HostWorkerCfg)
	if err != nil {
		return err
	}

	hosts, err := outputs.ReadHostList(hostWorkerConfig)
	if err != nil {
		return err
	}

	group, err := logstash.MakeLogstashClients(beatVersion, l.logger, &l.config.Config, hosts, outputs.NewNilObserver())
	if err != nil {
		return err
	}
	workers := make([]internal.OutputWorker, 0, len(group.Clients))
	for _, cli := range group.Clients {
		workers = append(workers, internal.MakeClientWorker(l.workerChan, cli, *l.logger))
	}

	l.workers[beatVersion] = workers
	return nil
}
