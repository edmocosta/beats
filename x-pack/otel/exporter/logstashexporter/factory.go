// Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
// or more contributor license agreements. Licensed under the Elastic License;
// you may not use this file except in compliance with the Elastic License.

package logstashexporter

import (
	"context"
	"time"

	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/outputs/logstash"
	"github.com/elastic/beats/v7/x-pack/otel/exporter/logstashexporter/internal"
	"github.com/elastic/elastic-agent-libs/config"
	"github.com/elastic/elastic-agent-libs/logp"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/consumer"
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
	}

	qs := exporterhelper.NewDefaultQueueConfig()
	qs.Enabled = false // disabled for now for testing, but should be enabled in the future
	qs.Batch = configoptional.Some(exporterhelper.BatchConfig{
		FlushTimeout: 30 * time.Second,
		MinSize:      0,
		MaxSize:      int64(lsCfg.BulkMaxSize),
	})

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
		case err := <-resultChan:
			if err != nil {
				return err
			}
			res := batch.Result()
			if res.Cancelled {
				l.workerChan <- work
			} else if res.Retry {
				l.workerChan <- work
			} else {
				return err
			}
		}
	}
}

func (l *logstashExporter) makeLogstashWorkers(ctx context.Context) error {
	beatVersion := internal.BeatVersion(ctx)
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
