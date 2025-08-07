package internal

import (
	"context"
	"fmt"

	"github.com/elastic/beats/v7/libbeat/outputs"
	"github.com/elastic/beats/v7/libbeat/publisher"
	"github.com/elastic/elastic-agent-libs/logp"
)

type Work struct {
	batch      publisher.Batch
	resultChan chan error
}

func NewWork(batch publisher.Batch, result chan error) Work {
	return Work{
		batch:      batch,
		resultChan: result,
	}
}

type worker struct {
	input  chan Work
	cancel func()
}

type OutputWorker interface {
	Close() error
}

type clientWorker struct {
	worker
	client outputs.Client
}

type netClientWorker struct {
	worker
	client outputs.NetworkClient
	logger logp.Logger
}

func MakeClientWorker(qu chan Work, client outputs.Client, logger logp.Logger) OutputWorker {
	ctx, cancel := context.WithCancel(context.Background())
	w := worker{
		input:  qu,
		cancel: cancel,
	}

	var c interface {
		OutputWorker
		run(context.Context)
	}

	if nc, ok := client.(outputs.NetworkClient); ok {
		c = &netClientWorker{
			worker: w,
			client: nc,
			logger: logger,
		}
	} else {
		c = &clientWorker{worker: w, client: client}
	}

	go c.run(ctx)
	return c
}

func (w *worker) close() {
	w.cancel()
}

func (w *clientWorker) Close() error {
	w.worker.close()
	return w.client.Close()
}

func (w *clientWorker) run(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case task := <-w.input:
			if err := w.client.Publish(ctx, task.batch); err != nil {
				task.resultChan <- err
				return
			}
			task.resultChan <- nil
		}
	}
}

func (w *netClientWorker) Close() error {
	w.worker.close()
	return w.client.Close()
}

func (w *netClientWorker) run(ctx context.Context) {
	var (
		connected         = false
		reconnectAttempts = 0
	)

	for {
		select {

		case <-ctx.Done():
			return

		case task := <-w.input:
			// Try to (re)connect so we can publish batch
			if !connected {
				// Return batch to other output workers while we try to (re)connect
				task.batch.Cancelled()
				task.resultChan <- nil

				if reconnectAttempts == 0 {
					w.logger.Infof("Connecting to %v", w.client)
				} else {
					w.logger.Infof("Attempting to reconnect to %v with %d reconnect attempt(s)", w.client, reconnectAttempts)
				}

				err := w.client.Connect(ctx)
				connected = err == nil
				if connected {
					w.logger.Infof("Connection to %v established", w.client)
					reconnectAttempts = 0
				} else {
					w.logger.Errorf("Failed to connect to %v: %v", w.client, err)
					reconnectAttempts++
				}
				continue
			}

			if err := w.publishBatch(ctx, task.batch); err != nil {
				task.resultChan <- err
				connected = false
			} else {
				task.resultChan <- nil
			}
		}
	}
}

func (w *netClientWorker) publishBatch(ctx context.Context, batch publisher.Batch) error {
	err := w.client.Publish(ctx, batch)
	if err != nil {
		err = fmt.Errorf("failed to publish events: %w", err)
		w.logger.Error(err)
		// on error return to connect loop
		return err
	}
	return nil
}
