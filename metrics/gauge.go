package metrics

import (
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// Sample represents a single metric in the batch
type sample struct {
	labelValues []string
	value       float64
}

// Batch of metrics identified by the same timestamp
type batch struct {
	timestamp int64
	samples   []sample
}

// GaugeBatch is a set of gauges that we want to treat as a batch
//
// The default GaugeVec type in prometheus does not track the time
// the gauge was updated, and therefore it does not expose it when collected
// by prometheus.
//
// When a gauge is updated very infrequenty, we might want to expose
// this information to ptometheus, so it can notice that not only the
// gauge has not changed, but in fact it has not had any update since
// the previous collection.
//
// The GaugeBatch tracks the timestamp of the last update, in batches.
// The application is expected to perform a batch of work and update
// all the metrics associated to that batch "at once".
//
// The metrics associated to a batch will expose the timestamp
// when the batch was finished.
//
// This collector is simplified to workwith the following assumption:
//
//   - The application wil always begin a batch with  `Begin`, before
//     setting any value in the gauge.
//   - Batch will be closed with `Commit`.
//   - Inside a batch, a metric is updated at most once. I.E. for a
//     given set of labels, there is a single value in the whole batch.
//   - The values wont be exposed until the batch is finished.
type GaugeBatch struct {
	lock       sync.Mutex
	labelNames []string
	descriptor *prometheus.Desc
	last       batch
	current    batch
}

// Describe implements prometheus.Collector.
func (c *GaugeBatch) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.descriptor
}

// gaugeProxy implements the Metric interface for prometheus
type gaugeProxy struct {
	descriptor *prometheus.Desc
	timestamp  *int64
	label      []*dto.LabelPair
	gauge      *dto.Gauge
}

// Desc implements Metric
func (g gaugeProxy) Desc() *prometheus.Desc {
	return g.descriptor
}

// Write implements Metric
func (g gaugeProxy) Write(m *dto.Metric) error {
	// Nota: no sé si se supone que esta función debe adquirir el lock del gauge
	m.TimestampMs = g.timestamp
	m.Gauge = g.gauge
	m.Label = g.label
	return nil
}

// Collect implements prometheus.Collector.
func (c *GaugeBatch) Collect(ch chan<- prometheus.Metric) {
	c.lock.Lock()
	snap := c.last
	c.lock.Unlock()
	if snap.samples != nil {
		// La API de dto.Metric es un monton de punteros...
		// para minimizar la reserva de memoria, hago que
		// todos esos punteros apunten dentro de slices
		// creadas con tamaño fijo.
		gauges := make([]dto.Gauge, len(snap.samples))
		scale := len(c.labelNames)
		labels := make([]dto.LabelPair, scale*len(snap.samples))
		lp := make([]*dto.LabelPair, scale*len(snap.samples))
		for metric_idx := range snap.samples {
			// Agrego al elemento actual del slice, los valores correspondientes
			gauges[metric_idx].Value = &snap.samples[metric_idx].value
			for label_idx := range c.labelNames {
				labels[metric_idx*scale+label_idx].Name = &c.labelNames[label_idx]
				labels[metric_idx*scale+label_idx].Value = &snap.samples[metric_idx].labelValues[label_idx]
				lp[metric_idx*scale+label_idx] = &labels[metric_idx*scale+label_idx]
			}
			// Y envío la métrica al canal
			mp := gaugeProxy{
				descriptor: c.descriptor,
				timestamp:  &snap.timestamp,
				label:      lp[metric_idx*scale : (metric_idx+1)*scale],
				gauge:      &gauges[metric_idx],
			}
			ch <- mp
		}
	}
}

// Begin a new batch
func (c *GaugeBatch) Begin() {
	c.current.samples = make([]sample, 0, 16)
}

// Commit the current batch
func (c *GaugeBatch) Commit() {
	c.current.timestamp = time.Now().UnixMilli()
	c.lock.Lock()
	c.last = c.current
	c.current.samples = nil
	c.lock.Unlock()
}

// Set a value in the batch
func (c *GaugeBatch) Set(labelValues []string, value float64) {
	c.current.samples = append(c.current.samples, sample{
		labelValues: labelValues,
		value:       value,
	})
}

// NewGaugeBatch creates a new Gauge Batch collector
func NewGaugeBatch(name string, help string, labels []string) *GaugeBatch {
	gb := &GaugeBatch{
		labelNames: labels,
		descriptor: prometheus.NewDesc(name, help, labels, nil),
	}
	// Comprobar que implementamos la interfaz
	_ = (prometheus.Collector)(gb)
	return gb
}
