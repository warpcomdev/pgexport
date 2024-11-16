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
	ts      int64
	samples []sample
}

// GaugeBatch is a vector of gauges with a common timestamp
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
	ts         *int64
	label      []*dto.LabelPair
	gauge      *dto.Gauge
}

// Desc implements Metric
func (g gaugeProxy) Desc() *prometheus.Desc {
	return g.descriptor
}

// Write implements Metric
func (g gaugeProxy) Write(m *dto.Metric) error {
	m.TimestampMs = g.ts
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
				ts:         &snap.ts,
				label:      lp[metric_idx*scale : (metric_idx+1)*scale],
				gauge:      &gauges[metric_idx],
			}
			ch <- mp
		}
	}
}

// Begin a new batch
func (c *GaugeBatch) Begin(ts time.Time) {
	c.current.ts = ts.UnixMilli()
	c.current.samples = make([]sample, 0, 16)
}

// Commit the current batch
func (c *GaugeBatch) Commit() {
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
