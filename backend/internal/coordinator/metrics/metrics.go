package metrics 

import "github.com/prometheus/client_golang/prometheus"

type Metrics struct {
	lockAcquisitions prometheus.Counter
	lockReleases     prometheus.Counter
	lockWaitTime     prometheus.Histogram
	activeSessions   prometheus.Gauge
	activeLocks      prometheus.Gauge
}

func (m *Metrics) RecordLockAcquisred(lockName string) {
	m.lockAcquisitions.Inc()
	// Additional labels can be added if needed
	m.activeLocks.Inc()
}
