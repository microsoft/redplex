package redplex

import "github.com/prometheus/client_golang/prometheus"

var (
	clientsCount = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "redplex_clients",
		Help: "Number of subscribers currently connected through Redplex",
	})
	serverReconnects = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "redplex_reconnects",
		Help: "Number of times this server has reconnected to the remote Redis instance",
	})
	throughputMessages = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "redplex_messages_count",
		Help: "Number messages this instance has sent to subscribers.",
	})
	throughputBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "redplex_bytes_count",
		Help: "Number bytes this instance has sent to subscribers.",
	})
)

func init() {
	prometheus.MustRegister(clientsCount)
	prometheus.MustRegister(serverReconnects)
	prometheus.MustRegister(throughputMessages)
	prometheus.MustRegister(throughputBytes)
}
