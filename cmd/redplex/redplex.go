package main

import (
	"net"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"

	"github.com/microsoft/redplex"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/sirupsen/logrus"
	kingpin "gopkg.in/alecthomas/kingpin.v2"
)

var (
	address        = kingpin.Flag("listen", "Address to listen on").Short('l').Default("127.0.0.1:3000").String()
	network        = kingpin.Flag("network", "Network to listen on").Short('n').Default("tcp").String()
	remote         = kingpin.Flag("remote", "Remote address of the Redis server").Default("127.0.0.1:6379").String()
	remoteEnv      = kingpin.Flag("remote-env", "Environment variable for --remote").Default("").String()
	passwordEnv    = kingpin.Flag("password-env", "Environment variable for redis password").Default("REDIS_PASSWORD").String()
	sentinelEnv    = kingpin.Flag("sentinel-env", "Environment variable for sentinel password").Default("SENTINEL_PASSWORD").String()
	remoteNetwork  = kingpin.Flag("remote-network", "Remote network to dial through (usually tcp or tcp6)").Default("tcp").String()
	useTLS         = kingpin.Flag("use-tls", "Use TLS to connect to redis").Default("false").Bool()
	sentinels      = kingpin.Flag("sentinels", "A list of Redis sentinel addresses").Strings()
	sentinelMaster = kingpin.Flag("sentinel-name", "The name of the sentinel master").String()
	logLevel       = kingpin.Flag("log-level", "Log level (one of debug, info, warn, error").Default("info").String()
	dialTimeout    = kingpin.Flag("dial-timeout", "Timeout connecting to Redis").Default("10s").Duration()
	writeTimeout   = kingpin.Flag("write-timeout", "Timeout during write operations").Default("2s").Duration()
	pprofServer    = kingpin.Flag("pprof-server", "Address to bind a pprof server on. Not bound if empty.").String()
)

func main() {
	kingpin.UsageTemplate(kingpin.DefaultUsageTemplate)

	kingpin.Parse()
	level, err := logrus.ParseLevel(*logLevel)
	if err != nil {
		logrus.WithError(err).Fatal("redplex/main: error parsing log level")
		os.Exit(1)
	}

	listener, err := net.Listen(*network, *address)
	if err != nil {
		logrus.WithError(err).Fatal("redplex/main: could not listen on the requested address")
		os.Exit(1)
	}

	logrus.SetLevel(level)

	password := os.Getenv(*passwordEnv)
	sentinelPassword := os.Getenv(*sentinelEnv)

	var dialer redplex.Dialer
	if *sentinelMaster != "" {
		dialer = redplex.NewSentinelDialer(*remoteNetwork, *sentinels, *sentinelMaster, password, sentinelPassword, *useTLS, *dialTimeout)
	} else {
		useRemote := *remote
		useRemoveEnv := *remoteEnv
		if useRemoveEnv != "" {
			useRemote = os.Getenv(useRemoveEnv)
		}
		dialer = redplex.NewDirectDialer(*remoteNetwork, useRemote, password, *useTLS, *dialTimeout)
	}

	closed := make(chan struct{})
	go func() {
		awaitInterrupt()
		close(closed)
		listener.Close()
	}()

	go startPprof()

	logrus.Debugf("redplex/main: listening on %s://%s", *network, *address)
	if err := redplex.NewServer(listener, redplex.NewPubsub(dialer, *writeTimeout)).Listen(); err != nil {
		select {
		case <-closed:
			os.Exit(0)
		default:
			logrus.WithError(err).Fatal("redplex/main: error accepting incoming connections")
		}
	}
}

func startPprof() {
	if *pprofServer == "" {
		return
	}

	http.Handle("/stats", promhttp.Handler())

	if err := http.ListenAndServe(*pprofServer, nil); err != nil {
		logrus.WithError(err).Error("redplex/main: could not start pprof server")
	}
}

func awaitInterrupt() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, syscall.SIGINT)
	<-interrupt
}
