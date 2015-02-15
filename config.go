package workers

import (
	"github.com/fzzy/radix/extra/cluster"
	"strconv"
	"time"
)

type config struct {
	processId string
	Namespace string
	Cluster   *cluster.Cluster
	Fetch     func(queue string) Fetcher
}

var Config *config

func Configure(options map[string]string) {
	var poolSize int
	var timeout int
	var reset int
	var namespace string

	if options["server"] == "" {
		panic("Configure requires a 'server' option, which identifies a Redis instance")
	}
	if options["process"] == "" {
		panic("Configure requires a 'process' option, which uniquely identifies this instance")
	}
	if options["pool"] == "" {
		options["pool"] = "1"
	}
	if options["namespace"] != "" {
		namespace = options["namespace"] + ":"
	}
	if options["timeout"] == "" {
		options["timeout"] = "240"
	}
	if options["reset"] == "" {
		options["reset"] = "10"
	}

	poolSize, _ = strconv.Atoi(options["pool"])
	timeout, _ = strconv.Atoi(options["timeout"])
	reset, _ = strconv.Atoi(options["reset"])

	clusterOpts := new(cluster.Opts)
	clusterOpts.Addr = options["server"]
	clusterOpts.PoolSize = poolSize
	clusterOpts.Timeout = time.Duration(timeout) * time.Second
	clusterOpts.ResetThrottle = time.Duration(reset) * time.Second

	cluster, err := cluster.NewClusterWithOpts(*clusterOpts)
	if err != nil {
		panic("Cannot connect to a Redis Cluster")
	}

	// TODO: Add support for PASSWORD
	// TODO: Add support for SELECT

	Config = &config{
		options["process"],
		namespace,
		cluster,
		func(queue string) Fetcher {
			return NewFetch(queue, make(chan *Msg), make(chan bool))
		},
	}
}
