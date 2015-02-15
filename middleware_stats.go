package workers

import (
	"time"
)

type MiddlewareStats struct{}

func (l *MiddlewareStats) Call(queue string, message *Msg, next func() bool) (acknowledge bool) {
	defer func() {
		if e := recover(); e != nil {
			incrementStats("failed")
			panic(e)
		}
	}()

	acknowledge = next()

	incrementStats("processed")

	return
}

func incrementStats(metric string) {

	today := time.Now().UTC().Format("2006-01-02")

	r := Config.Cluster.Cmd("incr", Config.Namespace+"stat:"+metric)

	if r.Err != nil {
		Logger.Println("couldn't save stats:", r.Err)
	}

	r = Config.Cluster.Cmd("incr", Config.Namespace+"stat:"+metric+":"+today)

	if r.Err != nil {
		Logger.Println("couldn't save stats:", r.Err)
	}
}
