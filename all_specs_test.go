package workers

import (
	"github.com/fzzy/radix/redis"
	"github.com/customerio/gospec"
	"testing"
	"strconv"
)

// You will need to list every spec in a TestXxx method like this,
// so that gotest can be used to run the specs. Later GoSpec might
// get its own command line tool similar to gotest, but for now this
// is the way to go. This shouldn't require too much typing, because
// there will be typically only one top-level spec per class/feature.

func TestAllSpecs(t *testing.T) {
	r := gospec.NewRunner()

	r.Parallel = false

	r.BeforeEach = func() {
		Configure(map[string]string{
			"server":   "localhost:7000",
			"process":  "1",
			"database": "15",
			"pool":     "1",
		})

		for port := 7000; port <= 7002; port++ {
			c, _ := redis.Dial("tcp", "localhost:"+strconv.Itoa(port))
			c.Cmd("flushdb")
			c.Close()
        }
	}

	// List all specs here
	r.AddSpec(WorkersSpec)
	r.AddSpec(ConfigSpec)
	r.AddSpec(MsgSpec)
	r.AddSpec(FetchSpec)
	r.AddSpec(WorkerSpec)
	r.AddSpec(ManagerSpec)
	r.AddSpec(ScheduledSpec)
	r.AddSpec(EnqueueSpec)
	r.AddSpec(MiddlewareSpec)
	r.AddSpec(MiddlewareRetrySpec)
	r.AddSpec(MiddlewareStatsSpec)

	// Run GoSpec and report any errors to gotest's `testing.T` instance
	gospec.MainGoTest(r, t)
}
