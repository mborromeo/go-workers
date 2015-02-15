package workers

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"time"
)

func ScheduledSpec(c gospec.Context) {
	scheduled := newScheduled(RETRY_KEY)

	was := Config.Namespace
	Config.Namespace = "prod:"

	c.Specify("empties retry queues up to the current time", func() {
		now := time.Now().Unix()

		message1, _ := NewMsg("{\"queue\":\"default\",\"foo\":\"bar1\"}")
		message2, _ := NewMsg("{\"queue\":\"myqueue\",\"foo\":\"bar2\"}")
		message3, _ := NewMsg("{\"queue\":\"default\",\"foo\":\"bar3\"}")

		Config.Cluster.Cmd("zadd", "prod:"+RETRY_KEY, now-60, message1.ToJson())
		Config.Cluster.Cmd("zadd", "prod:"+RETRY_KEY, now-10, message2.ToJson())
		Config.Cluster.Cmd("zadd", "prod:"+RETRY_KEY, now+60, message3.ToJson())

		scheduled.poll(false)

		defaultCount, _ := Config.Cluster.Cmd("llen", "prod:queue:default").Int()
		myqueueCount, _ := Config.Cluster.Cmd("llen", "prod:queue:myqueue").Int()
		pending, _ := Config.Cluster.Cmd("zcard", "prod:"+RETRY_KEY).Int()

		c.Expect(defaultCount, Equals, 1)
		c.Expect(myqueueCount, Equals, 1)
		c.Expect(pending, Equals, 1)
	})

	Config.Namespace = was
}
