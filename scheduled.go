package workers

import (
	"strings"
	"time"
)

const (
	POLL_INTERVAL = 15
)

type scheduled struct {
	keys   []string
	closed bool
	exit   chan bool
}

func (s *scheduled) start() {
	go s.poll(true)
}

func (s *scheduled) quit() {
	s.closed = true
}

func (s *scheduled) poll(continuing bool) {
	if s.closed {
		return
	}

	now := time.Now().Unix()

	for _, key := range s.keys {
		key = Config.Namespace + key
		for {
			messages, _ := Config.Cluster.Cmd("zrangebyscore", key, "-inf", now, "limit", 0, 1).List()

			if len(messages) == 0 {
				break
			}

			message, _ := NewMsg(messages[0])

			if removed, _ := Config.Cluster.Cmd("zrem", key, messages[0]).Bool(); removed {
				queue, _ := message.Get("queue").String()
				queue = strings.TrimPrefix(queue, Config.Namespace)
				Config.Cluster.Cmd("lpush", Config.Namespace+"queue:"+queue, message.ToJson())
			}
		}
	}

	if continuing {
		time.Sleep(POLL_INTERVAL * time.Second)
		s.poll(true)
	}
}

func newScheduled(keys ...string) *scheduled {
	return &scheduled{keys, false, make(chan bool)}
}
