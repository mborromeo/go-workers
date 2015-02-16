package workers

import (
	"fmt"
	"github.com/fzzy/radix/redis"
)

type Fetcher interface {
	Queue() string
	Fetch()
	Acknowledge(*Msg)
	Ready() chan bool
	Messages() chan *Msg
	Close()
	Closed() bool
}

type fetch struct {
	queue    string
	ready    chan bool
	messages chan *Msg
	stop     chan bool
	exit     chan bool
	closed   bool
}

func NewFetch(queue string, messages chan *Msg, ready chan bool) Fetcher {
	return &fetch{
		queue,
		ready,
		messages,
		make(chan bool),
		make(chan bool),
		false,
	}
}

func (f *fetch) Queue() string {
	return f.queue
}

func (f *fetch) processOldMessages() {
	messages := f.inprogressMessages()

	for _, message := range messages {
		<-f.Ready()
		f.sendMessage(message)
	}
}

func (f *fetch) Fetch() {
	messages := make(chan string)

	f.processOldMessages()

	go (func(c chan string) {
		for {
			if f.Closed() {
				break
			}

			<-f.Ready()

			(func() {

				message := Config.Cluster.Cmd("brpoplpush", f.queue, f.inprogressQueue(), 1)

				if message.Type == redis.NilReply {
					// If redis returns null, the queue is empty. Just ignore the error.
				} else {
					c <- message.String()
				}
			})()
		}
	})(messages)

	for {
		select {
		case message := <-messages:
			f.sendMessage(message)
		case <-f.stop:
			f.closed = true
			f.exit <- true
			break
		}
	}
}

func (f *fetch) sendMessage(message string) {
	msg, err := NewMsg(message)

	if err != nil {
		Logger.Println("ERR: Couldn't create message from", message, ":", err)
		return
	}

	f.Messages() <- msg
}

func (f *fetch) Acknowledge(message *Msg) {
	Config.Cluster.Cmd("lrem", f.inprogressQueue(), -1, message.OriginalJson())
}

func (f *fetch) Messages() chan *Msg {
	return f.messages
}

func (f *fetch) Ready() chan bool {
	return f.ready
}

func (f *fetch) Close() {
	f.stop <- true
	<-f.exit
}

func (f *fetch) Closed() bool {
	return f.closed
}

func (f *fetch) inprogressMessages() []string {
	messages, err := Config.Cluster.Cmd("lrange", f.inprogressQueue(), 0, -1).List()

	if err != nil {
		Logger.Println("ERR: ", err)
	}

	return messages
}

func (f *fetch) inprogressQueue() string {
	return fmt.Sprint(f.queue, ":", Config.processId, ":inprogress")
}
