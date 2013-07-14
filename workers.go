package workers

import (
	"log"
	"os"
	"sync"
)

var control = make(map[string]chan string)
var processing sync.WaitGroup
var managers = make(map[string]*manager)

var logger = log.New(os.Stdout, "background: ", log.Ldate|log.Lmicroseconds)

func Process(queue string, job workerJob, concurrency int) {
	logger.Println("starting processing queue: ", queue)
	processing.Add(1)

	control[queue] = make(chan string)

	managers[queue] = newManager(queue, job, concurrency)

	go managers[queue].manage(control[queue])
}

func Run() {
	processing.Wait()
}

func PrepareForExit() {
	for _, c := range control {
		c <- "prepare"
	}
}

func Quit() {
	for _, c := range control {
		c <- "quit"
	}
}