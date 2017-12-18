package util

import (
	"fmt"
	"runtime"

	"github.com/tiglabs/raft/logger"
)

func HandleCrash(handlers ...func(interface{})) {
	if r := recover(); r != nil {
		logPanic(r)
		for _, fn := range handlers {
			fn(r)
		}
	}
}

func logPanic(r interface{}) {
	callers := ""
	for i := 0; true; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		callers = callers + fmt.Sprintf("%v:%v\n", file, line)
	}
	logger.Error("Recovered from panic: %#v (%v)\n%v", r, r, callers)
}

func RunWorker(f func(), handlers ...func(interface{})) {
	go func() {
		defer HandleCrash(handlers...)

		f()
	}()
}

func RunWorkerUtilStop(f func(), stopCh <-chan struct{}, handlers ...func(interface{})) {
	go func() {
		for {
			select {
			case <-stopCh:
				return

			default:
				func() {
					defer HandleCrash(handlers...)
					f()
				}()
			}
		}
	}()
}
