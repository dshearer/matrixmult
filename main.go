package main

import "context"
import "os"
import "os/signal"
import "sync"

func main() {
    ctx, cancel := context.WithCancel(context.Background())
    var waitGroup sync.WaitGroup

    signals := make(chan os.Signal, 1)
    signal.Notify(signals, os.Interrupt)
    go func() {
        <-signals
        cancel()
    }()

    waitGroup.Add(1)
    go func() {
        defer waitGroup.Done()
        Producer("input", ctx)
    }()
    waitGroup.Add(1)
    go func() {
        defer waitGroup.Done()
        Dispatcher(ctx)
    }()
    waitGroup.Add(1)
    go func() {
        defer waitGroup.Done()
        Multiplier([]SubmatrixId{11, 21}, ctx)
    }()
    waitGroup.Add(1)
    go func() {
        defer waitGroup.Done()
        Multiplier([]SubmatrixId{12, 22}, ctx)
    }()
    waitGroup.Add(1)
    go func() {
        defer waitGroup.Done()
        Composer(ctx)
    }()
    waitGroup.Add(1)
    go func() {
        defer waitGroup.Done()
        Consumer("output", ctx)
    }()

    waitGroup.Wait()
    os.Exit(0)
}
