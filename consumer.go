package main

import "context"
import "gopkg.in/Shopify/sarama.v1"
import "log"
import "os"

func Consumer(outputPath string, ctx context.Context) {
    // make consumer
    consumer, err := sarama.NewConsumer([]string{gKafkaHost}, nil)
    if err != nil {
        panic(err)
    }
    defer CloseCloser(consumer)
    partConsumer, err := consumer.ConsumePartition(
        gOutputMatrixesTopic,
        0,
        sarama.OffsetNewest,
    )
    if err != nil {
        panic(err)
    }
    defer CloseCloser(partConsumer)

    // open output file
    f, err := os.Create(outputPath)
    if err != nil {
        panic(err)
    }
    defer f.Close()

    // read result matrixes
    consumed := 0
    ConsumeLoop:
    for {
        select {
        case msg := <-partConsumer.Messages():
            log.Println("Consumer: got matrix")
            _, err = f.Write(msg.Value)
            if err != nil {
                panic(err)
            }
            _, err = f.WriteString("\n\n")
            if err != nil {
                panic(err)
            }
            consumed++

        case err := <-partConsumer.Errors():
            log.Fatalln("Consumer: consumer error: %v", err)
            break ConsumeLoop

        case <-ctx.Done():
            break ConsumeLoop
        }
    }

    log.Printf("Consumer: consumed %v\n", consumed)
}
