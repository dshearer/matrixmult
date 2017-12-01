package main

import "context"
import "gopkg.in/Shopify/sarama.v1"
import "log"

func Composer(ctx context.Context) {
    // make consumer
    consumer, err := sarama.NewConsumer([]string{gKafkaHost}, nil)
    if err != nil {
        panic(err)
    }
    defer CloseCloser(consumer)

    // make partition consumer for each submatrix
    partConsumers := make(map[SubmatrixId]sarama.PartitionConsumer)
    for id, info := range gSubmatrixInfos {
        pc, err := consumer.ConsumePartition(
            info.OutputTopic,
            0,
            sarama.OffsetNewest,
        )
        if err != nil {
            panic(err)
        }
        defer CloseCloser(pc)
        partConsumers[id] = pc
    }

    // make producer for output topic
    producer, err := sarama.NewAsyncProducer([]string{gKafkaHost}, nil)
    if err != nil {
        panic(err)
    }
    defer CloseCloser(producer)

    composed := 0
    ComposeLoop:
    for {
        // read each submatrix
        outputs := make(map[SubmatrixId]*Matrix)
        outputsToRead := make([]SubmatrixId, 0)
        for id, _ := range gSubmatrixInfos {
            outputsToRead = append(outputsToRead, id)
        }
        for len(outputsToRead) > 0 {
            id := outputsToRead[0]
            partConsumer := partConsumers[id]
            select {
            case msg := <-partConsumer.Messages():
                log.Printf("Composer: Got submatrix %v\n", id)
                outputs[id] = StrToMatrix(string(msg.Value))
                outputsToRead = outputsToRead[1:]

            case err := <-partConsumer.Errors():
                log.Fatalln("Multiplier: consumer error: %v", err)
                break ComposeLoop

            case <-ctx.Done():
                break ComposeLoop
            }
        }
        log.Println("Composer: Got all submatrixes!")

        // compose into matrix
        matrix := NewEmptyMatrix(gMatrixDimen, gMatrixDimen)
        for id, info := range gSubmatrixInfos {
            matrix.SetSubmatrix(outputs[id], info.RowFrom, info.ColFrom)
        }

        // publish matrix
        msg := sarama.ProducerMessage{
            Topic: gOutputMatrixesTopic,
            Key: nil,
            Value: sarama.StringEncoder(matrix.String()),
        }
        select {
        case producer.Input() <- &msg:
            log.Println("Composer: published matrix")

        case err := <-producer.Errors():
            log.Fatalln("Composer: producer error: %v", err)
            break ComposeLoop

        case <-ctx.Done():
            break ComposeLoop
        }

        composed++
    }

    log.Printf("Composer: composed %v\n", composed)
}
