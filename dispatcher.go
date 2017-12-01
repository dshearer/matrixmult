package  main

import "context"
import "gopkg.in/Shopify/sarama.v1"
import "log"

func consumeMsg(msg *sarama.ConsumerMessage, producer sarama.AsyncProducer, ctx context.Context) {
    // chop matrix up and send its pieces to different topics
    matrix := StrToMatrix(string(msg.Value))
    for _, submatrixInfo := range gSubmatrixInfos {
        submatrix := matrix.Submatrix(
            submatrixInfo.RowFrom,
            submatrixInfo.RowTo,
            submatrixInfo.ColFrom,
            submatrixInfo.ColTo,
        )
        prodMsg := sarama.ProducerMessage{
            Topic: submatrixInfo.InputTopic,
            Key: nil,
            Value: sarama.StringEncoder(submatrix.String()),
        }
        select {
        case producer.Input() <- &prodMsg:
            break

        case err := <-producer.Errors():
            log.Fatalln("Dispatcher: Producer error: %v", err)

        case <-ctx.Done():
            return
        }
    }
}

func Dispatcher(ctx context.Context) {
    consumer, err := sarama.NewConsumer([]string{gKafkaHost}, nil)
    if err != nil {
        panic(err)
    }
    defer CloseCloser(consumer)
    partitionConsumer, err := consumer.ConsumePartition(
        gInputMatrixesTopic,
        0,
        sarama.OffsetNewest,
    )
    if err != nil {
        panic(err)
    }
    defer CloseCloser(partitionConsumer)

    producer, err := sarama.NewAsyncProducer([]string{gKafkaHost}, nil)
    if err != nil {
        panic(err)
    }
    defer CloseCloser(producer)

    consumed := 0
    errors := 0
    ConsumerLoop:
    for {
        select {
        case msg := <-partitionConsumer.Messages():
            log.Printf("Dispatcher: Consuming message offset %d\n", msg.Offset)
            consumeMsg(msg, producer, ctx)
            log.Printf("Dispatcher: Consumed message offset %d\n", msg.Offset)
            consumed++

        case err := <-partitionConsumer.Errors():
            log.Println("Dispatcher: Failed to consume message", err)
            errors++

        case <-ctx.Done():
            break ConsumerLoop
        }
    }

    log.Printf("Dispatcher: Consumed: %d\n", consumed)
    log.Printf("Dispatcher: Errors: %d\n", errors)
}
