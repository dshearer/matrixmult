package main

import "context"
import "gopkg.in/Shopify/sarama.v1"
import "log"
import "sort"
import "time"

func getLastOutputSubmatrix(outputSubmatrixId SubmatrixId, consumer sarama.Consumer, ctx context.Context) (*Matrix, bool) {
    outputSubmatrixInfo := gSubmatrixInfos[outputSubmatrixId]
    var partition int32 = 0

    // get ID of last msg
    hwmMap, ok := consumer.HighWaterMarks()[outputSubmatrixInfo.OutputTopic]
    if !ok {
        return nil, false
    }
    lastId, ok := hwmMap[partition]
    if !ok {
        return nil, false
    }
    log.Printf("ID of last msg in %v: %v\n", outputSubmatrixInfo.OutputTopic, lastId)

    // get last msg
    partitionConsumer, err := consumer.ConsumePartition(
        outputSubmatrixInfo.OutputTopic,
        partition,
        lastId - 1,
    )
    if err != nil {
        panic(err)
    }
    defer CloseCloser(partitionConsumer)
    timerChan := time.After(5 * time.Second)
    select {
    case msg := <-partitionConsumer.Messages():
        return StrToMatrix(string(msg.Value)), false

    case err := <-partitionConsumer.Errors():
        log.Fatalln("Multiplier: Failed to read from %v: %v", outputSubmatrixInfo.OutputTopic, err)
        return nil, true

    case <-timerChan:
        log.Fatalln("Multiplier: Failed to read from %v: timeout", outputSubmatrixInfo.OutputTopic)
        return nil, true

    case <-ctx.Done():
        return nil, true
    }
}

func Multiplier(outputSubmatrixes []SubmatrixId, ctx context.Context) {
    if len(outputSubmatrixes) != 2 {
        panic("Invalid arg")
    }

    // load state
    consumer, err := sarama.NewConsumer([]string{gKafkaHost}, nil)
    if err != nil {
        panic(err)
    }
    defer CloseCloser(consumer)
    state := make(map[SubmatrixId]*Matrix)
    for _, id := range outputSubmatrixes {
        sm, cancelled := getLastOutputSubmatrix(id, consumer, ctx)
        if cancelled {
            return
        }
        if sm == nil {
            state[id] = NewIdMatrix(gMatrixDimen/2)
        } else {
            state[id] = sm
        }
        log.Printf("State for submatrix %v: \n%v\n", id, state[id])
    }

    // make partition consumers for the four input submatrixes
    partConsumers := make(map[SubmatrixId]sarama.PartitionConsumer)
    for id, info := range gSubmatrixInfos {
        partConsumer, err := consumer.ConsumePartition(
            info.InputTopic,
            0,
            sarama.OffsetNewest,
        )
        if err != nil {
            panic(err)
        }
        defer CloseCloser(partConsumer)
        partConsumers[id] = partConsumer
    }

    // make producer for the two output submatrixes
    producer, err := sarama.NewAsyncProducer([]string{gKafkaHost}, nil)
    if err != nil {
        panic(err)
    }
    defer CloseCloser(producer)

    // sort output submatrix IDs
    var outputIds [2]SubmatrixId
    for i, id := range outputSubmatrixes {
        outputIds[i] = id
    }
    sort.Ints(outputIds[:])
    fstOutputSubmatrixId := outputIds[0]
    sndOutputSubmatrixId := outputIds[1]

    // do multiplications
    MultLoop:
    for {
        /*
        We compute this:

            B_x' = A_11*B_x + A_12*B_y
            B_y' = A_21*B_x + A_22*B_y
        */

        // read the four input submatrixes
        inputs := make(map[SubmatrixId]*Matrix)
        inputsToRead := make([]SubmatrixId, 0)
        for id, _ := range gSubmatrixInfos {
            inputsToRead = append(inputsToRead, id)
        }
        for len(inputsToRead) > 0 {
            id := inputsToRead[0]
            partConsumer := partConsumers[id]
            select {
            case msg := <-partConsumer.Messages():
                inputs[id] = StrToMatrix(string(msg.Value))
                log.Printf("Multiplier: Got submatrix %v: %v\n", id, inputs[id])
                inputsToRead = inputsToRead[1:]

            case err := <-partConsumer.Errors():
                log.Fatalln("Multiplier: consumer error: %v", err)
                break MultLoop

            case <-ctx.Done():
                break MultLoop
            }
        }

        // compute B_x' = A_11*B_x + A_12*B_y
        tmp1 := MultiplyMatrixes(inputs[11], state[fstOutputSubmatrixId])
        tmp2 := MultiplyMatrixes(inputs[12], state[sndOutputSubmatrixId])
        state[fstOutputSubmatrixId] = AddMatrixes(tmp1, tmp2)

        // compute B_y' = A_21*B_x + A_22*B_y
        tmp1 = MultiplyMatrixes(inputs[21], state[fstOutputSubmatrixId])
        tmp2 = MultiplyMatrixes(inputs[22], state[sndOutputSubmatrixId])
        state[sndOutputSubmatrixId] = AddMatrixes(tmp1, tmp2)

        log.Printf("State: %v\n", state)

        // publish output
        for _, id := range outputSubmatrixes {
            msg := sarama.ProducerMessage{
                Topic: gSubmatrixInfos[id].OutputTopic,
                Key: nil,
                Value: sarama.StringEncoder(state[id].String()),
            }
            select {
            case producer.Input() <- &msg:
                break

            case err := <-producer.Errors():
                log.Fatalln("Multiplier: producer error: %v", err)
                break MultLoop

            case <-ctx.Done():
                break MultLoop
            }
        }
    }
}
