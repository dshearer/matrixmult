package main

import "bufio"
import "gopkg.in/Shopify/sarama.v1"
import "log"
import "time"
import "context"
import "math/rand"
import "os"
import "strings"

func genMatrix(numRows, numCols int) *Matrix {
    matrix := NewEmptyMatrix(numRows, numCols)
    for r := 0; r < numRows; r++ {
        for c := 0; c < numCols; c++ {
            matrix.Set(r, c, rand.Intn(10))
        }
    }
    return matrix
}

func nextInputMatrix(f *os.File) *Matrix {
    var lines []string
    scanner := bufio.NewScanner(f)
    for len(lines) < gMatrixDimen {
        if !scanner.Scan() {
            if scanner.Err() != nil {
                panic(scanner.Err())
            } else {
                break
            }
        }
        if len(scanner.Text()) == 0 {
            continue
        }

        lines = append(lines, scanner.Text())
    }
    if len(lines) == 0 {
        return nil
    } else if len(lines) < gMatrixDimen {
        panic("Not enough data in input file")
    }

    return StrToMatrix(strings.Join(lines, "\n"))
}

func Producer(inputPath string, ctx context.Context) {
    // open input file
    f, err := os.Open(inputPath)
    if err != nil {
        panic(err)
    }
    defer f.Close()

    // make producer
    producer, err := sarama.NewAsyncProducer([]string{gKafkaHost}, nil)
    if err != nil {
        panic(err)
    }
    defer CloseCloser(producer)

    var enqueued, errors int
    ticker := time.NewTicker(3 * time.Second)
    ProducerLoop:
    for {
        // read next matrix
        matrix := nextInputMatrix(f)
        if matrix == nil {
            break ProducerLoop
        }

        // publish the matrix
        select {
        case <-ticker.C:
            msg := sarama.ProducerMessage{
                Topic: gInputMatrixesTopic,
                Key: nil,
                Value: sarama.StringEncoder(matrix.String()),
            }
            producer.Input() <- &msg
            log.Println("Generated matrix")
            enqueued++

        case err := <-producer.Errors():
            log.Println("Failed to produce message", err)
            errors++

        case <-ctx.Done():
            break ProducerLoop
        }
    }

    log.Printf("Enqueued: %d; errors: %d\n", enqueued, errors)
}
