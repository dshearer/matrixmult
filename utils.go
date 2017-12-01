package main

import "io"
import "log"

const gInputMatrixesTopic = "input-matrixes"
const gOutputMatrixesTopic = "output-matrixes"
const gKafkaHost = "localhost:9092"

func CloseCloser(closer io.Closer) {
    if err := closer.Close(); err != nil {
        log.Fatalln(err)
    }
}
