package main

import (
	"context"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

const (
	topic          = "test-kafka-topic"
	broker1Address = "localhost:9093"
	broker2Address = "localhost:9094"
	broker3Address = "localhost:9095"
)

func main() {
	ctx := context.Background()

	go produce(ctx)
	consume(ctx)
}

func produce(ctx context.Context) {
	logger := log.New(os.Stdout, "kafka writer: ", 0)

	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:      []string{broker1Address, broker2Address, broker3Address},
		Topic:        topic,
		RequiredAcks: 1,
		Logger:       logger,
	})

	i := 0
	for {
		err := writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte("test message" + strconv.Itoa(i)),
		})
		if err != nil {
			panic("could not write message on kafka " + err.Error())
		}

		log.Println("writes", i)
		i++
		time.Sleep(time.Second)
	}
}

func consume(ctx context.Context) {
	logger := log.New(os.Stdout, "kafka reader: ", 0)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     []string{broker1Address, broker2Address, broker3Address},
		Topic:       topic,
		GroupID:     "group-test",
		StartOffset: kafka.FirstOffset,
		Logger:      logger,
	})

	for {
		message, err := reader.ReadMessage(ctx)
		if err != nil {
			panic("could not read message by kafka " + err.Error())
		}

		log.Println("Received Message: ", string(message.Value))
	}
}
