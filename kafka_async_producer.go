package main

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
)

//Message to be sent
type Message struct {
	Who string
	TimeAsString string
}

func main() {
	config := sarama.NewConfig()

	config.Producer.RequiredAcks = sarama.WaitForAll

	config.Producer.Retry.Max = 5

	brokers := []string{"localhost:9092"}
	//sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)

	asyncProducer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := asyncProducer.Close(); err != nil {
			log.Fatal(err)
		}
	} ()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	exitProgram := make(chan struct{})

	var nPublished, nErrors int
	go func() {
		for {
			time.Sleep(5 * time.Second)

			body := Message{
				Who:          "aProcess",
				TimeAsString: strconv.Itoa(int(time.Now().Unix())),
			}

			payload, _ := json.Marshal(body)

			msg := &sarama.ProducerMessage{
				Topic: "currentTime",
				Key:   sarama.StringEncoder("aKey"),
				Value: sarama.ByteEncoder(payload),
			}

			select {
			case asyncProducer.Input() <- msg:
				nPublished++
				fmt.Println("Produce message")
			case err := <- asyncProducer.Errors():
				nErrors++
				fmt.Println("failed to produce message: ", err)
			case <- signals:
				exitProgram <- struct{}{}
			}

			log.Printf("time: %d; Published: %d; Errors: %d\n",time.Now(), nPublished, nErrors)
		}
	}()

	<- exitProgram
}