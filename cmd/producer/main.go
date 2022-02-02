package main

import (
	"fmt"
	"github.com/confluentinc/confluent-kafka-go/kafka"
	"log"
)

func main() {
	deliveryChan := make(chan kafka.Event)

	producer := NewKafkaProducer()
	Publish("transferencia", "teste", producer, []byte("transferencia2"), deliveryChan)
	go DeliveryReport(deliveryChan) //assincrono (Outra thread)

	// e := <-deliveryChan //sincrono (Não é muito boa essa forma)
	// msg := e.(*kafka.Message)
	// if msg.TopicPartition.Error != nil {
	// 	fmt.Println("Erro ao enviar")
	// }else{
	// 	fmt.Println("Mensagem enviada:", msg.TopicPartition)
	// }

	producer.Flush(10000)
}

func NewKafkaProducer() *kafka.Producer{
	configMap := &kafka.ConfigMap{
		"bootstrap.servers": "gokafka_kafka_1:9092",
		"delivery.timeout.ms": "0",
		"acks": "all", //0,1 ou all
		"enable.idempotence": "true", //default: false (When true acks = all)
	}
	p, err := kafka.NewProducer(configMap)
	if err != nil {
		log.Println(err.Error())
	}
	return p
}

func Publish(msg string, topic string, producer *kafka.Producer, key []byte, deliveryChan chan kafka.Event) error {
	message := &kafka.Message{
		Value: []byte(msg),
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key: key,
	}
	err := producer.Produce(message, deliveryChan)
	if err != nil {
		return err
	}
	return nil
}

func DeliveryReport(deliveryChan chan kafka.Event) {
	for e := range deliveryChan {
		switch ev := e.(type) {
		case *kafka.Message:
			if ev.TopicPartition.Error != nil {
				fmt.Println("Erro ao enviar")
			}else{
				fmt.Println("Mensagem enviada:", ev.TopicPartition)
				// anotar no banco de dados que a mensagem foi processada
				// ex: confirma que uma transferencia bancaria ocorreu.
			}	
		}
	}
}