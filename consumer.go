package main

import (
	"context"
	"fmt"
	"log"

	"github.com/segmentio/kafka-go"
)

func main() {
	// Параметры подключения
	brokers := []string{"localhost:9092"}
	topic := "my-first-topic"
	groupID := "my-consumer-group"

	// Создаём Reader (читатель сообщений) с Consumer Group
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        brokers,
		Topic:          topic,
		GroupID:        groupID,
		StartOffset:    kafka.FirstOffset, // Читаем с самого начала
		CommitInterval: 1,                 // Коммитим смещение после каждого сообщения
	})

	defer r.Close()

	fmt.Printf("🎧 Слушаю топик '%s' в группе '%s'...\n\n", topic, groupID)

	ctx := context.Background()

	// Бесконечно читаем сообщения
	for {
		msg, err := r.ReadMessage(ctx)
		if err != nil {
			log.Fatalf("❌ Ошибка при чтении сообщения: %v", err)
		}

		fmt.Printf("📨 Новое сообщение:\n")
		fmt.Printf("   Offset:    %d\n", msg.Offset)
		fmt.Printf("   Partition: %d\n", msg.Partition)
		fmt.Printf("   Key:       %s\n", string(msg.Key))
		fmt.Printf("   Value:     %s\n", string(msg.Value))
		fmt.Printf("   Time:      %s\n\n", msg.Time.Format("15:04:05"))
	}
}
