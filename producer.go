package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

// Функция для создания топика
func createTopicIfNotExists(brokers []string, topic string) error {
	// Подключаемся к Kafka для управления топиками
	conn, err := kafka.Dial("tcp", brokers[0])
	if err != nil {
		return fmt.Errorf("ошибка подключения к Kafka: %w", err)
	}
	defer conn.Close()

	// Получаем контроллер кластера
	controller, err := conn.Controller()
	if err != nil {
		return fmt.Errorf("ошибка получения контроллера: %w", err)
	}

	// Подключаемся к контроллеру
	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return fmt.Errorf("ошибка подключения к контроллеру: %w", err)
	}
	defer controllerConn.Close()

	// Конфигурация топика
	topicConfigs := []kafka.TopicConfig{
		{
			Topic:             topic,
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}

	// Пытаемся создать топик
	err = controllerConn.CreateTopics(topicConfigs...)
	if err != nil {
		// Если топик уже существует, это нормально
		if err.Error() != "Topic with this name already exists: -36" {
			log.Printf("Заметка: %v (топик может уже существовать)\n", err)
		}
	} else {
		fmt.Printf("✓ Топик '%s' успешно создан\n", topic)
	}

	return nil
}

func main() {
	// Адреса Kafka брокеров
	brokers := []string{"localhost:9092"}
	topic := "my-first-topic"

	// Создаём топик перед отправкой сообщений
	fmt.Println("Попытка создания топика...")
	if err := createTopicIfNotExists(brokers, topic); err != nil {
		log.Printf("Предупреждение при создании топика: %v\n", err)
	}
	time.Sleep(2 * time.Second) // Даём время Kafka на обновление

	// Создаём Writer (отправитель сообщений)
	// Используем новый API вместо WriterConfig
	w := &kafka.Writer{
		Addr:     kafka.TCP(brokers...),
		Topic:    topic,
		Balancer: &kafka.LeastBytes{},
	}

	defer w.Close()

	ctx := context.Background()

	// Отправляем 5 тестовых сообщений
	for i := 1; i <= 5; i++ {
		message := fmt.Sprintf("Сообщение номер %d, отправлено в %s", i, time.Now().Format("15:04:05"))

		err := w.WriteMessages(ctx, kafka.Message{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(message),
		})

		if err != nil {
			log.Fatalf("Ошибка при отправке: %v", err)
		}

		fmt.Printf("✓ Отправлено: %s\n", message)
		time.Sleep(1 * time.Second)
	}

	fmt.Println("\nВсе сообщения отправлены!")
}
