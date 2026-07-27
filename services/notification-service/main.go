// Notification Service: слушает payments.completed и payments.failed,
// отправляет уведомление клиенту (вебхук/email/SMS — здесь симулировано
// логированием + HTTP POST на настраиваемый webhook URL).
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"time"

	kafka "github.com/segmentio/kafka-go"
)

func main() {
	ctx := context.Background()
	kafkaBrokers := envOr("KAFKA_BROKERS", "localhost:9092")
	webhookURL := envOr("WEBHOOK_URL", "")

	topics := []string{"payments.completed", "payments.failed"}
	for _, topic := range topics {
		go consume(ctx, kafkaBrokers, topic, webhookURL)
	}

	log.Println("notification-service started")
	select {} // держим процесс живым; горутины работают в фоне
}

func consume(ctx context.Context, brokers, topic, webhookURL string) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{brokers},
		GroupID:        "notification-service",
		Topic:          topic,
		CommitInterval: 0,
	})
	defer reader.Close()

	client := &http.Client{Timeout: 5 * time.Second}

	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Printf("[%s] fetch error: %v", topic, err)
			continue
		}

		if webhookURL != "" {
			if err := postWebhook(client, webhookURL, m.Value); err != nil {
				log.Printf("[%s] webhook delivery failed, will retry: %v", topic, err)
				continue // не коммитим -> повторная попытка при следующей доставке
			}
		} else {
			log.Printf("[%s] notification: %s", topic, string(m.Value))
		}

		if err := reader.CommitMessages(ctx, m); err != nil {
			log.Printf("commit error: %v", err)
		}
	}
}

func postWebhook(client *http.Client, url string, payload []byte) error {
	req, err := http.NewRequest(http.MethodPost, url, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return errResponse(resp.StatusCode)
	}
	return nil
}

type errResponse int

func (e errResponse) Error() string {
	b, _ := json.Marshal(map[string]int{"status_code": int(e)})
	return string(b)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
