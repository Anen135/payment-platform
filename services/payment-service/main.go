// Payment Service: consumer группа читает payments.requested, выполняет
// антифрод-проверку и вызов внешнего процессинга (PSP), обновляет статус
// платежа и пишет результат в outbox для дальнейшей публикации в Kafka.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	kafka "github.com/segmentio/kafka-go"
	"github.com/sony/gobreaker"
)

type RequestedEvent struct {
	PaymentID string `json:"payment_id"`
	UserID    string `json:"user_id"`
	Amount    int64  `json:"amount"`
	Currency  string `json:"currency"`
	Recipient string `json:"recipient"`
}

type Worker struct {
	db      *pgxpool.Pool
	writer  *kafka.Writer
	breaker *gobreaker.CircuitBreaker
}

func main() {
	ctx := context.Background()

	pgURL := envOr("DATABASE_URL", "postgres://payment:payment@localhost:5432/payments?sslmode=disable")
	kafkaBrokers := envOr("KAFKA_BROKERS", "localhost:9092")
	groupID := envOr("KAFKA_GROUP_ID", "payment-service")

	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		log.Fatalf("pg connect: %v", err)
	}
	defer pool.Close()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{kafkaBrokers},
		GroupID:        groupID,
		Topic:          "payments.requested",
		MinBytes:       1,
		MaxBytes:       10e6,
		CommitInterval: 0, // ручной коммит после успешной обработки — at-least-once
	})
	defer reader.Close()

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaBrokers),
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: true,
	}
	defer writer.Close()

	cb := gobreaker.NewCircuitBreaker(gobreaker.Settings{
		Name:        "external-psp",
		MaxRequests: 5,
		Timeout:     10 * time.Second,
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			return counts.ConsecutiveFailures > 5
		},
	})

	w := &Worker{db: pool, writer: writer, breaker: cb}

	// Пул воркеров для параллельной обработки сообщений одного консьюмера.
	// Реальный параллелизм достигается количеством партиций топика и
	// количеством реплик сервиса (каждая реплика = 1 consumer в группе).
	concurrency := 8
	msgCh := make(chan kafka.Message, concurrency*2)

	for i := 0; i < concurrency; i++ {
		go func() {
			for m := range msgCh {
				w.process(ctx, reader, m)
			}
		}()
	}

	log.Printf("payment-service started, group=%s", groupID)
	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Printf("fetch error: %v", err)
			continue
		}
		msgCh <- m
	}
}

func (w *Worker) process(ctx context.Context, reader *kafka.Reader, m kafka.Message) {
	var evt RequestedEvent
	if err := json.Unmarshal(m.Value, &evt); err != nil {
		log.Printf("bad message, skipping: %v", err)
		_ = reader.CommitMessages(ctx, m)
		return
	}

	if err := w.handlePayment(ctx, evt); err != nil {
		log.Printf("payment %s processing error: %v (will retry via redelivery)", evt.PaymentID, err)
		// Не коммитим offset — сообщение будет повторно доставлено.
		// В проде: счётчик ретраев в заголовке сообщения + DLQ топик
		// payments.requested.dlq после N попыток.
		return
	}

	if err := reader.CommitMessages(ctx, m); err != nil {
		log.Printf("commit error: %v", err)
	}
}

func (w *Worker) handlePayment(ctx context.Context, evt RequestedEvent) error {
	// Идемпотентный переход pending -> processing.
	tx, err := w.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	var status string
	err = tx.QueryRow(ctx, `
		SELECT status FROM payments WHERE id = $1 FOR UPDATE`, evt.PaymentID).Scan(&status)
	if err != nil {
		return err
	}

	if status != "pending" {
		// Уже обработан (повторная доставка) — ничего не делаем, коммитим offset выше.
		return tx.Commit(ctx)
	}

	if _, err := tx.Exec(ctx, `UPDATE payments SET status = 'processing', updated_at = now() WHERE id = $1`, evt.PaymentID); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}

	// Антифрод + вызов внешнего PSP — за пределами транзакции БД (I/O),
	// обёрнуты circuit breaker'ом.
	approved, reason, err := w.riskCheckAndCharge(ctx, evt)
	if err != nil {
		return err // сеть/PSP недоступен -> ретрай всего события
	}

	finalStatus := "completed"
	if !approved {
		finalStatus = "failed"
	}

	payload, _ := json.Marshal(map[string]any{
		"payment_id": evt.PaymentID,
		"user_id":    evt.UserID,
		"amount":     evt.Amount,
		"currency":   evt.Currency,
		"recipient":  evt.Recipient,
		"reason":     reason,
	})

	tx2, err := w.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx2.Rollback(ctx)

	if _, err := tx2.Exec(ctx, `
		UPDATE payments SET status = $2, failure_reason = $3, updated_at = now()
		WHERE id = $1`, evt.PaymentID, finalStatus, reason); err != nil {
		return err
	}

	topic := "payments." + finalStatus
	if _, err := tx2.Exec(ctx, `
		INSERT INTO outbox_events (aggregate_id, topic, payload) VALUES ($1, $2, $3)`,
		evt.PaymentID, topic, payload); err != nil {
		return err
	}

	if err := tx2.Commit(ctx); err != nil {
		return err
	}

	return w.relayOutboxEvent(ctx, evt.PaymentID, topic, payload)
}

// relayOutboxEvent публикует событие сразу (best-effort), но т.к. запись
// уже лежит в outbox_events, отдельный relay-процесс подчистит недоставленное.
func (w *Worker) relayOutboxEvent(ctx context.Context, paymentID, topic string, payload []byte) error {
	_, err := w.breaker.Execute(func() (any, error) {
		return nil, w.writer.WriteMessages(ctx, kafka.Message{
			Topic: topic,
			Key:   []byte(paymentID),
			Value: payload,
		})
	})
	if err != nil {
		log.Printf("kafka publish deferred to relay for %s: %v", paymentID, err)
		return nil // не фатально — фоновый relay (см. relay.go) досоздаст
	}
	_, _ = w.db.Exec(ctx, `UPDATE outbox_events SET published = true, published_at = now()
		WHERE aggregate_id = $1 AND topic = $2 AND NOT published`, paymentID, topic)
	return nil
}

// riskCheckAndCharge — симуляция антифрод-проверки и вызова внешнего PSP.
// В реальной системе — HTTP/gRPC вызов с таймаутом и ретраями.
func (w *Worker) riskCheckAndCharge(ctx context.Context, evt RequestedEvent) (approved bool, reason string, err error) {
	result, cbErr := w.breaker.Execute(func() (any, error) {
		select {
		case <-time.After(50 * time.Millisecond): // имитация latency внешнего сервиса
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if evt.Amount > 1_000_000_00 { // порог антифрода (в минимальных единицах валюты)
			return false, nil
		}
		return true, nil
	})
	if cbErr != nil {
		if errors.Is(cbErr, gobreaker.ErrOpenState) {
			return false, "circuit_breaker_open", nil
		}
		return false, "", cbErr
	}
	if approvedVal, ok := result.(bool); ok && !approvedVal {
		return false, "risk_check_declined", nil
	}
	// небольшая случайная доля отказов внешнего PSP для реалистичности демо
	if rand.Intn(1000) == 0 {
		return false, "psp_declined", nil
	}
	return true, "", nil
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
