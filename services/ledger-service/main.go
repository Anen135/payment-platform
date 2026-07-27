// Ledger Service: консьюмер payments.completed, пишет двойную бухгалтерскую
// запись (debit/credit) в ledger_entries. Идемпотентно за счёт уникального
// индекса (payment_id, direction) — повторная доставка не создаст дубли.
package main

import (
	"context"
	"encoding/json"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	kafka "github.com/segmentio/kafka-go"
)

type CompletedEvent struct {
	PaymentID string `json:"payment_id"`
	UserID    string `json:"user_id"`
	Amount    int64  `json:"amount"`
	Currency  string `json:"currency"`
	Recipient string `json:"recipient"`
}

func main() {
	ctx := context.Background()

	pgURL := envOr("DATABASE_URL", "postgres://payment:payment@localhost:5432/payments?sslmode=disable")
	kafkaBrokers := envOr("KAFKA_BROKERS", "localhost:9092")

	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		log.Fatalf("pg connect: %v", err)
	}
	defer pool.Close()

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{kafkaBrokers},
		GroupID:        "ledger-service",
		Topic:          "payments.completed",
		CommitInterval: 0,
	})
	defer reader.Close()

	log.Println("ledger-service started")
	for {
		m, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Printf("fetch error: %v", err)
			continue
		}

		var evt CompletedEvent
		if err := json.Unmarshal(m.Value, &evt); err != nil {
			log.Printf("bad message: %v", err)
			_ = reader.CommitMessages(ctx, m)
			continue
		}

		if err := writeDoubleEntry(ctx, pool, evt); err != nil {
			log.Printf("ledger write failed for %s: %v (will retry)", evt.PaymentID, err)
			continue // не коммитим -> повторная доставка
		}

		if err := reader.CommitMessages(ctx, m); err != nil {
			log.Printf("commit error: %v", err)
		}
	}
}

func writeDoubleEntry(ctx context.Context, pool *pgxpool.Pool, evt CompletedEvent) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	// ON CONFLICT DO NOTHING — благодаря уникальному индексу (payment_id, direction)
	// повторная доставка события не создаст дублирующую проводку.
	_, err = tx.Exec(ctx, `
		INSERT INTO ledger_entries (payment_id, account_id, direction, amount, currency)
		VALUES ($1, $2, 'debit', $3, $4)
		ON CONFLICT (payment_id, direction) DO NOTHING`,
		evt.PaymentID, evt.UserID, evt.Amount, evt.Currency)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO ledger_entries (payment_id, account_id, direction, amount, currency)
		VALUES ($1, $2, 'credit', $3, $4)
		ON CONFLICT (payment_id, direction) DO NOTHING`,
		evt.PaymentID, evt.Recipient, evt.Amount, evt.Currency)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}
