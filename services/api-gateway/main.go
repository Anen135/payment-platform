package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	kafka "github.com/segmentio/kafka-go"
)

type PaymentRequest struct {
	UserID    string `json:"user_id"`
	Amount    int64  `json:"amount"`
	Currency  string `json:"currency"`
	Recipient string `json:"recipient"`
}

type PaymentResponse struct {
	PaymentID string `json:"payment_id"`
	Status    string `json:"status"`
}

type Server struct {
	db     *pgxpool.Pool
	rdb    *redis.Client
	writer *kafka.Writer
}

func main() {
	ctx := context.Background()

	pgURL := envOr("DATABASE_URL", "postgres://payment:payment@localhost:5432/payments?sslmode=disable")
	redisAddr := envOr("REDIS_ADDR", "localhost:6379")
	kafkaBrokers := envOr("KAFKA_BROKERS", "localhost:9092")

	pool, err := pgxpool.New(ctx, pgURL)
	if err != nil {
		log.Fatalf("pg connect: %v", err)
	}
	defer pool.Close()

	rdb := redis.NewClient(&redis.Options{Addr: redisAddr})
	defer rdb.Close()

	writer := &kafka.Writer{
		Addr:                   kafka.TCP(kafkaBrokers),
		Balancer:               &kafka.Hash{},
		AllowAutoTopicCreation: true,
		BatchTimeout:           10 * time.Millisecond,
	}
	defer writer.Close()

	srv := &Server{db: pool, rdb: rdb, writer: writer}

	// background job that publishes any pending outbox events to Kafka
	go srv.runOutboxRelay(ctx)

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/payments", srv.handleCreatePayment)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/readyz", srv.handleReady)

	addr := envOr("HTTP_ADDR", ":8080")
	log.Printf("api-gateway listening on %s", addr)
	log.Fatal(http.ListenAndServe(addr, mux))
}

func (s *Server) handleReady(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
	defer cancel()
	if err := s.db.Ping(ctx); err != nil {
		http.Error(w, "db not ready", http.StatusServiceUnavailable)
		return
	}
	if err := s.rdb.Ping(ctx).Err(); err != nil {
		http.Error(w, "redis not ready", http.StatusServiceUnavailable)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Server) handleCreatePayment(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	idemKey := r.Header.Get("Idempotency-Key")
	if idemKey == "" {
		http.Error(w, "Idempotency-Key header is required", http.StatusBadRequest)
		return
	}

	var req PaymentRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "invalid json body", http.StatusBadRequest)
		return
	}
	if req.Amount <= 0 || req.UserID == "" || req.Recipient == "" || len(req.Currency) != 3 {
		http.Error(w, "invalid payment request", http.StatusUnprocessableEntity)
		return
	}

	ctx := r.Context()

	// check idempotency key in Redis first
	lockKey := "idem:" + idemKey
	acquired, err := s.rdb.SetNX(ctx, lockKey, "1", 24*time.Hour).Result()
	if err != nil {
		log.Printf("redis error: %v", err)
	}

	if err == nil && !acquired {
		resp, found, err := s.findByIdempotencyKey(ctx, idemKey)
		if err != nil {
			http.Error(w, "internal error", http.StatusInternalServerError)
			return
		}
		if found {
			writeJSON(w, http.StatusOK, resp)
			return
		}
		http.Error(w, "request with this idempotency key is being processed", http.StatusConflict)
		return
	}

	paymentID := uuid.New()
	payload, _ := json.Marshal(map[string]any{
		"payment_id": paymentID.String(),
		"user_id":    req.UserID,
		"amount":     req.Amount,
		"currency":   req.Currency,
		"recipient":  req.Recipient,
	})

	tx, err := s.db.Begin(ctx)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO payments (id, idempotency_key, user_id, recipient, amount, currency, status)
		VALUES ($1, $2, $3, $4, $5, $6, 'pending')`,
		paymentID, idemKey, req.UserID, req.Recipient, req.Amount, req.Currency)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO outbox_events (aggregate_id, topic, payload)
		VALUES ($1, 'payments.requested', $2)`,
		paymentID, payload)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	if err := tx.Commit(ctx); err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	writeJSON(w, http.StatusAccepted, PaymentResponse{PaymentID: paymentID.String(), Status: "pending"})
}

func (s *Server) findByIdempotencyKey(ctx context.Context, key string) (PaymentResponse, bool, error) {
	var resp PaymentResponse
	err := s.db.QueryRow(ctx, `SELECT id::text, status FROM payments WHERE idempotency_key = $1`, key).
		Scan(&resp.PaymentID, &resp.Status)
	if errors.Is(err, pgx.ErrNoRows) {
		return resp, false, nil
	}
	if err != nil {
		return resp, false, err
	}
	return resp, true, nil
}

// periodically publish any unsent events from outbox_events to Kafka
func (s *Server) runOutboxRelay(ctx context.Context) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.relayBatch(ctx); err != nil {
				log.Printf("outbox relay error: %v", err)
			}
		}
	}
}

func (s *Server) relayBatch(ctx context.Context) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	rows, err := tx.Query(ctx, `
		SELECT id, aggregate_id, topic, payload
		FROM outbox_events
		WHERE NOT published
		ORDER BY id
		LIMIT 100
		FOR UPDATE SKIP LOCKED`)
	if err != nil {
		return err
	}

	type evt struct {
		id      int64
		aggID   string
		topic   string
		payload []byte
	}
	var events []evt
	for rows.Next() {
		var e evt
		if err := rows.Scan(&e.id, &e.aggID, &e.topic, &e.payload); err != nil {
			rows.Close()
			return err
		}
		events = append(events, e)
	}
	rows.Close()

	if len(events) == 0 {
		return tx.Commit(ctx)
	}

	msgs := make([]kafka.Message, 0, len(events))
	ids := make([]int64, 0, len(events))
	for _, e := range events {
		msgs = append(msgs, kafka.Message{
			Topic: e.topic,
			Key:   []byte(e.aggID),
			Value: e.payload,
		})
		ids = append(ids, e.id)
	}

	if err := s.writer.WriteMessages(ctx, msgs...); err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		UPDATE outbox_events SET published = true, published_at = now()
		WHERE id = ANY($1)`, ids)
	if err != nil {
		return err
	}

	return tx.Commit(ctx)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func envOr(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}