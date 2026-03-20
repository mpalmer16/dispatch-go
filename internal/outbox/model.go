package outbox

import "time"

type Row struct {
	ID			string
	AggregateID string
  	EventType   string
  	Payload     []byte
  	CreatedAt   time.Time
  	ProcessedAt *time.Time
}