package app

import (
	"context"
	"database/sql"
	"errors"
	"reflect"
	"testing"
	"time"

	"dispatch-go/internal/config"
	"dispatch-go/internal/outbox"

	ckafka "github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func TestProcessBatchReturnsNilWhenNoPendingRows(t *testing.T) {
	restoreRelayDependencies(t)

	listPendingOutbox = func(_ context.Context, _ *sql.DB, limit int) ([]outbox.Row, error) {
		if limit != 10 {
			t.Fatalf("limit = %d, want %d", limit, 10)
		}

		return nil, nil
	}

	publish = func(_ *ckafka.Producer, _ string, _ []byte) error {
		t.Fatal("publish() should not be called")
		return nil
	}

	markOutboxProcessed = func(_ context.Context, _ *sql.DB, _ string) error {
		t.Fatal("markOutboxProcessed() should not be called")
		return nil
	}

	err := processBatch(context.Background(), testConfig(), nil, nil)
	if err != nil {
		t.Fatalf("processBatch() error = %v", err)
	}
}

func TestProcessBatchWrapsListError(t *testing.T) {
	restoreRelayDependencies(t)

	listPendingOutbox = func(context.Context, *sql.DB, int) ([]outbox.Row, error) {
		return nil, errors.New("boom")
	}

	err := processBatch(context.Background(), testConfig(), nil, nil)
	if err == nil {
		t.Fatal("processBatch() error = nil, want error")
	}

	if got, want := err.Error(), "list pending outbox rows: boom"; got != want {
		t.Fatalf("processBatch() error = %q, want %q", got, want)
	}
}

func TestProcessBatchStopsWhenPublishFails(t *testing.T) {
	restoreRelayDependencies(t)

	listPendingOutbox = func(context.Context, *sql.DB, int) ([]outbox.Row, error) {
		return []outbox.Row{
			{ID: "row-1", AggregateID: "order-1", Payload: []byte("payload-1")},
		}, nil
	}

	var markCalls int
	publish = func(_ *ckafka.Producer, topic string, payload []byte) error {
		if topic != "orders.created" {
			t.Fatalf("topic = %q, want %q", topic, "orders.created")
		}

		if string(payload) != "payload-1" {
			t.Fatalf("payload = %q, want %q", string(payload), "payload-1")
		}

		return errors.New("publish failed")
	}

	markOutboxProcessed = func(context.Context, *sql.DB, string) error {
		markCalls++
		return nil
	}

	err := processBatch(context.Background(), testConfig(), nil, nil)
	if err == nil {
		t.Fatal("processBatch() error = nil, want error")
	}

	if got, want := err.Error(), "publish outbox row row-1: publish failed"; got != want {
		t.Fatalf("processBatch() error = %q, want %q", got, want)
	}

	if markCalls != 0 {
		t.Fatalf("markOutboxProcessed() calls = %d, want %d", markCalls, 0)
	}
}

func TestProcessBatchWrapsMarkError(t *testing.T) {
	restoreRelayDependencies(t)

	listPendingOutbox = func(context.Context, *sql.DB, int) ([]outbox.Row, error) {
		return []outbox.Row{
			{ID: "row-1", AggregateID: "order-1", Payload: []byte("payload-1")},
		}, nil
	}

	publish = func(_ *ckafka.Producer, _ string, _ []byte) error {
		return nil
	}

	markOutboxProcessed = func(_ context.Context, _ *sql.DB, id string) error {
		if id != "row-1" {
			t.Fatalf("id = %q, want %q", id, "row-1")
		}

		return errors.New("mark failed")
	}

	err := processBatch(context.Background(), testConfig(), nil, nil)
	if err == nil {
		t.Fatal("processBatch() error = nil, want error")
	}

	if got, want := err.Error(), "mark outbox row row-1 processed: mark failed"; got != want {
		t.Fatalf("processBatch() error = %q, want %q", got, want)
	}
}

func TestProcessBatchPublishesAndMarksRowsInOrder(t *testing.T) {
	restoreRelayDependencies(t)

	listPendingOutbox = func(context.Context, *sql.DB, int) ([]outbox.Row, error) {
		return []outbox.Row{
			{ID: "row-1", AggregateID: "order-1", Payload: []byte("payload-1")},
			{ID: "row-2", AggregateID: "order-2", Payload: []byte("payload-2")},
		}, nil
	}

	var events []string
	publish = func(_ *ckafka.Producer, topic string, payload []byte) error {
		events = append(events, "publish:"+topic+":"+string(payload))
		return nil
	}

	markOutboxProcessed = func(_ context.Context, _ *sql.DB, id string) error {
		events = append(events, "mark:"+id)
		return nil
	}

	err := processBatch(context.Background(), testConfig(), nil, nil)
	if err != nil {
		t.Fatalf("processBatch() error = %v", err)
	}

	wantEvents := []string{
		"publish:orders.created:payload-1",
		"mark:row-1",
		"publish:orders.created:payload-2",
		"mark:row-2",
	}

	if !reflect.DeepEqual(events, wantEvents) {
		t.Fatalf("events = %#v, want %#v", events, wantEvents)
	}
}

func TestRunProcessesBatchImmediatelyAndOnTick(t *testing.T) {
	restoreRelayDependencies(t)

	ticker := &fakeTicker{ch: make(chan time.Time, 1)}
	newTicker = func(interval time.Duration) relayTicker {
		if interval != 25*time.Millisecond {
			t.Fatalf("interval = %s, want %s", interval, 25*time.Millisecond)
		}

		return ticker
	}

	callCh := make(chan int, 2)
	var calls int
	listPendingOutbox = func(context.Context, *sql.DB, int) ([]outbox.Row, error) {
		calls++
		callCh <- calls
		return nil, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(ctx, testConfig(), nil, nil)
	}()

	waitForCall(t, callCh, 1)
	ticker.ch <- time.Now()
	waitForCall(t, callCh, 2)

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not return after cancel")
	}

	if !ticker.stopped {
		t.Fatal("ticker.Stop() was not called")
	}
}

func TestRunContinuesAfterBatchFailure(t *testing.T) {
	restoreRelayDependencies(t)

	ticker := &fakeTicker{ch: make(chan time.Time, 1)}
	newTicker = func(time.Duration) relayTicker {
		return ticker
	}

	callCh := make(chan int, 2)
	var calls int
	listPendingOutbox = func(context.Context, *sql.DB, int) ([]outbox.Row, error) {
		calls++
		callCh <- calls

		if calls == 1 {
			return nil, errors.New("boom")
		}

		return nil, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- Run(ctx, testConfig(), nil, nil)
	}()

	waitForCall(t, callCh, 1)
	ticker.ch <- time.Now()
	waitForCall(t, callCh, 2)

	cancel()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Run() error = %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Run() did not return after cancel")
	}
}

type fakeTicker struct {
	ch      chan time.Time
	stopped bool
}

func (t *fakeTicker) C() <-chan time.Time {
	return t.ch
}

func (t *fakeTicker) Stop() {
	t.stopped = true
}

func restoreRelayDependencies(t *testing.T) {
	t.Helper()

	originalListPendingOutbox := listPendingOutbox
	originalMarkOutboxProcessed := markOutboxProcessed
	originalPublish := publish
	originalNewTicker := newTicker

	t.Cleanup(func() {
		listPendingOutbox = originalListPendingOutbox
		markOutboxProcessed = originalMarkOutboxProcessed
		publish = originalPublish
		newTicker = originalNewTicker
	})
}

func testConfig() config.Config {
	return config.Config{
		KafkaTopic:   "orders.created",
		PollInterval: 25 * time.Millisecond,
		BatchSize:    10,
	}
}

func waitForCall(t *testing.T, callCh <-chan int, want int) {
	t.Helper()

	select {
	case got := <-callCh:
		if got != want {
			t.Fatalf("call number = %d, want %d", got, want)
		}
	case <-time.After(time.Second):
		t.Fatalf("timed out waiting for call %d", want)
	}
}
