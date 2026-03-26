package postgres

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCountPendingOutboxReturnsCount(t *testing.T) {
	db := newTestDB(t, testExpectation{
		kind:  expectationKindQuery,
		query: countPendingOutboxQuery,
		rows: &testRows{
			columns: []string{"count"},
			data:    [][]driver.Value{{int64(3)}},
		},
	})

	count, err := CountPendingOutbox(context.Background(), db)
	if err != nil {
		t.Fatalf("CountPendingOutbox() error = %v", err)
	}

	if count != 3 {
		t.Fatalf("CountPendingOutbox() count = %d, want %d", count, 3)
	}
}

func TestCountPendingOutboxWrapsQueryError(t *testing.T) {
	db := newTestDB(t, testExpectation{
		kind:  expectationKindQuery,
		query: countPendingOutboxQuery,
		err:   errors.New("boom"),
	})

	_, err := CountPendingOutbox(context.Background(), db)
	if err == nil {
		t.Fatal("CountPendingOutbox() error = nil, want error")
	}

	if got, want := err.Error(), "count pending outbox rows: boom"; got != want {
		t.Fatalf("CountPendingOutbox() error = %q, want %q", got, want)
	}
}

func TestListPendingOutboxReturnsRows(t *testing.T) {
	createdAt := time.Date(2026, time.March, 25, 9, 30, 0, 0, time.UTC)

	db := newTestDB(t, testExpectation{
		kind:  expectationKindQuery,
		query: listPendingOutboxQuery,
		args:  []driver.Value{int64(2)},
		rows: &testRows{
			columns: []string{"id", "aggregate_id", "event_type", "payload", "created_at", "processed_at"},
			data: [][]driver.Value{
				{"row-1", "order-1", "order.created", []byte(`{"id":"order-1"}`), createdAt, nil},
				{"row-2", "order-2", "order.created", []byte(`{"id":"order-2"}`), createdAt.Add(time.Second), nil},
			},
		},
	})

	rows, err := ListPendingOutbox(context.Background(), db, 2)
	if err != nil {
		t.Fatalf("ListPendingOutbox() error = %v", err)
	}

	if len(rows) != 2 {
		t.Fatalf("ListPendingOutbox() len = %d, want %d", len(rows), 2)
	}

	if rows[0].ID != "row-1" || rows[0].AggregateID != "order-1" || rows[0].EventType != "order.created" {
		t.Fatalf("ListPendingOutbox() first row = %+v", rows[0])
	}

	if string(rows[0].Payload) != `{"id":"order-1"}` {
		t.Fatalf("ListPendingOutbox() first payload = %q", string(rows[0].Payload))
	}

	if rows[0].ProcessedAt != nil {
		t.Fatalf("ListPendingOutbox() first ProcessedAt = %v, want nil", rows[0].ProcessedAt)
	}

	if rows[1].ID != "row-2" || rows[1].AggregateID != "order-2" {
		t.Fatalf("ListPendingOutbox() second row = %+v", rows[1])
	}
}

func TestListPendingOutboxWrapsQueryError(t *testing.T) {
	db := newTestDB(t, testExpectation{
		kind:  expectationKindQuery,
		query: listPendingOutboxQuery,
		args:  []driver.Value{int64(10)},
		err:   errors.New("boom"),
	})

	_, err := ListPendingOutbox(context.Background(), db, 10)
	if err == nil {
		t.Fatal("ListPendingOutbox() error = nil, want error")
	}

	if got, want := err.Error(), "query pending outbox rows: boom"; got != want {
		t.Fatalf("ListPendingOutbox() error = %q, want %q", got, want)
	}
}

func TestListPendingOutboxWrapsScanError(t *testing.T) {
	db := newTestDB(t, testExpectation{
		kind:  expectationKindQuery,
		query: listPendingOutboxQuery,
		args:  []driver.Value{int64(1)},
		rows: &testRows{
			columns: []string{"id", "aggregate_id", "event_type", "payload", "created_at", "processed_at"},
			data: [][]driver.Value{
				{"row-1", "order-1", "order.created", []byte(`{"id":"order-1"}`), "not-a-time", nil},
			},
		},
	})

	_, err := ListPendingOutbox(context.Background(), db, 1)
	if err == nil {
		t.Fatal("ListPendingOutbox() error = nil, want error")
	}

	if !strings.Contains(err.Error(), "scan pending outbox row:") {
		t.Fatalf("ListPendingOutbox() error = %q", err.Error())
	}
}

func TestListPendingOutboxWrapsIterationError(t *testing.T) {
	db := newTestDB(t, testExpectation{
		kind:  expectationKindQuery,
		query: listPendingOutboxQuery,
		args:  []driver.Value{int64(1)},
		rows: &testRows{
			columns: []string{"id", "aggregate_id", "event_type", "payload", "created_at", "processed_at"},
			data: [][]driver.Value{
				{"row-1", "order-1", "order.created", []byte(`{"id":"order-1"}`), time.Now(), nil},
			},
			finalErr: errors.New("rows failed"),
		},
	})

	_, err := ListPendingOutbox(context.Background(), db, 1)
	if err == nil {
		t.Fatal("ListPendingOutbox() error = nil, want error")
	}

	if got, want := err.Error(), "iterate pending outbox rows: rows failed"; got != want {
		t.Fatalf("ListPendingOutbox() error = %q, want %q", got, want)
	}
}

func TestMarkOutboxProcessedSucceedsWhenOneRowIsUpdated(t *testing.T) {
	db := newTestDB(t, testExpectation{
		kind:   expectationKindExec,
		query:  markOutboxProcessedQuery,
		args:   []driver.Value{"row-1"},
		result: testResult{rowsAffected: 1},
	})

	err := MarkOutboxProcessed(context.Background(), db, "row-1")
	if err != nil {
		t.Fatalf("MarkOutboxProcessed() error = %v", err)
	}
}

func TestMarkOutboxProcessedWrapsExecError(t *testing.T) {
	db := newTestDB(t, testExpectation{
		kind:  expectationKindExec,
		query: markOutboxProcessedQuery,
		args:  []driver.Value{"row-1"},
		err:   errors.New("boom"),
	})

	err := MarkOutboxProcessed(context.Background(), db, "row-1")
	if err == nil {
		t.Fatal("MarkOutboxProcessed() error = nil, want error")
	}

	if got, want := err.Error(), "mark outbox row processed: boom"; got != want {
		t.Fatalf("MarkOutboxProcessed() error = %q, want %q", got, want)
	}
}

func TestMarkOutboxProcessedWrapsRowsAffectedError(t *testing.T) {
	db := newTestDB(t, testExpectation{
		kind:  expectationKindExec,
		query: markOutboxProcessedQuery,
		args:  []driver.Value{"row-1"},
		result: testResult{
			rowsAffectedErr: errors.New("rows affected failed"),
		},
	})

	err := MarkOutboxProcessed(context.Background(), db, "row-1")
	if err == nil {
		t.Fatal("MarkOutboxProcessed() error = nil, want error")
	}

	if got, want := err.Error(), "read rows affected for outbox update: rows affected failed"; got != want {
		t.Fatalf("MarkOutboxProcessed() error = %q, want %q", got, want)
	}
}

func TestMarkOutboxProcessedRejectsUnexpectedRowsAffected(t *testing.T) {
	db := newTestDB(t, testExpectation{
		kind:   expectationKindExec,
		query:  markOutboxProcessedQuery,
		args:   []driver.Value{"row-1"},
		result: testResult{rowsAffected: 0},
	})

	err := MarkOutboxProcessed(context.Background(), db, "row-1")
	if err == nil {
		t.Fatal("MarkOutboxProcessed() error = nil, want error")
	}

	if got, want := err.Error(), "expected to update 1 outbox row, updated 0"; got != want {
		t.Fatalf("MarkOutboxProcessed() error = %q, want %q", got, want)
	}
}

const (
	testDriverName = "dispatch-go-postgres-test-driver"

	countPendingOutboxQuery = `
		SELECT COUNT(*)
		FROM order_outbox
		WHERE processed_at IS NULL
	`

	listPendingOutboxQuery = `
  		SELECT id, aggregate_id, event_type, payload, created_at, processed_at
  		FROM order_outbox
  		WHERE processed_at IS NULL
  		ORDER BY created_at ASC
  		LIMIT $1
  	`

	markOutboxProcessedQuery = `
		UPDATE order_outbox
		SET processed_at = NOW()
		WHERE id = $1
		  AND processed_at IS NULL
	`
)

type expectationKind string

const (
	expectationKindExec  expectationKind = "exec"
	expectationKindQuery expectationKind = "query"
)

type testExpectation struct {
	kind   expectationKind
	query  string
	args   []driver.Value
	rows   *testRows
	result driver.Result
	err    error
}

type testRows struct {
	columns  []string
	data     [][]driver.Value
	finalErr error
	index    int
}

func (r *testRows) Columns() []string {
	return r.columns
}

func (r *testRows) Close() error {
	return nil
}

func (r *testRows) Next(dest []driver.Value) error {
	if r.index < len(r.data) {
		copy(dest, r.data[r.index])
		r.index++
		return nil
	}

	if r.finalErr != nil {
		err := r.finalErr
		r.finalErr = nil
		return err
	}

	return io.EOF
}

type testResult struct {
	rowsAffected    int64
	rowsAffectedErr error
}

func (r testResult) LastInsertId() (int64, error) {
	return 0, errors.New("not implemented")
}

func (r testResult) RowsAffected() (int64, error) {
	if r.rowsAffectedErr != nil {
		return 0, r.rowsAffectedErr
	}

	return r.rowsAffected, nil
}

type testDriver struct{}

type testConn struct {
	state *testState
}

type testState struct {
	mu           sync.Mutex
	expectations []testExpectation
}

var (
	registerTestDriverOnce sync.Once
	testStateCounter       uint64
	testStatesMu           sync.Mutex
	testStates             = map[string]*testState{}
)

func (d *testDriver) Open(name string) (driver.Conn, error) {
	testStatesMu.Lock()
	state := testStates[name]
	testStatesMu.Unlock()

	if state == nil {
		return nil, fmt.Errorf("no test state for %q", name)
	}

	return &testConn{state: state}, nil
}

func (c *testConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("not implemented")
}

func (c *testConn) Close() error {
	return nil
}

func (c *testConn) Begin() (driver.Tx, error) {
	return nil, errors.New("not implemented")
}

func (c *testConn) QueryContext(_ context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	expectation, err := c.state.pop(expectationKindQuery, query, args)
	if err != nil {
		return nil, err
	}

	if expectation.err != nil {
		return nil, expectation.err
	}

	return expectation.rows, nil
}

func (c *testConn) ExecContext(_ context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	expectation, err := c.state.pop(expectationKindExec, query, args)
	if err != nil {
		return nil, err
	}

	if expectation.err != nil {
		return nil, expectation.err
	}

	return expectation.result, nil
}

func (s *testState) pop(kind expectationKind, query string, args []driver.NamedValue) (testExpectation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.expectations) == 0 {
		return testExpectation{}, fmt.Errorf("unexpected %s: %s", kind, normalizeWhitespace(query))
	}

	expectation := s.expectations[0]
	s.expectations = s.expectations[1:]

	if expectation.kind != kind {
		return testExpectation{}, fmt.Errorf("operation kind = %s, want %s", kind, expectation.kind)
	}

	if got, want := normalizeWhitespace(query), normalizeWhitespace(expectation.query); got != want {
		return testExpectation{}, fmt.Errorf("query = %q, want %q", got, want)
	}

	if err := compareArgs(args, expectation.args); err != nil {
		return testExpectation{}, err
	}

	return expectation, nil
}

func (s *testState) remaining() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.expectations)
}

func newTestDB(t *testing.T, expectations ...testExpectation) *sql.DB {
	t.Helper()

	registerTestDriverOnce.Do(func() {
		sql.Register(testDriverName, &testDriver{})
	})

	dsn := fmt.Sprintf("dispatch-go-postgres-test-%d", atomic.AddUint64(&testStateCounter, 1))
	state := &testState{
		expectations: append([]testExpectation(nil), expectations...),
	}

	testStatesMu.Lock()
	testStates[dsn] = state
	testStatesMu.Unlock()

	db, err := sql.Open(testDriverName, dsn)
	if err != nil {
		t.Fatalf("sql.Open() error = %v", err)
	}

	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("db.Close() error = %v", err)
		}

		if remaining := state.remaining(); remaining != 0 {
			t.Errorf("unmet expectations = %d", remaining)
		}

		testStatesMu.Lock()
		delete(testStates, dsn)
		testStatesMu.Unlock()
	})

	return db
}

func compareArgs(actual []driver.NamedValue, expected []driver.Value) error {
	if len(actual) != len(expected) {
		return fmt.Errorf("arg count = %d, want %d", len(actual), len(expected))
	}

	for i := range actual {
		if actual[i].Ordinal != i+1 {
			return fmt.Errorf("arg ordinal = %d, want %d", actual[i].Ordinal, i+1)
		}

		if !driverValuesEqual(actual[i].Value, expected[i]) {
			return fmt.Errorf("arg %d = %#v, want %#v", i+1, actual[i].Value, expected[i])
		}
	}

	return nil
}

func driverValuesEqual(actual any, expected driver.Value) bool {
	actualBytes, actualIsBytes := actual.([]byte)
	expectedBytes, expectedIsBytes := expected.([]byte)
	if actualIsBytes || expectedIsBytes {
		return actualIsBytes && expectedIsBytes && string(actualBytes) == string(expectedBytes)
	}

	return actual == expected
}

func normalizeWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

var (
	_ driver.Driver         = (*testDriver)(nil)
	_ driver.Conn           = (*testConn)(nil)
	_ driver.ExecerContext  = (*testConn)(nil)
	_ driver.QueryerContext = (*testConn)(nil)
	_ driver.Rows           = (*testRows)(nil)
)
