package sqlite

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/daobathanh/celesnity/core"
	_ "github.com/mattn/go-sqlite3"
)

func tempStore(t *testing.T) *Store {
	t.Helper()
	dir := t.TempDir()
	s, err := New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("create store: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func TestSaveAndGetSession(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	sess := &core.Session{
		ID:        "s1",
		Model:     "test-model",
		Status:    core.SessionActive,
		CreatedAt: time.Now(),
	}
	if err := s.SaveSession(ctx, sess); err != nil {
		t.Fatalf("save: %v", err)
	}

	got, err := s.GetSession(ctx, "s1")
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if got.Model != "test-model" || got.Status != core.SessionActive {
		t.Fatalf("unexpected session: %+v", got)
	}
}

func TestGetSessionNotFound(t *testing.T) {
	s := tempStore(t)
	_, err := s.GetSession(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestSessionStatusUpdate(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	sess := &core.Session{ID: "s1", Model: "m", Status: core.SessionActive, CreatedAt: time.Now()}
	s.SaveSession(ctx, sess)

	sess.Status = core.SessionClosed
	s.SaveSession(ctx, sess)

	got, _ := s.GetSession(ctx, "s1")
	if got.Status != core.SessionClosed {
		t.Fatalf("expected closed, got %s", got.Status)
	}
}

func TestSaveAndGetRun(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	sess := &core.Session{ID: "s1", Model: "m", Status: core.SessionActive, CreatedAt: time.Now()}
	s.SaveSession(ctx, sess)

	run := &core.Run{
		ID:        "r1",
		SessionID: "s1",
		Status:    core.RunRunning,
		StartedAt: time.Now(),
	}
	if err := s.SaveRun(ctx, run); err != nil {
		t.Fatalf("save run: %v", err)
	}

	got, err := s.GetRun(ctx, "r1")
	if err != nil {
		t.Fatalf("get run: %v", err)
	}
	if got.Status != core.RunRunning {
		t.Fatalf("expected running, got %s", got.Status)
	}
}

func TestGetRunNotFound(t *testing.T) {
	s := tempStore(t)
	_, err := s.GetRun(context.Background(), "nonexistent")
	if err == nil {
		t.Fatal("expected error")
	}
}

func TestGetActiveRun(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	sess := &core.Session{ID: "s1", Model: "m", Status: core.SessionActive, CreatedAt: time.Now()}
	s.SaveSession(ctx, sess)

	run := &core.Run{ID: "r1", SessionID: "s1", Status: core.RunRunning, StartedAt: time.Now()}
	s.SaveRun(ctx, run)

	active, err := s.GetActiveRun(ctx, "s1")
	if err != nil {
		t.Fatalf("get active: %v", err)
	}
	if active == nil || active.ID != "r1" {
		t.Fatal("expected active run r1")
	}

	now := time.Now()
	run.Status = core.RunSucceeded
	run.EndedAt = &now
	s.SaveRun(ctx, run)

	active, err = s.GetActiveRun(ctx, "s1")
	if err != nil {
		t.Fatalf("get active after finish: %v", err)
	}
	if active != nil {
		t.Fatal("expected no active run after completion")
	}
}

func TestRunWithError(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	sess := &core.Session{ID: "s1", Model: "m", Status: core.SessionActive, CreatedAt: time.Now()}
	s.SaveSession(ctx, sess)

	now := time.Now()
	run := &core.Run{
		ID:        "r1",
		SessionID: "s1",
		Status:    core.RunFailed,
		StartedAt: time.Now(),
		EndedAt:   &now,
		Error:     &core.RunError{Code: "budget_exceeded", Message: "too many steps"},
	}
	s.SaveRun(ctx, run)

	got, _ := s.GetRun(ctx, "r1")
	if got.Error == nil {
		t.Fatal("expected error in run")
	}
	if got.Error.Code != "budget_exceeded" {
		t.Fatalf("expected budget_exceeded, got %s", got.Error.Code)
	}
}

func TestAppendAndLoadEvents(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	for i := 0; i < 5; i++ {
		s.Append(ctx, core.Event{
			SessionID: "s1",
			RunID:     "r1",
			Type:      core.EventModelRequest,
			Payload:   core.MustMarshalPayload(map[string]string{"i": "test"}),
			At:        time.Now(),
		})
	}

	events, err := s.LoadFrom(ctx, "s1", 0)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if len(events) != 5 {
		t.Fatalf("expected 5 events, got %d", len(events))
	}

	events, _ = s.LoadFrom(ctx, "s1", 3)
	if len(events) != 3 {
		t.Fatalf("expected 3 events from seq 3, got %d", len(events))
	}
}


func TestAppendAndUpdateRunAtomic(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	sess := &core.Session{ID: "s1", Model: "m", Status: core.SessionActive, CreatedAt: time.Now()}
	s.SaveSession(ctx, sess)

	now := time.Now()
	run := &core.Run{ID: "r1", SessionID: "s1", Status: core.RunRunning, StartedAt: time.Now()}
	s.SaveRun(ctx, run)

	run.Status = core.RunSucceeded
	run.EndedAt = &now
	event := core.Event{
		SessionID: "s1", RunID: "r1",
		Type: core.EventRunSucceeded, Payload: []byte("{}"), At: now,
	}

	if err := s.AppendAndUpdateRun(ctx, event, run); err != nil {
		t.Fatalf("atomic save: %v", err)
	}

	got, _ := s.GetRun(ctx, "r1")
	if got.Status != core.RunSucceeded {
		t.Fatalf("expected succeeded, got %s", got.Status)
	}

	events, _ := s.LoadFrom(ctx, "s1", 0)
	if len(events) != 1 || events[0].Type != core.EventRunSucceeded {
		t.Fatal("expected run_succeeded event")
	}
}

func TestMarkInterruptedRuns(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	s.SaveSession(ctx, &core.Session{ID: "s1", Model: "m", Status: core.SessionActive, CreatedAt: time.Now()})
	s.SaveSession(ctx, &core.Session{ID: "s2", Model: "m", Status: core.SessionActive, CreatedAt: time.Now()})

	s.SaveRun(ctx, &core.Run{ID: "r1", SessionID: "s1", Status: core.RunRunning, StartedAt: time.Now()})
	s.SaveRun(ctx, &core.Run{ID: "r2", SessionID: "s2", Status: core.RunRunning, StartedAt: time.Now()})
	now := time.Now()
	s.SaveRun(ctx, &core.Run{ID: "r3", SessionID: "s1", Status: core.RunSucceeded, StartedAt: time.Now(), EndedAt: &now})

	count, err := s.MarkInterruptedRuns(ctx)
	if err != nil {
		t.Fatalf("mark: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 interrupted, got %d", count)
	}

	r1, _ := s.GetRun(ctx, "r1")
	r2, _ := s.GetRun(ctx, "r2")
	r3, _ := s.GetRun(ctx, "r3")

	if r1.Status != core.RunInterrupted || r2.Status != core.RunInterrupted {
		t.Fatal("running runs should be interrupted")
	}
	if r3.Status != core.RunSucceeded {
		t.Fatal("succeeded run should be unchanged")
	}

	events1, _ := s.LoadFrom(ctx, "s1", 0)
	events2, _ := s.LoadFrom(ctx, "s2", 0)
	if len(events1) != 1 || events1[0].Type != core.EventRunInterrupted {
		t.Fatal("expected run_interrupted event for s1")
	}
	if len(events2) != 1 || events2[0].Type != core.EventRunInterrupted {
		t.Fatal("expected run_interrupted event for s2")
	}
}

func TestSubscribeAndBroadcast(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	ch, unsub := s.Subscribe("s1")
	defer unsub()

	evt := core.Event{
		SessionID: "s1", RunID: "r1",
		Type: core.EventRunStarted, Payload: []byte("{}"), At: time.Now(),
	}
	s.Append(ctx, evt)

	select {
	case got := <-ch:
		if got.Type != core.EventRunStarted {
			t.Fatalf("expected run_started, got %s", got.Type)
		}
	case <-time.After(time.Second):
		t.Fatal("timeout waiting for broadcast")
	}
}

func TestUnsubscribeStopsBroadcast(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	ch, unsub := s.Subscribe("s1")
	unsub()

	s.Append(ctx, core.Event{
		SessionID: "s1", RunID: "r1",
		Type: core.EventRunStarted, Payload: []byte("{}"), At: time.Now(),
	})

	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("should not receive events after unsubscribe")
		}
	case <-time.After(100 * time.Millisecond):
	}
}

func TestConcurrentBroadcastAndUnsubscribe(t *testing.T) {
	s := tempStore(t)
	ctx := context.Background()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, unsub := s.Subscribe("s1")
			time.Sleep(time.Millisecond)
			unsub()
		}()
	}

	for i := 0; i < 20; i++ {
		s.Append(ctx, core.Event{
			SessionID: "s1", RunID: "r1",
			Type: core.EventRunStarted, Payload: []byte("{}"), At: time.Now(),
		})
	}

	wg.Wait()
}

func TestWALMode(t *testing.T) {
	dir := t.TempDir()
	s, err := New(filepath.Join(dir, "test.db"))
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	defer s.Close()

	var mode string
	err = s.db.QueryRow("PRAGMA journal_mode").Scan(&mode)
	if err != nil {
		t.Fatalf("query journal_mode: %v", err)
	}
	if mode != "wal" {
		t.Fatalf("expected WAL mode, got %s", mode)
	}
}

func TestDatabasePersistsAcrossReopen(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "test.db")

	s1, _ := New(dbPath)
	ctx := context.Background()
	s1.SaveSession(ctx, &core.Session{ID: "s1", Model: "m", Status: core.SessionActive, CreatedAt: time.Now()})
	s1.Append(ctx, core.Event{SessionID: "s1", RunID: "r1", Type: core.EventRunStarted, Payload: []byte("{}"), At: time.Now()})
	s1.Close()

	s2, err := New(dbPath)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer s2.Close()

	sess, err := s2.GetSession(ctx, "s1")
	if err != nil {
		t.Fatalf("session lost after reopen: %v", err)
	}
	if sess.ID != "s1" {
		t.Fatal("wrong session")
	}

	events, _ := s2.LoadFrom(ctx, "s1", 0)
	if len(events) != 1 {
		t.Fatalf("expected 1 event after reopen, got %d", len(events))
	}

	_ = os.Remove(dbPath)
}

func TestMigrateOldDBWithDuplicateRunningRuns(t *testing.T) {
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "old.db")

	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("open raw db: %v", err)
	}
	_, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS sessions (
			id TEXT PRIMARY KEY, model TEXT NOT NULL,
			system_prompt TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL DEFAULT 'active',
			created_at TIMESTAMP NOT NULL
		);
		CREATE TABLE IF NOT EXISTS runs (
			id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES sessions(id),
			status TEXT NOT NULL, step_count INTEGER NOT NULL DEFAULT 0,
			tokens_used INTEGER NOT NULL DEFAULT 0,
			started_at TIMESTAMP NOT NULL, ended_at TIMESTAMP, error TEXT
		);
		CREATE TABLE IF NOT EXISTS events (
			session_id TEXT NOT NULL, seq INTEGER NOT NULL,
			run_id TEXT NOT NULL, type TEXT NOT NULL,
			payload TEXT NOT NULL DEFAULT '{}', at TIMESTAMP NOT NULL,
			PRIMARY KEY (session_id, seq)
		);
	`)
	if err != nil {
		t.Fatalf("create tables: %v", err)
	}

	now := time.Now()
	db.Exec(`INSERT INTO sessions (id, model, status, created_at) VALUES ('s1', 'm', 'active', ?)`, now)
	db.Exec(`INSERT INTO runs (id, session_id, status, started_at) VALUES ('r1', 's1', 'running', ?)`, now)
	db.Exec(`INSERT INTO runs (id, session_id, status, started_at) VALUES ('r2', 's1', 'running', ?)`, now)
	db.Close()

	store, err := New(dbPath)
	if err != nil {
		t.Fatalf("New() should succeed on old DB with duplicate running runs, got: %v", err)
	}
	defer store.Close()

	ctx := context.Background()
	r1, _ := store.GetRun(ctx, "r1")
	r2, _ := store.GetRun(ctx, "r2")

	// migrate() only cleans duplicates: keeps MIN(id) running for MarkInterruptedRuns
	if r1.Status != core.RunRunning {
		t.Fatalf("r1 (MIN id): expected running after migrate, got %s", r1.Status)
	}
	if r2.Status != core.RunInterrupted {
		t.Fatalf("r2 (duplicate): expected interrupted after migrate, got %s", r2.Status)
	}

	// MarkInterruptedRuns handles the remaining one with proper event emission
	count, err := store.MarkInterruptedRuns(ctx)
	if err != nil {
		t.Fatalf("MarkInterruptedRuns: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 interrupted run, got %d", count)
	}
	r1, _ = store.GetRun(ctx, "r1")
	if r1.Status != core.RunInterrupted {
		t.Fatalf("r1 after MarkInterruptedRuns: expected interrupted, got %s", r1.Status)
	}

	events, _ := store.LoadFrom(ctx, "s1", 0)
	found := false
	for _, e := range events {
		if e.Type == "run_interrupted" && e.RunID == "r1" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected run_interrupted event for r1")
	}
}
