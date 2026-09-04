package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/daobathanh/celesnity/core"
	_ "github.com/mattn/go-sqlite3"
)

type subscriber struct {
	ch     chan core.Event
	closed bool
}

type Store struct {
	db  *sql.DB
	mu  sync.RWMutex
	sub map[string][]*subscriber
}

func New(dbPath string) (*Store, error) {
	db, err := sql.Open("sqlite3", dbPath+"?_journal_mode=WAL&_busy_timeout=5000")
	if err != nil {
		return nil, fmt.Errorf("sqlite: open: %w", err)
	}
	if err := migrate(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("sqlite: migrate: %w", err)
	}
	return &Store{db: db, sub: make(map[string][]*subscriber)}, nil
}

func migrate(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS sessions (
			id TEXT PRIMARY KEY,
			model TEXT NOT NULL,
			system_prompt TEXT NOT NULL DEFAULT '',
			status TEXT NOT NULL DEFAULT 'active',
			created_at TIMESTAMP NOT NULL
		);
		CREATE TABLE IF NOT EXISTS runs (
			id TEXT PRIMARY KEY,
			session_id TEXT NOT NULL REFERENCES sessions(id),
			status TEXT NOT NULL,
			step_count INTEGER NOT NULL DEFAULT 0,
			tokens_used INTEGER NOT NULL DEFAULT 0,
			started_at TIMESTAMP NOT NULL,
			ended_at TIMESTAMP,
			error TEXT
		);
		CREATE TABLE IF NOT EXISTS events (
			session_id TEXT NOT NULL,
			seq INTEGER NOT NULL,
			run_id TEXT NOT NULL,
			type TEXT NOT NULL,
			payload TEXT NOT NULL DEFAULT '{}',
			at TIMESTAMP NOT NULL,
			PRIMARY KEY (session_id, seq)
		);
		CREATE INDEX IF NOT EXISTS idx_runs_session ON runs(session_id);
		CREATE INDEX IF NOT EXISTS idx_events_run ON events(run_id);
	`)
	return err
}

func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) SaveSession(ctx context.Context, sess *core.Session) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO sessions (id, model, system_prompt, status, created_at)
		VALUES (?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET status = excluded.status
	`, sess.ID, sess.Model, sess.SystemPrompt, sess.Status, sess.CreatedAt)
	return err
}

func (s *Store) GetSession(ctx context.Context, id string) (*core.Session, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, model, system_prompt, status, created_at FROM sessions WHERE id = ?`, id)
	var sess core.Session
	if err := row.Scan(&sess.ID, &sess.Model, &sess.SystemPrompt, &sess.Status, &sess.CreatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("session %q not found", id)
		}
		return nil, err
	}
	return &sess, nil
}

func (s *Store) SaveRun(ctx context.Context, run *core.Run) error {
	errJSON := sql.NullString{}
	if run.Error != nil {
		data, _ := json.Marshal(run.Error)
		errJSON = sql.NullString{String: string(data), Valid: true}
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO runs (id, session_id, status, step_count, tokens_used, started_at, ended_at, error)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(id) DO UPDATE SET
			status = excluded.status,
			step_count = excluded.step_count,
			tokens_used = excluded.tokens_used,
			ended_at = excluded.ended_at,
			error = excluded.error
	`, run.ID, run.SessionID, run.Status, run.StepCount, run.TokensUsed, run.StartedAt, run.EndedAt, errJSON)
	return err
}

func (s *Store) GetRun(ctx context.Context, id string) (*core.Run, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, session_id, status, step_count, tokens_used, started_at, ended_at, error FROM runs WHERE id = ?`, id)
	return s.scanRun(row)
}

func (s *Store) GetActiveRun(ctx context.Context, sessionID string) (*core.Run, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, session_id, status, step_count, tokens_used, started_at, ended_at, error FROM runs WHERE session_id = ? AND status = 'running'`, sessionID)
	run, err := s.scanRun(row)
	if err != nil && err.Error() == fmt.Sprintf("run not found") {
		return nil, nil
	}
	return run, err
}

func (s *Store) scanRun(row *sql.Row) (*core.Run, error) {
	var run core.Run
	var endedAt sql.NullTime
	var errJSON sql.NullString
	if err := row.Scan(&run.ID, &run.SessionID, &run.Status, &run.StepCount, &run.TokensUsed, &run.StartedAt, &endedAt, &errJSON); err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("run not found")
		}
		return nil, err
	}
	if endedAt.Valid {
		run.EndedAt = &endedAt.Time
	}
	if errJSON.Valid {
		var runErr core.RunError
		json.Unmarshal([]byte(errJSON.String), &runErr)
		run.Error = &runErr
	}
	return &run, nil
}

func (s *Store) Append(ctx context.Context, event core.Event) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO events (session_id, seq, run_id, type, payload, at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, event.SessionID, event.Seq, event.RunID, event.Type, string(event.Payload), event.At)
	if err != nil {
		return err
	}
	s.broadcast(event)
	return nil
}

func (s *Store) AppendAndUpdateRun(ctx context.Context, event core.Event, run *core.Run) error {
	errJSON := sql.NullString{}
	if run.Error != nil {
		data, _ := json.Marshal(run.Error)
		errJSON = sql.NullString{String: string(data), Valid: true}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	_, err = tx.ExecContext(ctx, `
		INSERT INTO events (session_id, seq, run_id, type, payload, at)
		VALUES (?, ?, ?, ?, ?, ?)
	`, event.SessionID, event.Seq, event.RunID, event.Type, string(event.Payload), event.At)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE runs SET status = ?, step_count = ?, tokens_used = ?, ended_at = ?, error = ?
		WHERE id = ?
	`, run.Status, run.StepCount, run.TokensUsed, run.EndedAt, errJSON, run.ID)
	if err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	s.broadcast(event)
	return nil
}

func (s *Store) LoadFrom(ctx context.Context, sessionID string, fromSeq int64) ([]core.Event, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT session_id, seq, run_id, type, payload, at
		FROM events WHERE session_id = ? AND seq >= ? ORDER BY seq
	`, sessionID, fromSeq)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []core.Event
	for rows.Next() {
		var e core.Event
		var payload string
		if err := rows.Scan(&e.SessionID, &e.Seq, &e.RunID, &e.Type, &payload, &e.At); err != nil {
			return nil, err
		}
		e.Payload = json.RawMessage(payload)
		events = append(events, e)
	}
	return events, rows.Err()
}

func (s *Store) NextSeq(ctx context.Context, sessionID string) (int64, error) {
	var maxSeq sql.NullInt64
	err := s.db.QueryRowContext(ctx, `SELECT MAX(seq) FROM events WHERE session_id = ?`, sessionID).Scan(&maxSeq)
	if err != nil {
		return 0, err
	}
	if !maxSeq.Valid {
		return 1, nil
	}
	return maxSeq.Int64 + 1, nil
}

func (s *Store) Subscribe(sessionID string) (<-chan core.Event, func()) {
	sub := &subscriber{ch: make(chan core.Event, 64)}
	s.mu.Lock()
	s.sub[sessionID] = append(s.sub[sessionID], sub)
	s.mu.Unlock()

	unsub := func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		if sub.closed {
			return
		}
		sub.closed = true
		close(sub.ch)
		subs := s.sub[sessionID]
		for i, ss := range subs {
			if ss == sub {
				s.sub[sessionID] = append(subs[:i], subs[i+1:]...)
				break
			}
		}
	}
	return sub.ch, unsub
}

func (s *Store) broadcast(event core.Event) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	for _, sub := range s.sub[event.SessionID] {
		if sub.closed {
			continue
		}
		select {
		case sub.ch <- event:
		default:
		}
	}
}

func (s *Store) MarkInterruptedRuns(ctx context.Context) (int64, error) {
	now := time.Now()
	res, err := s.db.ExecContext(ctx, `
		UPDATE runs SET status = 'interrupted', ended_at = ? WHERE status = 'running'
	`, now)
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}
