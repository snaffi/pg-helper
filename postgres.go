package postgresql

import (
	"context"
	"fmt"
	"sync"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/snaffi/errors"
)

// DB interface for work with DB
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (commandTag pgconn.CommandTag, err error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	ReplicaQuery(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	ReplicaQueryRow(ctx context.Context, sql string, args ...any) pgx.Row
	ReplicaSendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
	Begin(ctx context.Context) (*Transaction, error)
	RunTx(ctx context.Context, fn func(tx *Transaction) error) error
	Statistics() *pgxpool.Stat
	Close() error
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

type errRow struct {
	error
}

func (er errRow) Scan(...any) error {
	return er.error
}

// ConnectionPool is struct with connection pool
type ConnectionPool struct {
	*pgxpool.Pool
	ReplicaSet ReplicaSet
}

// Query sql
func (p *ConnectionPool) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return p.Pool.Query(ctx, sql, args...)
}

// QueryRow sql
func (p *ConnectionPool) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return p.Pool.QueryRow(ctx, sql, args...)
}

// Exec  sql with context
func (p *ConnectionPool) Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error) {
	return p.Pool.Exec(ctx, sql, arguments...)
}

// ReplicaQuery query sql on replica with context
func (p *ConnectionPool) ReplicaQuery(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return p.ReplicaSet.Replica(ctx).Query(ctx, sql, args...)
}

// ReplicaQueryRow query row on replica with context
func (p *ConnectionPool) ReplicaQueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return p.ReplicaSet.Replica(ctx).QueryRow(ctx, sql, args...)
}

// ReplicaSendBatch send pgx batch on replica
func (p *ConnectionPool) ReplicaSendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return p.ReplicaSet.Replica(ctx).SendBatch(ctx, b)
}

// Begin return new transaction with context
func (p *ConnectionPool) Begin(ctx context.Context) (*Transaction, error) {
	tx, err := p.Pool.Begin(ctx)
	if err != nil {
		return nil, errors.Wrap("create transaction", err)
	}
	return &Transaction{
		Tx: tx,
		mu: &sync.Mutex{},
	}, nil
}

// RunTx exec sql with transaction
func (p *ConnectionPool) RunTx(ctx context.Context, fn func(tx *Transaction) error) error {
	var err error
	tx, err := p.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		p := recover()
		switch {
		case p != nil:
			// a panic occurred, rollback and repanic
			_ = tx.Rollback()
			panic(p)
		case err != nil:
			// something went wrong, rollback
			_ = tx.Rollback()
		default:
			// all good, commit
			err = tx.Commit()
		}
	}()
	err = fn(tx)
	return err
}

func (p *ConnectionPool) Statistics() *pgxpool.Stat {
	return p.Stat()
}

// Close ...
func (p *ConnectionPool) Close() error {
	p.Pool.Close()
	if p.ReplicaSet != nil {
		p.ReplicaSet.Close()
	}
	return nil
}

// Transaction ...
type Transaction struct {
	pgx.Tx
	mu                *sync.Mutex
	savePointSequence uint8
}

// Exec sql

// Exec sql with context
func (t *Transaction) Exec(ctx context.Context, sql string, arguments ...any) (commandTag pgconn.CommandTag, err error) {
	return t.Tx.Exec(ctx, sql, arguments...)
}

// Query sql with context
func (t *Transaction) Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return t.Tx.Query(ctx, sql, args...)
}

// QueryRow query row with context
func (t *Transaction) QueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return t.Tx.QueryRow(ctx, sql, args...)
}

func (t *Transaction) ReplicaQuery(ctx context.Context, sql string, args ...any) (pgx.Rows, error) {
	return t.Query(ctx, sql, args...)
}

func (t *Transaction) ReplicaQueryRow(ctx context.Context, sql string, args ...any) pgx.Row {
	return t.QueryRow(ctx, sql, args...)
}

func (t *Transaction) ReplicaSendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return t.SendBatch(ctx, b)
}

// Begin create savepoint with context
func (t *Transaction) Begin(ctx context.Context) (*Transaction, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.savePointSequence++
	sql := fmt.Sprintf("SAVEPOINT savepoint_%d", t.savePointSequence)
	_, err := t.Tx.Exec(ctx, sql)
	if err != nil {
		return nil, errors.Wrap("create savepoint", err)
	}
	return t, nil
}

// Rollback transaction or rollback to savepoint
func (t *Transaction) Rollback() error {
	return t.RollbackCtx(context.Background())
}

// RollbackCtx transaction or rollback to savepoint with context
func (t *Transaction) RollbackCtx(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.savePointSequence == 0 {
		return t.Tx.Rollback(context.Background())
	}

	sql := fmt.Sprintf("ROLLBACK TO SAVEPOINT savepoint_%d", t.savePointSequence)
	_, err := t.Tx.Exec(ctx, sql)
	t.savePointSequence--
	if err != nil {
		return errors.Wrap("rollback to savepoint", err)
	}
	return nil
}

// Commit transaction
func (t *Transaction) Commit() error {
	return t.CommitCtx(context.Background())
}

// CommitCtx transaction with context
func (t *Transaction) CommitCtx(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	if t.savePointSequence == 0 {
		return t.Tx.Commit(ctx)
	}

	t.savePointSequence--
	return nil
}

// RunTx exec sql with transaction
func (t *Transaction) RunTx(ctx context.Context, fn func(tx *Transaction) error) error {
	tx, err := t.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		p := recover()
		switch {
		case p != nil:
			// a panic occurred, rollback and repanic
			_ = tx.Rollback()
			panic(p)
		case err != nil:
			// something went wrong, rollback
			_ = tx.Rollback()
		default:
			// all good, commit
			err = tx.Commit()
		}
	}()
	err = fn(tx)
	return err
}

// Close ...
func (t *Transaction) Close() error {
	return t.CloseCtx(context.Background())
}

// CloseCtx with context
func (t *Transaction) CloseCtx(ctx context.Context) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	_ = t.Tx.Rollback(ctx)
	return nil
}

func (t *Transaction) Statistics() *pgxpool.Stat {
	return nil
}
