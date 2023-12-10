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
	Exec(sql string, args ...interface{}) (commandTag pgconn.CommandTag, err error)
	ExecCtx(ctx context.Context, sql string, args ...interface{}) (commandTag pgconn.CommandTag, err error)
	Query(sql string, args ...interface{}) (pgx.Rows, error)
	QueryCtx(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	QueryRow(sql string, args ...interface{}) pgx.Row
	QueryRowCtx(ctx context.Context, sql string, args ...interface{}) pgx.Row
	ReplicaQuery(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error)
	ReplicaQueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row
	ReplicaSendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
	Begin() (*Transaction, error)
	BeginCtx(ctx context.Context) (*Transaction, error)
	RunTx(fn func(tx *Transaction) error) error
	Statistics() *pgxpool.Stat
	Close() error
	SendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults
}

type errRow struct {
	error
}

func (er errRow) Scan(...interface{}) error {
	return er.error
}

// ConnectionPool is struct with connection pool
type ConnectionPool struct {
	*pgxpool.Pool
	ReplicaSet ReplicaSet
}

// Exec sql
func (p *ConnectionPool) Exec(sql string, args ...interface{}) (commandTag pgconn.CommandTag, err error) {
	return p.ExecCtx(context.Background(), sql, args...)
}

// Query sql
func (p *ConnectionPool) Query(sql string, args ...interface{}) (pgx.Rows, error) {
	return p.QueryCtx(context.Background(), sql, args...)
}

// QueryRow sql
func (p *ConnectionPool) QueryRow(sql string, args ...interface{}) pgx.Row {
	return p.QueryRowCtx(context.Background(), sql, args...)
}

// ExecCtx exec sql with context
func (p *ConnectionPool) ExecCtx(ctx context.Context, sql string, arguments ...interface{}) (commandTag pgconn.CommandTag, err error) {
	return p.Pool.Exec(ctx, sql, arguments...)
}

// QueryCtx query sql with context
func (p *ConnectionPool) QueryCtx(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return p.Pool.Query(ctx, sql, args...)
}

// QueryRowCtx query row with context
func (p *ConnectionPool) QueryRowCtx(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	return p.Pool.QueryRow(ctx, sql, args...)
}

// ReplicaQuery query sql on replica with context
func (p *ConnectionPool) ReplicaQuery(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return p.ReplicaSet.Replica(ctx).Query(ctx, sql, args...)
}

// ReplicaQueryRow query row on replica with context
func (p *ConnectionPool) ReplicaQueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	return p.ReplicaSet.Replica(ctx).QueryRow(ctx, sql, args...)
}

// ReplicaSendBatch send pgx batch on replica
func (p *ConnectionPool) ReplicaSendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return p.ReplicaSet.Replica(ctx).SendBatch(ctx, b)
}

// Begin return new transaction
func (p *ConnectionPool) Begin() (*Transaction, error) {
	return p.BeginCtx(context.Background())
}

// BeginCtx return new transaction with context
func (p *ConnectionPool) BeginCtx(ctx context.Context) (*Transaction, error) {
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
func (p *ConnectionPool) RunTx(fn func(tx *Transaction) error) error {
	var err error
	tx, err := p.Begin()
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
func (t *Transaction) Exec(sql string, arguments ...interface{}) (commandTag pgconn.CommandTag, err error) {
	return t.ExecCtx(context.Background(), sql, arguments...)
}

// ExecCtx exec sql with context
func (t *Transaction) ExecCtx(ctx context.Context, sql string, arguments ...interface{}) (commandTag pgconn.CommandTag, err error) {
	return t.Tx.Exec(ctx, sql, arguments...)
}

// Query sql
func (t *Transaction) Query(sql string, args ...interface{}) (pgx.Rows, error) {
	return t.QueryCtx(context.Background(), sql, args...)
}

// QueryCtx query sql with context
func (t *Transaction) QueryCtx(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return t.Tx.Query(ctx, sql, args...)
}

// QueryRow sql
func (t *Transaction) QueryRow(sql string, args ...interface{}) pgx.Row {
	return t.QueryRowCtx(context.Background(), sql, args...)
}

// QueryRowCtx query row with context
func (t *Transaction) QueryRowCtx(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	return t.Tx.QueryRow(ctx, sql, args...)
}

func (t *Transaction) ReplicaQuery(ctx context.Context, sql string, args ...interface{}) (pgx.Rows, error) {
	return t.QueryCtx(ctx, sql, args...)
}

func (t *Transaction) ReplicaQueryRow(ctx context.Context, sql string, args ...interface{}) pgx.Row {
	return t.QueryRowCtx(ctx, sql, args...)
}


func (t *Transaction) ReplicaSendBatch(ctx context.Context, b *pgx.Batch) pgx.BatchResults {
	return t.SendBatch(ctx, b)
}

// Begin create savepoint
func (t *Transaction) Begin() (*Transaction, error) {
	return t.BeginCtx(context.Background())
}

// BeginCtx create savepoint with context
func (t *Transaction) BeginCtx(ctx context.Context) (*Transaction, error) {
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
func (t *Transaction) RunTx(fn func(tx *Transaction) error) error {
	tx, err := t.Begin()
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
