package postgresql

import (
	"context"
	"sync/atomic"

	"github.com/jackc/pgx/v5/pgxpool"
)

type ReplicaSet interface {
	Replica(ctx context.Context) *pgxpool.Pool
	Close()
}

type RoundRobinReplicaSet struct {
	replicas []*pgxpool.Pool
	current  int32
}

func (rs *RoundRobinReplicaSet) Close() {
	for _, replica := range rs.replicas {
		replica.Close()
	}
}

func NewRoundRobinReplicaSet(replicas []*pgxpool.Pool) *RoundRobinReplicaSet {
	return &RoundRobinReplicaSet{
		replicas: replicas,
	}
}

func (rs *RoundRobinReplicaSet) Replica(ctx context.Context) *pgxpool.Pool {
	if len(rs.replicas) == 1 {
		return rs.replicas[0]
	}

	current := atomic.AddInt32(&rs.current, 1)
	idx := int(current) % len(rs.replicas)

	return rs.replicas[idx]
}

func WithRoundRobinReplicaSet(replicaConfigs ...Config) ConnectionPoolOption {

	return func(connectionPool *ConnectionPool) error {
		if len(replicaConfigs) == 0 {
			connectionPool.ReplicaSet = NewRoundRobinReplicaSet([]*pgxpool.Pool{connectionPool.Pool})

			return nil
		}

		replicas, err := ReplicaPoolsFromConfig(replicaConfigs...)

		if err != nil {
			return err
		}

		connectionPool.ReplicaSet = NewRoundRobinReplicaSet(replicas)

		return nil
	}
}
