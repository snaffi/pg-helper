package postgresql

import (
	"context"
	"log/slog"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// Config struct for postgresql config
type Config struct {
	DSN               string        `mapstructure:"dsn"`
	MaxConnections    int32         `mapstructure:"max_connections"`
	MinConnections    int32         `mapstructure:"min_connections"`
	MaxConnectionAge  time.Duration `mapstructure:"max_connection_age"`
	AcquireTimeout    time.Duration `mapstructure:"acquire_timeout"`
	HealthCheckPeriod time.Duration `mapstructure:"health_check_period"`
	LoggerEnabled     bool          `mapstructure:"logger_enabled"`
	KeepAlive         time.Duration `mapstructure:"keep_alive"`
}

type wrappedDialer struct {
	*net.Dialer
}

func (d *wrappedDialer) DialContext(ctx context.Context, network, address string) (conn net.Conn, err error) {
	start := time.Now()
	defer func() {

		if err != nil {
			slog.ErrorContext(
				ctx, "dial postgres",
				"component", "pgx",
				"network", network,
				"address", address,
				"duration", time.Since(start),
				"error", err,
			)
		} else {
			slog.DebugContext(
				ctx, "dial postgres",
				"component", "pgx",
				"network", network,
				"address", address,
				"duration", time.Since(start),
			)
		}
	}()
	conn, err = d.Dialer.DialContext(ctx, network, address)
	return
}

type wrappedResolver struct {
	*net.Resolver
}

func (r *wrappedResolver) LookupHost(ctx context.Context, host string) (addrs []string, err error) {
	start := time.Now()
	defer func() {
		if err != nil {
			slog.ErrorContext(
				ctx, "resolve postgres host",
				"component", "pgx",
				"host", host,
				"resolved_addresses", addrs,
				"duration", time.Since(start),
				"error", err,
			)
		} else {
			slog.DebugContext(
				ctx, "resolve postgres host",
				"component", "pgx",
				"host", host,
				"resolved_addrs", addrs,
				"duration", time.Since(start),
			)
		}
	}()
	addrs, err = r.Resolver.LookupHost(ctx, host)
	return
}

type ConnectionPoolOption func(*ConnectionPool) error

// NewConnectionPool return new Connection Pool
func NewConnectionPool(conf Config, opts ...ConnectionPoolOption) (DB, error) {
	poolConfig, err := pgxpool.ParseConfig(conf.DSN)
	if err != nil {
		return nil, err
	}

	conf.ApplyPoolConfig(poolConfig)

	p, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, err
	}

	connectionPool := &ConnectionPool{Pool: p}

	for _, opt := range opts {
		err := opt(connectionPool)
		if err != nil {
			return nil, err
		}
	}

	return connectionPool, nil

}

func (c Config) ApplyPoolConfig(poolConfig *pgxpool.Config) {
	poolConfig.MaxConns = c.MaxConnections
	poolConfig.MinConns = c.MinConnections
	poolConfig.MaxConnLifetime = c.MaxConnectionAge
	poolConfig.HealthCheckPeriod = c.HealthCheckPeriod
	if c.LoggerEnabled {
		dialer := &wrappedDialer{
			Dialer: &net.Dialer{
				KeepAlive: c.KeepAlive,
			},
		}
		resolver := &wrappedResolver{
			Resolver: net.DefaultResolver,
		}
		poolConfig.ConnConfig.DialFunc = dialer.DialContext
		poolConfig.ConnConfig.LookupFunc = resolver.LookupHost
	}
}

func ReplicaPoolsFromConfig(replicaConfigs ...Config) ([]*pgxpool.Pool, error) {
	var replicas []*pgxpool.Pool
	for _, conf := range replicaConfigs {
		replicaPoolConfig, err := pgxpool.ParseConfig(conf.DSN)
		if err != nil {
			return nil, err
		}

		conf.ApplyPoolConfig(replicaPoolConfig)

		replica, err := pgxpool.NewWithConfig(context.Background(), replicaPoolConfig)
		if err != nil {
			return nil, err
		}

		replicas = append(replicas, replica)
	}

	return replicas, nil
}
