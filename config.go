package postgresql

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/rs/zerolog/log"
)

// Config struct for postgresql config
type Config struct {
	Port                 int           `mapstructure:"port"`
	Host                 string        `mapstructure:"host"`
	User                 string        `mapstructure:"user"`
	Database             string        `mapstructure:"database"`
	Password             string        `mapstructure:"password"`
	Secured              bool          `mapstructure:"secured"`
	MaxConnections       int32         `mapstructure:"max_connections"`
	MinConnections       int32         `mapstructure:"min_connections"`
	MaxConnectionAge     time.Duration `mapstructure:"max_connection_age"`
	AcquireTimeout       time.Duration `mapstructure:"acquire_timeout"`
	HealthCheckPeriod    time.Duration `mapstructure:"health_check_period"`
	LoggerEnabled        bool          `mapstructure:"logger_enabled"`
	KeepAlive            time.Duration `mapstructure:"keep_alive"`
	Schema               string        `mapstructure:"schema"`
	PreferSimpleProtocol bool          `mapstructure:"prefer_simple_protocol"`
}

// ConnString return connection string
func (c Config) ConnString() string {
	return c.ConnStringFor(c.Host)
}

func (c Config) ConnStringFor(host string) string {
	connStr := fmt.Sprintf(
		"host=%s port=%d user=%s sslmode=disable",
		host, c.Port, c.User,
	)
	if c.Database != "" {
		connStr = connStr + " dbname=" + c.Database
	}
	if c.Password != "" {
		connStr = connStr + " password=" + c.Password
	}
	if c.Schema != "" {
		connStr = connStr + " search_path=" + c.Schema
	}
	if c.PreferSimpleProtocol {
		connStr += " prefer_simple_protocol=true"
	}
	return connStr
}

type wrappedDialer struct {
	*net.Dialer
}

func (d *wrappedDialer) DialContext(ctx context.Context, network, address string) (conn net.Conn, err error) {
	start := time.Now()
	defer func() {
		l := log.Debug().
			Str("component", "pgx").
			Dur("duration", time.Since(start)).
			Str("network", network).
			Str("address", address)
		if err != nil {
			l = l.Err(err)
		}
		l.Msg("dial postgres")
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
		l := log.Debug().
			Str("component", "pgx").
			Dur("duration", time.Since(start)).
			Str("host", host).
			Strs("resolved_addrs", addrs)
		if err != nil {
			l = l.Err(err)
		}
		l.Msg("resolve postgres host")
	}()
	addrs, err = r.Resolver.LookupHost(ctx, host)
	return
}

type ConnectionPoolOption func(*ConnectionPool) error

// NewConnectionPool return new Connection Pool
func NewConnectionPool(conf Config, opts ...ConnectionPoolOption) (DB, error) {
	poolConfig, err := pgxpool.ParseConfig(conf.ConnString())
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
		replicaPoolConfig, err := pgxpool.ParseConfig(conf.ConnString())
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
