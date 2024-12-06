package gh

import (
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"strings"
	"time"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

var (
	// ErrEmptyDSN is returned when the DSN string is empty
	ErrEmptyDSN = errors.New("DSN is empty")

	// ErrInvalidDSN is returned when the DSN cannot be parsed
	ErrInvalidDSN = errors.New("DSN is invalid")

	// ErrMissingRequiredField is returned when a required DSN field is missing
	ErrMissingRequiredField = errors.New("missing required DSN field")
)

// ConnectionPoolConfig allows customization of database connection pool settings
type ConnectionPoolConfig struct {
	MaxIdleConns    int
	MaxOpenConns    int
	ConnMaxLifetime time.Duration
	ConnMaxIdleTime time.Duration
}

// DefaultConnectionPoolConfig provides sensible default connection pool settings
func DefaultConnectionPoolConfig() ConnectionPoolConfig {
	return ConnectionPoolConfig{
		MaxIdleConns:    10,
		MaxOpenConns:    100,
		ConnMaxLifetime: time.Hour,
		ConnMaxIdleTime: 30 * time.Minute,
	}
}

// PgConfig is a postgres configuration struct.
// When the DSN is parsed, the values are stored in this struct.
type PgConfig struct {
	Database string // dbname
	User     string // user
	Password string // password, default ""
	Host     string // host, default: localhost
	Port     string // postgres port, default 5432
	SSLMode  string // ssl_mode, default=disabled
	Timezone string // Timezone
}

// ParseDSN parses the DSN string and stores the values in the PgConfig struct.
// The DSN string should be in the format:
// "dbname=test user=postgres password=postgres host=localhost port=5432 sslmode=disable TimeZone=Asia/Kolkata"
//
// If host is not provided, it defaults to localhost and if port is not provided, it defaults to 5432.
// If sslmode is not provided, it defaults to disabled. Other values are stored as is.
// Usage:
//
//	config := &PgConfig{}
//	err := config.ParseDSN("dbname=test user=postgres password=postgres host=localhost port=5432 sslmode=disable TimeZone=Asia/Kolkata")
//	if err != nil {
//		fmt.Println(err)
//	}
func (config *PgConfig) ParseDSN(dsn string) error {
	if dsn == "" {
		return fmt.Errorf("DSN is empty")
	}

	var keys int8
	configMap := map[string]string{}
	for _, s := range strings.Split(dsn, " ") {
		v := strings.Split(s, "=")

		if len(v) == 2 {
			configMap[v[0]] = v[1]
			keys++
		}
	}

	if keys == 0 {
		return fmt.Errorf("DSN is invalid")
	}

	config.Database = configMap["dbname"]
	config.User = configMap["user"]
	config.Host = configMap["host"]
	config.Password = configMap["password"]
	config.Timezone = configMap["TimeZone"]

	if configMap["host"] == "" {
		config.Host = "localhost"
	} else {
		if ip := net.ParseIP(config.Host); ip == nil {
			// If not an IP, check if it's a valid hostname
			if _, err := net.LookupIP(config.Host); err != nil {
				return fmt.Errorf("invalid host: %s:%v", config.Host, err)
			}
		}
	}

	if configMap["port"] != "" {
		config.Port = configMap["port"]
		portNum, err := strconv.Atoi(config.Port)
		if err != nil {
			return fmt.Errorf("invalid port number: %w", err)
		}
		if portNum < 1 || portNum > 65535 {
			return fmt.Errorf("invalid port number: %w", err)
		}
	} else {
		config.Port = "5432"
	}

	if configMap["sslmode"] != "" {
		config.SSLMode = configMap["sslmode"]
	} else {
		config.SSLMode = "disabled"
	}

	return nil
}

// PgConnect connects to the postgres database using the DSN string.
// logOutput is the writer where logs will be written.
// logLevel is the log level for the logger. The default slow threshold for slow queries is 1 second.
// poolConfig is the connection pool configuration. If nil, default values are used.
// It returns a gorm.DB instance and an error if any.
func PgConnect(dsn string, logOutput io.Writer, logLevel logger.LogLevel, poolConfig *ConnectionPoolConfig) (*gorm.DB, error) {
	cfg := &PgConfig{}
	err := cfg.ParseDSN(dsn)
	if err != nil {
		return nil, fmt.Errorf("error parsing DSN: %w", err)
	}

	// Use default pool config if not provided
	if poolConfig == nil {
		defaultConfig := DefaultConnectionPoolConfig()
		poolConfig = &defaultConfig
	}

	gormConfig := &gorm.Config{
		PrepareStmt:                      true,
		IgnoreRelationshipsWhenMigrating: false,
		Logger: logger.New(log.New(logOutput, "\r\n", log.LstdFlags), logger.Config{
			SlowThreshold: time.Second,
			LogLevel:      logLevel,
			Colorful:      true,
		}),
	}

	// Set timezone if specified
	if cfg.Timezone != "" {
		gormConfig.NowFunc = func() time.Time {
			loc, err := time.LoadLocation(cfg.Timezone)
			if err != nil {
				return time.Now()
			}
			return time.Now().In(loc)
		}
	}

	db, err := gorm.Open(postgres.Open(dsn), gormConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, fmt.Errorf("failed to get SQL database: %w", err)
	}

	// Apply connection pool settings
	sqlDB.SetMaxIdleConns(poolConfig.MaxIdleConns)
	sqlDB.SetMaxOpenConns(poolConfig.MaxOpenConns)
	sqlDB.SetConnMaxLifetime(poolConfig.ConnMaxLifetime)
	sqlDB.SetConnMaxIdleTime(poolConfig.ConnMaxIdleTime)

	// Ping the database
	if err = sqlDB.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	return db, nil
}

// PgClose closes the database connection.
// This function is used to close the database connection.
// It returns an error if any.
func PgClose(db *gorm.DB) error {
	if db == nil {
		return nil
	}

	sqlDB, err := db.DB()
	if err != nil {
		return err
	}

	err = sqlDB.Close()
	if err != nil {
		return err
	}
	return nil
}

// PgConnectWithConn establishes a connection to the database using the provided connection.
// It returns a gorm.DB instance and an error if any.
func PgConnectWithConn(conn *sql.DB, logOutput io.Writer, logLevel logger.LogLevel, poolConfig *ConnectionPoolConfig) (*gorm.DB, error) {
	// Use default pool config if not provided
	if poolConfig == nil {
		defaultConfig := DefaultConnectionPoolConfig()
		poolConfig = &defaultConfig
	}

	gormConfig := &gorm.Config{
		PrepareStmt:                      true,
		IgnoreRelationshipsWhenMigrating: false,
		Logger: logger.New(log.New(logOutput, "\r\n", log.LstdFlags), logger.Config{
			SlowThreshold: time.Second,
			LogLevel:      logLevel,
			Colorful:      true,
		}),
	}

	db, err := gorm.Open(postgres.New(postgres.Config{
		Conn: conn,
	}), gormConfig)

	if err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	sqlDB, err := db.DB()
	if err != nil {
		return nil, err
	}

	err = sqlDB.Ping()
	if err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Apply connection pool settings
	sqlDB.SetMaxIdleConns(poolConfig.MaxIdleConns)
	sqlDB.SetMaxOpenConns(poolConfig.MaxOpenConns)
	sqlDB.SetConnMaxLifetime(poolConfig.ConnMaxLifetime)
	sqlDB.SetConnMaxIdleTime(poolConfig.ConnMaxIdleTime)

	return db, nil
}
