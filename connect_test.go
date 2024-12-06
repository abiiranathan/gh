package gh_test

import (
	"testing"

	"github.com/abiiranathan/gh"
	"github.com/stretchr/testify/assert"
)

func TestParseDSN(t *testing.T) {
	tests := []struct {
		name    string
		dsn     string
		want    *gh.PgConfig
		wantErr bool
	}{
		{
			name:    "Empty DSN",
			dsn:     "",
			wantErr: true,
		},
		{
			name: "Valid DSN",
			dsn:  "dbname=test user=postgres password=postgres host=localhost port=5432 sslmode=disable TimeZone=Asia/Kolkata",
			want: &gh.PgConfig{
				Database: "test",
				User:     "postgres",
				Password: "postgres",
				Host:     "localhost",
				Port:     "5432",
				SSLMode:  "disable",
				Timezone: "Asia/Kolkata",
			},
			wantErr: false,
		},
		{
			name: "DSN with default port and sslmode",
			dsn:  "dbname=test user=postgres password=postgres host=localhost TimeZone=Asia/Kolkata",
			want: &gh.PgConfig{
				Database: "test",
				User:     "postgres",
				Password: "postgres",
				Host:     "localhost",
				Port:     "5432",
				SSLMode:  "disabled",
				Timezone: "Asia/Kolkata",
			},
			wantErr: false,
		},
		{
			name:    "Invalid DSN",
			dsn:     "invalid_dsn",
			wantErr: true,
		},
		{
			name: "Missing values",
			dsn:  "dbname=test user=postgres",
			want: &gh.PgConfig{
				Database: "test",
				User:     "postgres",
				Password: "",
				Host:     "localhost",
				Port:     "5432",
				SSLMode:  "disabled",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := &gh.PgConfig{}
			err := config.ParseDSN(tt.dsn)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, config)
			}
		})
	}
}
