package config

import (
	"os"
	"testing"
	"time"
)

type envTestCfg struct {
	DBSource            string        `mapstructure:"DB_SOURCE"`
	ServerAddress       string        `mapstructure:"SERVER_ADDRESS"`
	TokenKey            string        `mapstructure:"TOKEN_KEY"`
	AccessTokenDuration time.Duration `mapstructure:"ACCESS_TOKEN_DURATION"`
	RefreshToken        time.Duration `mapstructure:"REFRESH_TOKEN_DURATION"`
	Environment         string        `mapstructure:"ENVIRONMENT"`
	RbmUrl              string        `mapstructure:"RBM_URL"`
	HealthAddress       string        `mapstructure:"HEALTH_ADDRESS"`
}

func setEnv(t *testing.T) func() {
	t.Helper()
	vals := map[string]string{
		"DB_SOURCE":             "postgresql://root:secret@db:5432/tellmi_users?sslmode=disable",
		"SERVER_ADDRESS":        "0.0.0.0:8081",
		"TOKEN_KEY":             "secret",
		"ACCESS_TOKEN_DURATION": "15m",
		"REFRESH_TOKEN_DURATION": "720h",
		"ENVIRONMENT":           "docker",
		"RBM_URL":               "amqp://guest:guest@rabbitmq:5672/",
		"HEALTH_ADDRESS":        "0.0.0.0:8084",
	}
	for k, v := range vals {
		if err := os.Setenv(k, v); err != nil {
			t.Fatal(err)
		}
	}
	return func() {
		for k := range vals {
			os.Unsetenv(k)
		}
	}
}

func TestLoadEnv(t *testing.T) {
	cleanup := setEnv(t)
	defer cleanup()

	var cfg envTestCfg
	if err := LoadEnv(&cfg); err != nil {
		t.Fatalf("LoadEnv: %v", err)
	}
	if cfg.DBSource == "" || cfg.ServerAddress == "" || cfg.TokenKey == "" {
		t.Fatalf("strings not populated: %+v", cfg)
	}
	if cfg.AccessTokenDuration != 15*time.Minute {
		t.Fatalf("duration not parsed: %v", cfg.AccessTokenDuration)
	}
	if cfg.RefreshToken != 720*time.Hour {
		t.Fatalf("refresh duration not parsed: %v", cfg.RefreshToken)
	}
	if cfg.Environment != "docker" || cfg.RbmUrl == "" || cfg.HealthAddress == "" {
		t.Fatalf("misc fields wrong: %+v", cfg)
	}
}

func TestLoadContainerPath(t *testing.T) {
	cleanup := setEnv(t)
	defer cleanup()

	var cfg envTestCfg
	if err := Load(".", &cfg); err != nil {
		t.Fatalf("Load container path: %v", err)
	}
	if cfg.ServerAddress != "0.0.0.0:8081" {
		t.Fatalf("Load did not use env path: %+v", cfg)
	}
}

func TestIsContainer(t *testing.T) {
	for _, tc := range []struct {
		env  string
		want bool
	}{
		{"docker", true},
		{"k8s", true},
		{"dev", false},
		{"", false},
	} {
		os.Setenv("ENVIRONMENT", tc.env)
		if got := IsContainer(); got != tc.want {
			t.Fatalf("ENVIRONMENT=%q IsContainer=%v want %v", tc.env, got, tc.want)
		}
	}
}
