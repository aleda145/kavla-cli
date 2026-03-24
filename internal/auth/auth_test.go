package auth

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfigAllowMissingReturnsEmptyConfigWhenFileIsMissing(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	config, err := LoadConfigAllowMissing()
	if err != nil {
		t.Fatalf("LoadConfigAllowMissing returned error: %v", err)
	}
	if config == nil {
		t.Fatal("expected config, got nil")
	}
	if config.Token != "" || config.ApiUrl != "" || config.AuthUrl != "" || config.AppUrl != "" {
		t.Fatalf("expected empty config, got %#v", config)
	}
}

func TestSaveTokenDoesNotOverwriteMalformedConfig(t *testing.T) {
	t.Setenv("HOME", t.TempDir())

	path, err := GetConfigPath()
	if err != nil {
		t.Fatalf("GetConfigPath returned error: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("MkdirAll returned error: %v", err)
	}

	badConfig := "token: [\n"
	if err := os.WriteFile(path, []byte(badConfig), 0o600); err != nil {
		t.Fatalf("WriteFile returned error: %v", err)
	}

	if err := SaveToken("new-token"); err == nil {
		t.Fatal("expected SaveToken to fail on malformed config")
	}

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile returned error: %v", err)
	}
	if string(data) != badConfig {
		t.Fatalf("expected malformed config to remain unchanged, got %q", string(data))
	}
}
