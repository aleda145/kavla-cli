package session

import (
	"path/filepath"
	"testing"

	"github.com/aleda145/kavla-cli/internal/auth"
)

func TestSourceListIncludesUnavailableSourcesAfterStart(t *testing.T) {
	goodDir := t.TempDir()
	badDir := filepath.Join(t.TempDir(), "missing")

	s := New(map[string]auth.SourceConfig{
		"good_dir": {
			Type:       "directory",
			Connection: goodDir,
		},
		"bad_dir": {
			Type:       "directory",
			Connection: badDir,
		},
	})

	if err := s.Start(); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	defer s.Close()

	sourceList := s.SourceList()
	if len(sourceList) != 2 {
		t.Fatalf("expected 2 source entries, got %d", len(sourceList))
	}

	byName := make(map[string]map[string]interface{}, len(sourceList))
	for _, entry := range sourceList {
		name, _ := entry["name"].(string)
		byName[name] = entry
	}

	good := byName["good_dir"]
	if available, _ := good["available"].(bool); !available {
		t.Fatalf("expected good_dir to be available, got %+v", good)
	}
	if _, ok := good["error"]; ok {
		t.Fatalf("did not expect good_dir error, got %+v", good)
	}

	bad := byName["bad_dir"]
	if available, _ := bad["available"].(bool); available {
		t.Fatalf("expected bad_dir to be unavailable, got %+v", bad)
	}
	errMsg, _ := bad["error"].(string)
	if errMsg == "" {
		t.Fatalf("expected bad_dir error message, got %+v", bad)
	}
}
