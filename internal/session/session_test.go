package session

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	"github.com/aleda145/kavla-cli/internal/auth"
	"github.com/aleda145/kavla-cli/internal/runner"
)

type stubClient struct {
	messages          []map[string]interface{}
	presignedReadURLs map[string]string
}

func (c *stubClient) SendJSON(msg map[string]interface{}) error {
	c.messages = append(c.messages, msg)
	return nil
}

func (c *stubClient) SendResultData(shapeID string, format runner.ResultFormat, data []byte) error {
	return nil
}

func (c *stubClient) GetR2PresignedURL(shapeID string) (string, string, error) {
	return "", "", nil
}

func (c *stubClient) GetPresignedReadURL(r2ObjectKey string) (string, error) {
	url, ok := c.presignedReadURLs[r2ObjectKey]
	if !ok {
		return "", context.DeadlineExceeded
	}
	return url, nil
}

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

func TestMountRemoteFileSourcesRejectsUnsupportedFileType(t *testing.T) {
	s := New(map[string]auth.SourceConfig{})
	if err := s.Start(); err != nil {
		t.Fatalf("Start returned error: %v", err)
	}
	defer s.Close()

	client := &stubClient{
		presignedReadURLs: map[string]string{
			"owner/room/events.tsv": "https://example.com/events.tsv?sig=1",
		},
	}

	err := s.mountRemoteFileSources(context.Background(), client, []runner.MountedFileSource{
		{
			SourceName:  "events",
			R2ObjectKey: "owner/room/events.tsv",
			FileName:    "events.tsv",
		},
	})
	if err == nil {
		t.Fatal("expected mountRemoteFileSources to fail for unsupported file type")
	}

	if !strings.Contains(err.Error(), "unsupported mounted file type") {
		t.Fatalf("expected unsupported mounted file type error, got %v", err)
	}
}
