package transport

import (
	"testing"
)

func TestWebsocketDialConfigUsesAuthorizationHeader(t *testing.T) {
	client := NewClient("https://app.kavla.dev", "room-123", "secret-token")

	wsURL, opts, err := client.websocketDialConfig()
	if err != nil {
		t.Fatalf("websocketDialConfig returned error: %v", err)
	}
	if wsURL != "wss://app.kavla.dev/api/data-socket/room-123?clientType=cli" {
		t.Fatalf("unexpected websocket URL: %s", wsURL)
	}
	if opts == nil {
		t.Fatal("expected dial options")
	}
	if got := opts.HTTPHeader.Get("Authorization"); got != "Bearer secret-token" {
		t.Fatalf("unexpected authorization header: %q", got)
	}
}
