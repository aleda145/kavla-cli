package cmd

import (
	"testing"
	"time"
)

func TestWaitForLoginResultReturnsToken(t *testing.T) {
	tokenCh := make(chan string, 1)
	tokenCh <- "token"

	token, err := waitForLoginResultPassive(tokenCh, time.Second)
	if err != nil {
		t.Fatalf("waitForLoginResultPassive returned error: %v", err)
	}
	if token != "token" {
		t.Fatalf("expected token, got %q", token)
	}
}

func TestWaitForLoginResultTimesOut(t *testing.T) {
	tokenCh := make(chan string, 1)

	_, err := waitForLoginResultPassive(tokenCh, time.Millisecond)
	if err == nil {
		t.Fatal("expected timeout error")
	}
	if err.Error() != "login timed out. Please try again" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestWaitForLoginResultReturnsCancelled(t *testing.T) {
	tokenCh := make(chan string, 1)
	cancelCh := make(chan struct{}, 1)
	cancelCh <- struct{}{}

	_, err := waitForLoginResult(tokenCh, cancelCh, time.Second)
	if err == nil {
		t.Fatal("expected cancellation error")
	}
	if err.Error() != "login cancelled" {
		t.Fatalf("unexpected error: %v", err)
	}
}
