package cmd

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"syscall"
	"time"

	"github.com/aleda145/kavla-cli/internal/auth"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

var loginUrl string

func newLoginState() (string, error) {
	buf := make([]byte, 16)
	if _, err := rand.Read(buf); err != nil {
		return "", err
	}
	return hex.EncodeToString(buf), nil
}

func originForURL(rawURL string) (string, error) {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return "", fmt.Errorf("invalid URL: %s", rawURL)
	}
	return parsed.Scheme + "://" + parsed.Host, nil
}

const callbackHTML = `<!DOCTYPE html>
<html>
<head>
  <title>Kavla CLI</title>
  <style>
    body {
      font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
      display: flex;
      align-items: center;
      justify-content: center;
      min-height: 100vh;
      margin: 0;
      background: #f8f7f4;
    }
    .card {
      background: white;
      border: 4px solid black;
      border-radius: 12px;
      box-shadow: 8px 8px 0px 0px rgba(0,0,0,1);
      padding: 40px;
      text-align: center;
      max-width: 400px;
    }
    h1 { font-size: 24px; margin-bottom: 8px; }
    p { color: #666; font-size: 14px; }
  </style>
</head>
<body>
  <div class="card">
    <h1>CLI Authenticated!</h1>
    <p>You can close this tab and return to your terminal.</p>
  </div>
</body>
</html>`

var loginCmd = &cobra.Command{
	Use:   "login",
	Short: "Authenticate with Kavla",
	Run: func(cmd *cobra.Command, args []string) {
		if err := runLogin(); err != nil {
			fmt.Println(err)
		}
	},
}

func runLogin() error {
	config, err := auth.LoadConfigAllowMissing()
	if err != nil {
		return fmt.Errorf("error loading config: %w", err)
	}

	appUrl := "https://app.kavla.dev"
	authUrl := "https://auth.kavla.dev"
	if config != nil && config.AppUrl != "" {
		appUrl = config.AppUrl
	}
	if config != nil && config.AuthUrl != "" {
		authUrl = config.AuthUrl
	}
	if loginUrl != "" {
		appUrl = loginUrl
	}

	expectedOrigin, err := originForURL(appUrl)
	if err != nil {
		return fmt.Errorf("invalid app URL: %w", err)
	}

	loginState, err := newLoginState()
	if err != nil {
		return fmt.Errorf("error generating login state: %w", err)
	}

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("error starting local server: %w", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	listener.Close()

	tokenCh := make(chan string, 1)

	mux := http.NewServeMux()
	mux.HandleFunc("/callback", func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin != expectedOrigin {
			http.Error(w, "Forbidden origin", http.StatusForbidden)
			return
		}

		if r.URL.Query().Get("state") != loginState {
			http.Error(w, "Invalid state", http.StatusForbidden)
			return
		}

		// Handle CORS preflight for the Kavla app only.
		w.Header().Set("Access-Control-Allow-Origin", expectedOrigin)
		w.Header().Set("Access-Control-Allow-Methods", "POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}

		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var body struct {
			Token string `json:"token"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Token == "" {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}

		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, callbackHTML)

		tokenCh <- body.Token
	})

	server := &http.Server{
		Addr:    fmt.Sprintf("127.0.0.1:%d", port),
		Handler: mux,
	}

	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			fmt.Printf("Server error: %v\n", err)
		}
	}()

	authURL := fmt.Sprintf("%s/auth?mode=login&cli_port=%d&cli_state=%s", appUrl, port, loginState)

	fmt.Println("Opening browser for authentication...")
	fmt.Printf("If the browser doesn't open, visit: %s\n\n", authURL)
	fmt.Println("Waiting for authentication...")
	cancelCh, cleanupCancel, err := startLoginCancelListener()
	loginCancelActive := err == nil
	if err == nil {
		fmt.Println("  \033[2mq quit\033[0m")
	}

	if err := openBrowser(authURL); err != nil {
		fmt.Printf("Could not open browser: %v\n", err)
		fmt.Printf("Please open this URL manually: %s\n", authURL)
	}

	token, err := waitForLoginResult(tokenCh, cancelCh, 2*time.Minute)
	if loginCancelActive {
		cleanupCancel()
	}
	if err != nil {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		server.Shutdown(ctx)
		return err
	}

	time.Sleep(500 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	server.Shutdown(ctx)

	validation, err := auth.ValidateToken(authUrl, token)
	if err != nil {
		return fmt.Errorf("error validating returned token: %w", err)
	}

	if err := auth.SaveToken(validation.Token); err != nil {
		return fmt.Errorf("error saving token: %w", err)
	}
	fmt.Println("Successfully logged in!")
	return nil
}

func startLoginCancelListener() (<-chan struct{}, func(), error) {
	if !term.IsTerminal(int(os.Stdin.Fd())) || !term.IsTerminal(int(os.Stdout.Fd())) {
		return nil, func() {}, fmt.Errorf("login cancel unavailable without a terminal")
	}

	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		return nil, func() {}, err
	}

	dupFD, err := syscall.Dup(int(os.Stdin.Fd()))
	if err != nil {
		term.Restore(int(os.Stdin.Fd()), oldState)
		return nil, func() {}, err
	}
	if err := syscall.SetNonblock(dupFD, true); err != nil {
		term.Restore(int(os.Stdin.Fd()), oldState)
		syscall.Close(dupFD)
		return nil, func() {}, err
	}
	dupStdin := os.NewFile(uintptr(dupFD), "stdin-dup")
	cancelCh := make(chan struct{}, 1)
	doneCh := make(chan struct{})
	stopCh := make(chan struct{})

	go func() {
		defer close(doneCh)
		buf := []byte{0}
		for {
			select {
			case <-stopCh:
				return
			default:
			}

			n, err := syscall.Read(dupFD, buf)
			if err != nil {
				if err == syscall.EAGAIN || err == syscall.EWOULDBLOCK || err == syscall.EINTR {
					time.Sleep(25 * time.Millisecond)
					continue
				}
				return
			}
			if n != 1 {
				time.Sleep(25 * time.Millisecond)
				continue
			}
			if !isLoginCancelByte(buf[0]) {
				continue
			}
			select {
			case cancelCh <- struct{}{}:
			default:
			}
			return
		}
	}()

	cleanup := func() {
		close(stopCh)
		syscall.SetNonblock(dupFD, false)
		term.Restore(int(os.Stdin.Fd()), oldState)
		dupStdin.Close()
		<-doneCh
	}

	return cancelCh, cleanup, nil
}

func waitForLoginResult(tokenCh <-chan string, cancelCh <-chan struct{}, timeout time.Duration) (string, error) {
	select {
	case token := <-tokenCh:
		fmt.Print("\r\033[2K")
		return token, nil
	case <-cancelCh:
		fmt.Print("\r\033[2K")
		return "", fmt.Errorf("login cancelled")
	case <-time.After(timeout):
		return "", fmt.Errorf("login timed out. Please try again")
	}
}

func waitForLoginResultPassive(tokenCh <-chan string, timeout time.Duration) (string, error) {
	select {
	case token := <-tokenCh:
		return token, nil
	case <-time.After(timeout):
		return "", fmt.Errorf("login timed out. Please try again")
	}
}

func isLoginCancelByte(b byte) bool {
	return b == 'q' || b == 'Q'
}

func init() {
	loginCmd.Flags().StringVar(&loginUrl, "url", "", "Kavla App URL (overrides config)")
	rootCmd.AddCommand(loginCmd)
}
