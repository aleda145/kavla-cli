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
	"time"

	"github.com/aleda145/kavla-cli/internal/auth"
	"github.com/spf13/cobra"
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
		config, err := auth.LoadConfigAllowMissing()
		if err != nil {
			fmt.Printf("Error loading config: %v\n", err)
			return
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
			fmt.Printf("Invalid app URL: %v\n", err)
			return
		}

		loginState, err := newLoginState()
		if err != nil {
			fmt.Printf("Error generating login state: %v\n", err)
			return
		}

		// Find a free port
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			fmt.Printf("Error starting local server: %v\n", err)
			return
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

		if err := openBrowser(authURL); err != nil {
			fmt.Printf("Could not open browser: %v\n", err)
			fmt.Printf("Please open this URL manually: %s\n", authURL)
		}

		// Wait for the token with a timeout
		select {
		case token := <-tokenCh:
			// Give the browser a moment to render the success page
			time.Sleep(500 * time.Millisecond)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			server.Shutdown(ctx)

			validation, err := auth.ValidateToken(authUrl, token)
			if err != nil {
				fmt.Printf("Error validating returned token: %v\n", err)
				return
			}

			if err := auth.SaveToken(validation.Token); err != nil {
				fmt.Printf("Error saving token: %v\n", err)
				return
			}
			fmt.Println("Successfully logged in!")

		case <-time.After(2 * time.Minute):
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			server.Shutdown(ctx)
			fmt.Println("Login timed out. Please try again.")
		}
	},
}

func init() {
	loginCmd.Flags().StringVar(&loginUrl, "url", "", "Kavla App URL (overrides config)")
	rootCmd.AddCommand(loginCmd)
}
