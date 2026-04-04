package cmd

import (
	"fmt"
	"io"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/aleda145/kavla-cli/internal/auth"
	"github.com/aleda145/kavla-cli/internal/transport"
	"github.com/spf13/cobra"
)

var connectUrl string

// checkRoomAccess verifies the user has access to the room before attempting a WebSocket connection.
func checkRoomAccess(authUrl, token, roomId string) error {
	url := fmt.Sprintf("%s/api/collections/rooms/records/%s", authUrl, roomId)
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return fmt.Errorf("could not create request: %w", err)
	}
	req.Header.Set("Authorization", token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("could not reach auth server: %w", err)
	}
	defer resp.Body.Close()

	switch resp.StatusCode {
	case 200:
		return nil
	case 404:
		return fmt.Errorf("canvas not found. Check the ID and make sure you have access")
	case 401:
		return fmt.Errorf("token is invalid or expired. Run 'kavla login' to re-authenticate")
	case 403:
		return fmt.Errorf("you don't have access to this canvas. Ask the owner to add you as a collaborator")
	default:
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("unexpected error (status %d): %s", resp.StatusCode, string(body))
	}
}

var connectCmd = &cobra.Command{
	Use:   "connect [ROOM_ID]",
	Short: "Connect to a Kavla room",
	Args:  cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		if err := runConnect(args, connectUrl, cmd.Flags().Changed("url")); err != nil {
			fmt.Println(err)
		}
	},
}

func runConnect(args []string, urlOverride string, urlOverridden bool) error {
	config, err := auth.LoadConfigAllowMissing()
	if err != nil {
		return fmt.Errorf("error loading config: %w", err)
	}
	if config.Token == "" {
		return fmt.Errorf("Not logged in. Please run 'kavla login' first.")
	}

	authUrl := "https://auth.kavla.dev"
	if config.AuthUrl != "" {
		authUrl = config.AuthUrl
	}

	validation, err := auth.ValidateToken(authUrl, config.Token)
	if err != nil {
		return fmt.Errorf("Token is invalid or expired. Run 'kavla login' to re-authenticate.")
	}
	if validation.Token != config.Token {
		config.Token = validation.Token
		if err := auth.SaveConfig(config); err != nil {
			return fmt.Errorf("Failed to persist refreshed token: %w", err)
		}
	}

	var roomId string
	var roomName string

	if len(args) == 1 {
		roomId = args[0]
	} else {
		fmt.Println("Fetching your canvases...")
		rooms, err := fetchRooms(authUrl, config.Token)
		if err != nil {
			return fmt.Errorf("Error fetching canvases: %w", err)
		}

		if len(rooms) == 0 {
			return fmt.Errorf("No canvases found. Create one at app.kavla.dev first.")
		}

		selected := pickRoom(rooms)
		if selected == -1 {
			return fmt.Errorf("Cancelled.")
		}

		roomId = rooms[selected].ID
		roomName = rooms[selected].Name
	}

	targetUrl := urlOverride
	if !urlOverridden && config.ApiUrl != "" {
		targetUrl = config.ApiUrl
	}

	manager := transport.NewManager(targetUrl, roomId, config.Token, config.Sources, verbose)
	if err := manager.Prepare(); err != nil {
		return fmt.Errorf("Failed to prepare local sources: %w", err)
	}
	if verbose {
		manager.Log("Verbose logging enabled\n")
	}

	fmt.Print("Checking access... ")
	if err := checkRoomAccess(authUrl, config.Token, roomId); err != nil {
		manager.Stop(transport.DisconnectReasonLocalShutdown, nil)
		return fmt.Errorf("\n%w", err)
	}
	if roomName != "" {
		fmt.Printf("OK\nConnecting to \"%s\"...\n", roomName)
	} else {
		fmt.Printf("OK\nConnecting to %s...\n", roomId)
	}

	if err := manager.Start(); err != nil {
		manager.Stop(transport.DisconnectReasonLocalShutdown, nil)
		return fmt.Errorf("Connection failed: %w", err)
	}

	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(c)

	select {
	case <-c:
		manager.Stop(transport.DisconnectReasonLocalShutdown, nil)
	case event := <-manager.Done:
		switch event.Reason {
		case transport.DisconnectReasonRemoteClose:
			fmt.Println("\nDisconnected by remote.")
		case transport.DisconnectReasonReadError:
			fmt.Printf("\nDisconnected due to connection error: %v\n", event.Err)
		}
	}

	return nil
}

func init() {
	connectCmd.Flags().StringVar(&connectUrl, "url", "https://app.kavla.dev", "Kavla app API URL")
	rootCmd.AddCommand(connectCmd)
}
