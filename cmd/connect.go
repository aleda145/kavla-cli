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
		config, err := auth.LoadConfig()
		if err != nil {
			fmt.Printf("Error loading config: %v. Please run 'kavla login' first.\n", err)
			return
		}
		if config.Token == "" {
			fmt.Println("Not logged in. Please run 'kavla login' first.")
			return
		}

		authUrl := "https://auth.kavla.dev"
		if config.AuthUrl != "" {
			authUrl = config.AuthUrl
		}

		validation, err := auth.ValidateToken(authUrl, config.Token)
		if err != nil {
			fmt.Println("Token is invalid or expired. Run 'kavla login' to re-authenticate.")
			return
		}
		if validation.Token != config.Token {
			config.Token = validation.Token
			if err := auth.SaveConfig(config); err != nil {
				fmt.Printf("Failed to persist refreshed token: %v\n", err)
				return
			}
		}

		var roomId string
		var roomName string

		if len(args) == 1 {
			roomId = args[0]
		} else {
			// Interactive canvas selection
			fmt.Println("Fetching your canvases...")
			rooms, err := fetchRooms(authUrl, config.Token)
			if err != nil {
				fmt.Printf("Error fetching canvases: %v\n", err)
				return
			}

			if len(rooms) == 0 {
				fmt.Println("No canvases found. Create one at app.kavla.dev first.")
				return
			}

			selected := pickRoom(rooms)
			if selected == -1 {
				fmt.Println("Cancelled.")
				return
			}

			roomId = rooms[selected].ID
			roomName = rooms[selected].Name
		}

		targetUrl := connectUrl
		// If flag is default, and config has value, use config
		if !cmd.Flags().Changed("url") && config.ApiUrl != "" {
			targetUrl = config.ApiUrl
		}

		manager := transport.NewManager(targetUrl, roomId, config.Token, config.Sources, verbose)
		if err := manager.Prepare(); err != nil {
			fmt.Printf("Failed to prepare local sources: %v\n", err)
			return
		}
		if verbose {
			manager.Log("Verbose logging enabled\n")
		}

		// Pre-check room access
		fmt.Print("Checking access... ")
		if err := checkRoomAccess(authUrl, config.Token, roomId); err != nil {
			manager.Stop(transport.DisconnectReasonLocalShutdown, nil)
			fmt.Printf("\n%v\n", err)
			return
		}
		if roomName != "" {
			fmt.Printf("OK\nConnecting to \"%s\"...\n", roomName)
		} else {
			fmt.Printf("OK\nConnecting to %s...\n", roomId)
		}

		if err := manager.Start(); err != nil {
			manager.Stop(transport.DisconnectReasonLocalShutdown, nil)
			fmt.Printf("Connection failed: %v\n", err)
			return
		}

		// Wait for interrupt or disconnect
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(c)

		select {
		case <-c:
			// User pressed Ctrl+C
			manager.Stop(transport.DisconnectReasonLocalShutdown, nil)
		case event := <-manager.Done:
			switch event.Reason {
			case transport.DisconnectReasonRemoteClose:
				fmt.Println("\nDisconnected by remote.")
			case transport.DisconnectReasonReadError:
				fmt.Printf("\nDisconnected due to connection error: %v\n", event.Err)
			}
		}
	},
}

func init() {
	connectCmd.Flags().StringVar(&connectUrl, "url", "https://app.kavla.dev", "Kavla app API URL")
	rootCmd.AddCommand(connectCmd)
}
