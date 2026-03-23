package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"

	"golang.org/x/term"
)

type Room struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func fetchRooms(authUrl, token string) ([]Room, error) {
	url := fmt.Sprintf("%s/api/kavla/rooms/list", authUrl)

	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", token)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("could not reach auth server: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 200 {
		if resp.StatusCode == 401 {
			return nil, fmt.Errorf("token is invalid or expired. Run 'kavla login' to re-authenticate")
		}
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("failed to list rooms (status %d): %s", resp.StatusCode, string(body))
	}

	var rooms []Room
	if err := json.NewDecoder(resp.Body).Decode(&rooms); err != nil {
		return nil, fmt.Errorf("failed to parse rooms: %w", err)
	}

	return rooms, nil
}

const pageSize = 10

// pickRoom shows an interactive arrow-key menu to select a room.
// Shows at most pageSize items at a time with scroll indicators.
// Returns the index of the selected room, or -1 if cancelled.
func pickRoom(rooms []Room) int {
	if len(rooms) == 0 {
		return -1
	}

	// Switch terminal to raw mode
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		// Fallback: just return first room
		fmt.Println("Could not enter interactive mode, selecting first room.")
		return 0
	}
	defer term.Restore(int(os.Stdin.Fd()), oldState)

	selected := 0
	scrollOffset := 0
	buf := make([]byte, 3)
	lastRenderedLines := 0

	for {
		// Calculate visible window
		visibleCount := len(rooms)
		if visibleCount > pageSize {
			visibleCount = pageSize
		}

		// Adjust scroll offset to keep selected item visible
		if selected < scrollOffset {
			scrollOffset = selected
		}
		if selected >= scrollOffset+pageSize {
			scrollOffset = selected - pageSize + 1
		}

		hasMore := len(rooms) > pageSize
		showUpIndicator := hasMore && scrollOffset > 0
		showDownIndicator := hasMore && scrollOffset+pageSize < len(rooms)

		// Clear previous render
		if lastRenderedLines > 0 {
			for i := 0; i < lastRenderedLines; i++ {
				fmt.Print("\033[A\033[2K")
			}
		}

		// Draw
		lineCount := 0
		fmt.Print("\r\033[J")
		fmt.Printf("\033[1m  Select a canvas:\033[0m")
		if hasMore {
			fmt.Printf("  \033[2m(%d/%d)\033[0m", selected+1, len(rooms))
		}
		fmt.Print("\r\n\r\n")
		lineCount += 2

		if showUpIndicator {
			fmt.Print("  \033[2m  more above\033[0m\r\n")
			lineCount++
		}

		end := scrollOffset + pageSize
		if end > len(rooms) {
			end = len(rooms)
		}
		for i := scrollOffset; i < end; i++ {
			if i == selected {
				fmt.Printf("  \033[1m\033[43m\033[30m > %s \033[0m\r\n", rooms[i].Name)
			} else {
				fmt.Printf("    %s\r\n", rooms[i].Name)
			}
			lineCount++
		}

		if showDownIndicator {
			fmt.Print("  \033[2m  more below\033[0m\r\n")
			lineCount++
		}

		fmt.Print("\r\n  \033[2mup/down navigate | enter select | q quit\033[0m")
		lineCount += 1

		lastRenderedLines = lineCount

		// Read input
		n, err := os.Stdin.Read(buf)
		if err != nil {
			return -1
		}

		if n == 1 {
			switch buf[0] {
			case 13: // Enter
				fmt.Print("\033[2K") // Clear current line first
				for i := 0; i < lastRenderedLines; i++ {
					fmt.Print("\033[A\033[2K")
				}
				fmt.Print("\r")
				return selected
			case 'q', 3: // q or Ctrl+C
				fmt.Print("\033[2K") // Clear current line first
				for i := 0; i < lastRenderedLines; i++ {
					fmt.Print("\033[A\033[2K")
				}
				fmt.Print("\r")
				return -1
			case 'k': // vim up
				if selected > 0 {
					selected--
				}
			case 'j': // vim down
				if selected < len(rooms)-1 {
					selected++
				}
			}
		} else if n == 3 && buf[0] == 27 && buf[1] == 91 {
			switch buf[2] {
			case 65: // Up arrow
				if selected > 0 {
					selected--
				}
			case 66: // Down arrow
				if selected < len(rooms)-1 {
					selected++
				}
			}
		}
	}
}
