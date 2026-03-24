package cmd

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
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

// pickRoom shows an interactive arrow-key menu to select a room.
// Shows at most pageSize items at a time with scroll indicators.
// Returns the index of the selected room, or -1 if cancelled.
func pickRoom(rooms []Room) int {
	if len(rooms) == 0 {
		return -1
	}

	labels := make([]string, 0, len(rooms))
	for _, room := range rooms {
		labels = append(labels, room.Name)
	}

	selected, err := chooseOptionInteractive("Select a canvas:", labels)
	if err != nil {
		fmt.Println("Could not enter interactive mode, selecting first room.")
		return 0
	}

	return selected
}
