package cmd

import (
	"fmt"
	"math"
	"time"

	"github.com/aleda145/kavla-cli/internal/auth"
	"github.com/spf13/cobra"
)

func formatDuration(d time.Duration) string {
	if d < 0 {
		return "expired"
	}
	hours := d.Hours()
	if hours < 1 {
		return fmt.Sprintf("%d minutes", int(math.Round(d.Minutes())))
	}
	if hours < 24 {
		return fmt.Sprintf("%.0f hours", hours)
	}
	days := hours / 24
	return fmt.Sprintf("%.0f days", days)
}

var statusCmd = &cobra.Command{
	Use:   "status",
	Short: "Check authentication status",
	Run: func(cmd *cobra.Command, args []string) {
		config, err := auth.LoadConfig()
		if err != nil || config.Token == "" {
			fmt.Println("Not logged in. Run 'kavla login' to authenticate.")
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

		fmt.Printf("Logged in as %s (%s)\n", validation.User.Name, validation.User.Email)

		// Check token expiry from the freshly rotated token we just validated.
		expiry, expiryErr := auth.GetTokenExpiry(config.Token)
		if expiryErr == nil {
			remaining := time.Until(expiry)
			if remaining > 0 {
				fmt.Printf("Token expires in %s\n", formatDuration(remaining))
			} else {
				fmt.Println("Token has expired. Run 'kavla login' to refresh.")
			}
		}

		// Show configured sources
		if config.Sources != nil && len(config.Sources) > 0 {
			fmt.Printf("\nConfigured sources:\n")
			for name, src := range config.Sources {
				fmt.Printf("   - %s (%s)\n", name, src.Type)
			}
		}
	},
}

func init() {
	rootCmd.AddCommand(statusCmd)
}
