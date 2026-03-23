package cmd

import (
	"fmt"

	"github.com/aleda145/kavla-cli/internal/auth"
	"github.com/spf13/cobra"
)

var configCmd = &cobra.Command{
	Use:   "config",
	Short: "Manage Kavla configuration",
}

var setApiUrlCmd = &cobra.Command{
	Use:   "set-api-url [URL]",
	Short: "Set the Kavla app API URL",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		url := args[0]
		config, err := auth.LoadConfig()
		if err != nil {
			config = &auth.Config{}
		}
		config.ApiUrl = url
		if err := auth.SaveConfig(config); err != nil {
			fmt.Printf("Error saving config: %v\n", err)
			return
		}
		fmt.Printf("API URL set to: %s\n", url)
	},
}

var setAuthUrlCmd = &cobra.Command{
	Use:   "set-auth-url [URL]",
	Short: "Set the Kavla Auth URL",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		url := args[0]
		config, err := auth.LoadConfig()
		if err != nil {
			config = &auth.Config{}
		}
		config.AuthUrl = url
		if err := auth.SaveConfig(config); err != nil {
			fmt.Printf("Error saving config: %v\n", err)
			return
		}
		fmt.Printf("Auth URL set to: %s\n", url)
	},
}

var setAppUrlCmd = &cobra.Command{
	Use:   "set-app-url [URL]",
	Short: "Set the Kavla App URL (for CLI login)",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		url := args[0]
		config, err := auth.LoadConfig()
		if err != nil {
			config = &auth.Config{}
		}
		config.AppUrl = url
		if err := auth.SaveConfig(config); err != nil {
			fmt.Printf("Error saving config: %v\n", err)
			return
		}
		fmt.Printf("App URL set to: %s\n", url)
	},
}

var viewConfigCmd = &cobra.Command{
	Use:   "view",
	Short: "View current configuration",
	Run: func(cmd *cobra.Command, args []string) {
		config, err := auth.LoadConfig()
		if err != nil {
			fmt.Println("No configuration found or error loading it.")
			return
		}
		fmt.Printf("App URL:  %s\n", config.AppUrl)
		fmt.Printf("API URL:  %s\n", config.ApiUrl)
		fmt.Printf("Auth URL: %s\n", config.AuthUrl)
		if config.Token != "" {
			fmt.Println("Token:    [Set]")
		} else {
			fmt.Println("Token:    [Not Set]")
		}
	},
}

func init() {
	configCmd.AddCommand(setApiUrlCmd)
	configCmd.AddCommand(setAuthUrlCmd)
	configCmd.AddCommand(setAppUrlCmd)
	configCmd.AddCommand(viewConfigCmd)
	rootCmd.AddCommand(configCmd)
}
