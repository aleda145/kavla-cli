package cmd

import (
	"fmt"
	"path/filepath"

	"github.com/aleda145/kavla-cli/internal/auth"
	"github.com/spf13/cobra"
)

var (
	sourceName       string
	sourceType       string
	sourceConnection string
)

var addSourceCmd = &cobra.Command{
	Use:   "add-source",
	Short: "Add a new data source",
	Run: func(cmd *cobra.Command, args []string) {
		if sourceName == "" || sourceType == "" || sourceConnection == "" {
			fmt.Println("Error: --name, --type, and --connection flags are required.")
			_ = cmd.Help()
			return
		}

		config, err := auth.LoadConfig()
		if err != nil {
			config = &auth.Config{Sources: make(map[string]auth.SourceConfig)}
		}
		if config.Sources == nil {
			config.Sources = make(map[string]auth.SourceConfig)
		}

		connection := sourceConnection

		// Resolve to absolute path for directory and duckdb types
		if sourceType == "directory" || sourceType == "duckdb" {
			absPath, err := filepath.Abs(connection)
			if err != nil {
				fmt.Printf("Error resolving path: %v\n", err)
				return
			}
			connection = absPath
		}

		config.Sources[sourceName] = auth.SourceConfig{
			Type:       sourceType,
			Connection: connection,
		}

		if err := auth.SaveConfig(config); err != nil {
			fmt.Printf("Error saving config: %v\n", err)
			return
		}
		fmt.Printf("Source '%s' (%s) added successfully.\n", sourceName, sourceType)
	},
}

var listSourcesCmd = &cobra.Command{
	Use:   "list-sources",
	Short: "List configured data sources",
	Run: func(cmd *cobra.Command, args []string) {
		config, err := auth.LoadConfig()
		if err != nil || config.Sources == nil || len(config.Sources) == 0 {
			fmt.Println("No sources configured.")
			return
		}

		fmt.Println("Configured Sources:")
		for name, src := range config.Sources {
			fmt.Printf("- %s (%s): %s\n", name, src.Type, src.Connection)
		}
	},
}

var removeSourceCmd = &cobra.Command{
	Use:   "remove-source [NAME]",
	Short: "Remove a data source",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		name := args[0]
		config, err := auth.LoadConfig()
		if err != nil {
			fmt.Println("Error loading config.")
			return
		}

		if _, exists := config.Sources[name]; !exists {
			fmt.Printf("Source '%s' not found.\n", name)
			return
		}

		delete(config.Sources, name)
		if err := auth.SaveConfig(config); err != nil {
			fmt.Printf("Error saving config: %v\n", err)
			return
		}
		fmt.Printf("Source '%s' removed.\n", name)
	},
}

func init() {
	addSourceCmd.Flags().StringVar(&sourceName, "name", "", "Name of the source")
	addSourceCmd.Flags().StringVar(&sourceType, "type", "", "Type of source (e.g., duckdb)")
	addSourceCmd.Flags().StringVar(&sourceConnection, "connection", "", "Connection string or file path")

	configCmd.AddCommand(addSourceCmd)
	configCmd.AddCommand(listSourcesCmd)
	configCmd.AddCommand(removeSourceCmd)
}
