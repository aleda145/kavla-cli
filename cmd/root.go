package cmd

import (
	"os"

	"github.com/spf13/cobra"
)

var verbose bool

var rootCmd = &cobra.Command{
	Use:   "kavla",
	Short: "Kavla CLI - Tunnel local database connections to a canvas",
	Long: `Kavla CLI allows you to securely tunnel your local database connections
to a canvas. Credentials are kept on your machine.`,
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose log output")
}
