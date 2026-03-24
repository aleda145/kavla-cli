package cmd

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aleda145/kavla-cli/internal/updater"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

var verbose bool

var rootCmd = &cobra.Command{
	Use:   "kavla",
	Short: "Kavla CLI - Tunnel local database connections to a canvas",
	Long: `Kavla CLI allows you to securely tunnel your local database connections
to a canvas. Credentials are kept on your machine.`,
}

func Execute() {
	executedCmd, err := rootCmd.ExecuteC()
	if err != nil {
		os.Exit(1)
	}

	maybeNotifyAboutUpdate(executedCmd)
}

func maybeNotifyAboutUpdate(cmd *cobra.Command) {
	if !shouldCheckForUpdates(cmd) {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	result, err := updater.New().Check(ctx, Version)
	if err != nil {
		if verbose {
			fmt.Fprintf(os.Stderr, "Update check failed: %v\n", err)
		}
		return
	}
	if !result.ShouldNotify {
		return
	}

	fmt.Fprintf(os.Stderr, "A new Kavla CLI version is available: %s (current: %s). Run: kavla update\n", result.LatestVersion, result.CurrentVersion)
}

func shouldCheckForUpdates(cmd *cobra.Command) bool {
	if cmd == nil || cmd == rootCmd {
		return false
	}
	if os.Getenv("KAVLA_NO_UPDATE_CHECK") == "1" {
		return false
	}
	if !updater.IsManagedVersion(Version) {
		return false
	}
	if !term.IsTerminal(int(os.Stderr.Fd())) {
		return false
	}

	name := strings.TrimSpace(cmd.Name())
	switch {
	case name == "help":
		return false
	case name == "version":
		return false
	case name == "update":
		return false
	case name == "upgrade":
		return false
	case strings.HasPrefix(name, "__complete"):
		return false
	default:
		return true
	}
}

func init() {
	rootCmd.PersistentFlags().BoolVarP(&verbose, "verbose", "v", false, "Enable verbose log output")
}
