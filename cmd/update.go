package cmd

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/aleda145/kavla-cli/internal/updater"
	"github.com/spf13/cobra"
)

var updateCmd = &cobra.Command{
	Use:     "update",
	Aliases: []string{"upgrade"},
	Short:   "Update the Kavla CLI",
	Run: func(cmd *cobra.Command, args []string) {
		ctx := cmd.Context()
		if ctx == nil {
			ctx = context.Background()
		}

		updateCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		result, err := updater.New().Update(updateCtx, Version)
		if err != nil {
			switch {
			case errors.Is(err, updater.ErrSourceBuild):
				fmt.Fprintln(cmd.ErrOrStderr(), "kavla update is unavailable for source builds.")
			case errors.Is(err, updater.ErrUnsupportedPlatform):
				fmt.Fprintf(cmd.ErrOrStderr(), "Update failed: %v\n", err)
			case errors.Is(err, updater.ErrManualInstallRequired):
				fmt.Fprintf(cmd.ErrOrStderr(), "Update failed: %v\nPlease reinstall kavla manually.\n", err)
			default:
				fmt.Fprintf(cmd.ErrOrStderr(), "Update failed: %v\n", err)
			}
			return
		}

		if !result.Updated {
			fmt.Fprintln(cmd.OutOrStdout(), "kavla is already up to date")
			return
		}

		fmt.Fprintf(cmd.OutOrStdout(), "Updated kavla from %s to %s\n", result.PreviousVersion, result.NewVersion)
		if result.NotesURL != "" {
			fmt.Fprintf(cmd.OutOrStdout(), "Release notes: %s\n", result.NotesURL)
		}
	},
}

func init() {
	rootCmd.AddCommand(updateCmd)
}
