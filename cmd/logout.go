package cmd

import (
	"fmt"

	"github.com/aleda145/kavla-cli/internal/auth"
	"github.com/spf13/cobra"
)

var logoutCmd = &cobra.Command{
	Use:   "logout",
	Short: "Log out of Kavla",
	Run: func(cmd *cobra.Command, args []string) {
		if err := auth.SaveToken(""); err != nil {
			fmt.Printf("Error clearing token: %v\n", err)
			return
		}
		fmt.Println("Logged out.")
	},
}

func init() {
	rootCmd.AddCommand(logoutCmd)
}
