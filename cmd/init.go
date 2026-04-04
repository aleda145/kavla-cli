package cmd

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/aleda145/kavla-cli/internal/auth"
	"github.com/spf13/cobra"
	"golang.org/x/term"
)

type initPrompter interface {
	Confirm(label string) (bool, error)
}

type terminalInitPrompter struct{}

func newTerminalInitPrompter() *terminalInitPrompter {
	return &terminalInitPrompter{}
}

func (p *terminalInitPrompter) Confirm(label string) (bool, error) {
	oldState, err := term.MakeRaw(int(os.Stdin.Fd()))
	if err != nil {
		return false, err
	}
	defer term.Restore(int(os.Stdin.Fd()), oldState)

	fmt.Printf("%s [Y/n]: ", label)

	buf := make([]byte, 1)
	for {
		n, err := os.Stdin.Read(buf)
		if err != nil {
			fmt.Print("\r\n")
			return false, err
		}
		if n != 1 {
			continue
		}

		switch parseInitConfirmByte(buf[0]) {
		case initConfirmYes:
			if buf[0] == '\r' || buf[0] == '\n' {
				fmt.Print("\r\n")
			} else {
				fmt.Printf("%c\r\n", buf[0])
			}
			return true, nil
		case initConfirmNo:
			fmt.Printf("%c\r\n", buf[0])
			return false, nil
		case initConfirmCancelled:
			fmt.Print("\r\n")
			return false, errPromptCancelled
		default:
		}
	}
}

type initConfirmAnswer int

const (
	initConfirmInvalid initConfirmAnswer = iota
	initConfirmYes
	initConfirmNo
	initConfirmCancelled
)

func parseInitConfirmByte(answer byte) initConfirmAnswer {
	switch answer {
	case '\r', '\n':
		return initConfirmYes
	case 'y', 'Y':
		return initConfirmYes
	case 'n', 'N':
		return initConfirmNo
	case 3:
		return initConfirmCancelled
	default:
		return initConfirmInvalid
	}
}

var (
	runInitAddSourceStep = func(interactive bool) error {
		return runAddSourceWithPrompter(newTerminalSourcePrompter(), interactive)
	}
	runInitLoginStep   = runLogin
	runInitCanConnect  = canInitConnect
	runInitConnectStep = func() error {
		return runConnect(nil, "https://app.kavla.dev", false)
	}
)

var initCmd = &cobra.Command{
	Use:   "init",
	Short: "Set up Kavla CLI for first use",
	Args:  cobra.NoArgs,
	Run: func(cmd *cobra.Command, args []string) {
		interactive := term.IsTerminal(int(os.Stdin.Fd())) && term.IsTerminal(int(os.Stdout.Fd()))
		if err := runInitWithPrompter(newTerminalInitPrompter(), interactive); err != nil {
			fmt.Println(err)
		}
	},
}

func runInitWithPrompter(prompter initPrompter, interactive bool) error {
	if !interactive {
		return fmt.Errorf("kavla init must be run in a terminal")
	}

	fmt.Println("Kavla CLI initialization")
	fmt.Println()

	setupSource, err := prompter.Confirm("Set up a data source?")
	if err != nil {
		return wrapInitPromptError(err)
	}
	if setupSource {
		if err := runInitAddSourceStep(interactive); err != nil {
			if !isInitSourceSetupCancelled(err) {
				return err
			}
		}
	}

	loginNow, err := prompter.Confirm("Log in?")
	if err != nil {
		return wrapInitPromptError(err)
	}
	if loginNow {
		if err := runInitLoginStep(); err != nil {
			if !isInitLoginCancelled(err) {
				return err
			}
		}
	}

	canConnect, err := runInitCanConnect()
	if err != nil {
		return err
	}
	if canConnect {
		connectNow, err := prompter.Confirm("Connect to a canvas?")
		if err != nil {
			return wrapInitPromptError(err)
		}
		if connectNow {
			if err := runInitConnectStep(); err != nil {
				if !isInitConnectCancelled(err) {
					return err
				}
			}
		}
	}

	return nil
}

func runInit(interactive bool) error {
	return runInitWithPrompter(newTerminalInitPrompter(), interactive)
}

func canInitConnect() (bool, error) {
	config, err := auth.LoadConfigAllowMissing()
	if err != nil {
		return false, fmt.Errorf("load config: %w", err)
	}
	return strings.TrimSpace(config.Token) != "", nil
}

func wrapInitPromptError(err error) error {
	if err == nil {
		return nil
	}
	if err == errPromptCancelled {
		return fmt.Errorf("init cancelled")
	}
	return err
}

func isInitSourceSetupCancelled(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errPromptCancelled) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "source setup cancelled")
}

func isInitConnectCancelled(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errPromptCancelled) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "cancelled")
}

func isInitLoginCancelled(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, errPromptCancelled) {
		return true
	}
	return strings.Contains(strings.ToLower(err.Error()), "login cancelled")
}

func init() {
	initCmd.Flags().StringVar(&sourceName, "name", "", "Name of the source")
	initCmd.Flags().StringVar(&sourceType, "type", "", "Type of source (duckdb, directory, bigquery, postgres)")
	initCmd.Flags().StringVar(&sourceConnection, "connection", "", "Connection string, file path, or project ID")

	rootCmd.AddCommand(initCmd)
}
