package main

import (
	"os"

	"github.com/adevinta/vulcan-agent/cmd"
	"github.com/adevinta/vulcan-agent/kubernetes"
)

func main() {
	// NOTE: This is done in order to be able to return custom exit codes
	// while still executing deferred functions as expected.
	// Using os.Exit inside the main function is not an option:
	// https://golang.org/pkg/os/#Exit
	os.Exit(cmd.MainWithExitCode(kubernetes.NewAgent))
}
