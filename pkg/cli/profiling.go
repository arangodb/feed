package cli

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"

	"github.com/pkg/errors"
	"github.com/spf13/cobra"
)

// RunCommandWithProfile enables profile for CPU/Memory if env variables requests so
func RunCommandWithProfile(cmd *cobra.Command) error {
	// Turn on CPU profiling before the command is launched.
	if filename := os.Getenv("PPROF_CPU_FILENAME"); len(filename) > 0 {
		f, err := os.Create(filename)
		if err != nil {
			fmt.Printf("could not create CPU profile filename %s\n", filename)
			return nil
		}
		defer f.Close()

		if err := pprof.StartCPUProfile(f); err != nil {
			fmt.Println("could not start CPU profile")
			return nil
		}
		defer pprof.StopCPUProfile()
	}

	errExecute := cmd.Execute()

	// Close memory profiling after the command.
	if err := profileMemory(os.Getenv("PPROF_MEMORY_FILENAME")); err != nil {
		fmt.Printf("profileMemory failed: %s\n", err)
		if errExecute == nil {
			// When command does not fail then memory profile error can be returned.
			return err
		}
	}

	return errExecute
}

// profileMemory reports memory usage in the given file.
func profileMemory(filename string) error {
	if len(filename) == 0 {
		return nil
	}

	f, err := os.Create(filename)
	if err != nil {
		return errors.WithMessagef(err, "could not create memory profile filename %s", filename)
	}
	defer f.Close()

	// Get up-to-date statistics.
	runtime.GC()
	if err := pprof.WriteHeapProfile(f); err != nil {
		return errors.WithMessagef(err, "could not write memory profile for filename %s", filename)
	}

	return nil
}
