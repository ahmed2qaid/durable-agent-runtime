package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"sort"

	"github.com/ahmed2qaid/durable-agent-runtime/durable"
)

func main() {
	if len(os.Args) < 3 {
		usage()
		os.Exit(2)
	}
	command := os.Args[1]
	path := os.Args[2]
	snapshot, err := loadSnapshot(path)
	if err != nil {
		fmt.Fprintln(os.Stderr, "durablectl:", err)
		os.Exit(1)
	}

	switch command {
	case "inspect":
		inspect(snapshot)
	case "events":
		printEvents(snapshot)
	case "fork-plan":
		forkPlan(snapshot, os.Args[3:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: durablectl <inspect|events|fork-plan> snapshot.json [options]")
}

func loadSnapshot(path string) (durable.RunSnapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return durable.RunSnapshot{}, err
	}
	var snapshot durable.RunSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return durable.RunSnapshot{}, err
	}
	if snapshot.Format != durable.SnapshotFormat {
		return durable.RunSnapshot{}, fmt.Errorf("unsupported snapshot format: %s", snapshot.Format)
	}
	return snapshot, nil
}

func inspect(snapshot durable.RunSnapshot) {
	fmt.Printf("run: %s\n", snapshot.Run.RunID)
	fmt.Printf("status: %s\n", snapshot.Run.Status)
	fmt.Printf("steps: %d\n", len(snapshot.Steps))
	fmt.Printf("events: %d\n", len(snapshot.Events))
	fmt.Printf("dead_letters: %d\n", len(snapshot.DeadLetters))
	if snapshot.Run.LastError != "" {
		fmt.Printf("last_error: %s\n", snapshot.Run.LastError)
	}
}

func printEvents(snapshot durable.RunSnapshot) {
	events := append([]durable.RunEvent(nil), snapshot.Events...)
	sort.Slice(events, func(i, j int) bool { return events[i].Sequence < events[j].Sequence })
	for _, event := range events {
		fmt.Printf("%04d %-24s %-20s %s\n", event.Sequence, event.Type, event.StepKey, string(event.Payload))
	}
}

func forkPlan(snapshot durable.RunSnapshot, args []string) {
	fs := flag.NewFlagSet("fork-plan", flag.ExitOnError)
	through := fs.String("through", "", "completed step key to fork through")
	_ = fs.Parse(args)
	if *through == "" {
		fmt.Fprintln(os.Stderr, "durablectl fork-plan: --through is required")
		os.Exit(2)
	}

	steps := append([]durable.StepRecord(nil), snapshot.Steps...)
	sort.SliceStable(steps, func(i, j int) bool { return steps[i].UpdatedAt.Before(steps[j].UpdatedAt) })
	found := false
	for _, step := range steps {
		if step.Status != durable.StepCompleted {
			continue
		}
		fmt.Printf("copy %s attempts=%d\n", step.StepKey, step.Attempts)
		if step.StepKey == *through {
			found = true
			break
		}
	}
	if !found {
		fmt.Fprintf(os.Stderr, "checkpoint not found: %s\n", *through)
		os.Exit(1)
	}
}
