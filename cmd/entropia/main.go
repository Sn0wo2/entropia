package main

import (
	"context"
	"errors"
	"os"
	"os/signal"
	"syscall"

	"github.com/Sn0wo2/caelum"
	"github.com/Sn0wo2/entropia/internal/cli"
)

func main() {
	logger := caelum.New(caelum.Config{
		Level:   caelum.LevelInfo,
		Targets: []caelum.Target{{Writer: os.Stdout, Format: caelum.JSON}},
	})
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ctx.Done()
		cancel()
	}()
	err := cli.Execute(ctx, os.Args[1:], logger)
	cancel()
	if err != nil {
		logger.Error("entropia failed", "error", err)
		if errors.Is(err, context.Canceled) {
			os.Exit(130)
		}
		os.Exit(1)
	}
}
