package utils

import (
	"context"
	"os/exec"
	"time"

	"github.com/jesseduffield/kill"
)

type Process struct{}

func (Process) Command(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.WaitDelay = time.Second
	kill.PrepareForChildren(cmd)
	cmd.Cancel = func() error {
		_ = kill.Kill(cmd)
		return cmd.Process.Kill()
	}
	return cmd
}
