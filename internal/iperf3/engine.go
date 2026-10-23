package iperf3

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Sn0wo2/entropia/internal/transfer"
	"github.com/Sn0wo2/entropia/internal/utils"
)

type Engine struct {
	URL  string
	Path string

	bin        string
	host       string
	port       string
	probe      sync.Once
	probeErr   error
	jsonStream bool
}

func (e *Engine) Init() error {
	u, err := url.Parse(e.URL)
	if err != nil || u.Scheme != "iperf3" || u.Hostname() == "" || u.User != nil || (u.Path != "" && u.Path != "/") || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" {
		return errors.New("iperf3 URL must be iperf3://host[:port] without credentials, path, query or fragment")
	}

	port := u.Port()
	if port == "" {
		port = "5201"
	}

	portNumber, err := strconv.Atoi(port)
	if err != nil || portNumber < 1 || portNumber > 65535 {
		return errors.New("iperf3 port must be between 1 and 65535")
	}

	if e.Path == "" {
		e.Path = "iperf3"
	}

	bin, err := exec.LookPath(e.Path)
	if err != nil {
		return fmt.Errorf("iperf3 executable not found; install it or use --iperf-path: %w", err)
	}

	e.bin, e.host, e.port = bin, u.Hostname(), port

	return nil
}

func (e *Engine) Transfer(ctx context.Context, job transfer.Job, meter *transfer.Meter) error {
	if job.Direction != transfer.Download && job.Direction != transfer.Upload {
		return fmt.Errorf("unsupported iperf3 direction %q", job.Direction)
	}

	counter := &meter.Uploaded
	if job.Direction == transfer.Download {
		counter = &meter.Downloaded
	}

	if job.Concurrency != 1 {
		return errors.New("iperf3 adapter currently supports only --concurrency 1")
	}

	if job.Rate < 0 || job.Rate > math.MaxInt64/8 {
		return errors.New("iperf3 rate is outside the supported bytes/s range")
	}

	e.probe.Do(func() {
		probeCtx, cancel := context.WithTimeout(ctx, min(job.Timeout, 5*time.Second))
		defer cancel()

		cmd := utils.Command(probeCtx, e.bin, "--help")
		output := &utils.BoundedBuffer{Limit: 64 * 1024}

		cmd.Stdout, cmd.Stderr = output, output
		if err := cmd.Run(); err != nil {
			e.probeErr = fmt.Errorf("iperf3 capability discovery: %w", err)
			if probeCtx.Err() != nil {
				e.probeErr = probeCtx.Err()
			}

			return
		}

		e.jsonStream = bytes.Contains(output.Data, []byte("--json-stream"))
	})

	if e.probeErr != nil {
		return e.probeErr
	}

	for ctx.Err() == nil {
		args := []string{"-c", e.host, "-p", e.port, "-P", "1"}
		if job.Direction == transfer.Download {
			args = append(args, "-R")
		}

		if job.Loop {
			chunk := 5 * time.Second
			if e.jsonStream {
				chunk = 30 * time.Second
			}

			if job.Timeout <= time.Second {
				return errors.New("iperf3 loop requires --timeout greater than 1s")
			}

			chunk = min(chunk, job.Timeout-time.Second)
			if deadline, ok := ctx.Deadline(); ok {
				chunk = min(chunk, max(time.Second, time.Until(deadline)))
			}

			args = append(args, "-t", strconv.FormatInt(max(1, int64(chunk/time.Second)), 10))
		} else {
			args = append(args, "-n", strconv.FormatInt(job.Bytes, 10), "-l", strconv.FormatInt(min(job.Bytes, 32*1024), 10))
		}

		if job.Rate > 0 {
			args = append(args, "-b", strconv.FormatInt(job.Rate*8, 10))
		}

		if e.jsonStream {
			args = append(args, "--json-stream", "--forceflush", "-i", "1")
		} else {
			args = append(args, "-J", "-i", "0")
		}

		err := func() error {
			type byteCount struct {
				Bytes int64 `json:"bytes"`
			}

			type summary struct {
				Sum         *byteCount `json:"sum"`
				SumSent     *byteCount `json:"sum_sent"`
				SumReceived *byteCount `json:"sum_received"`
				Error       string     `json:"error"`
			}

			attemptCtx, cancel := context.WithTimeout(ctx, job.Timeout)
			defer cancel()

			cmd := utils.Command(attemptCtx, e.bin, args...)

			stdout, err := cmd.StdoutPipe()
			if err != nil {
				return err
			}

			stderr := &utils.BoundedBuffer{Limit: 8 * 1024}
			cmd.Stderr = stderr

			meter.Attempts.Add(1)

			if err := cmd.Start(); err != nil {
				return fmt.Errorf("start iperf3: %w", err)
			}

			reader := &io.LimitedReader{R: stdout}
			decoder := json.NewDecoder(reader)

			var parseErr error

			var (
				reported      int64
				final         *summary
				protocolError string
			)

			for {
				reader.N = 64 * 1024

				var message struct {
					Event string          `json:"event"`
					Data  json.RawMessage `json:"data"`
					End   *summary        `json:"end"`
					Error string          `json:"error"`
				}
				if err := decoder.Decode(&message); err != nil {
					if !errors.Is(err, io.EOF) {
						parseErr = fmt.Errorf("parse bounded iperf3 JSON output: %w", err)

						cancel()
					}

					break
				}

				if message.Error != "" {
					protocolError = message.Error
				}

				if message.End != nil {
					final = message.End
				}

				if message.Event == "error" && len(message.Data) > 0 && message.Data[0] == '"' {
					if err := json.Unmarshal(message.Data, &protocolError); err != nil {
						parseErr = err

						cancel()

						break
					}

					continue
				}

				if message.Event != "interval" && message.Event != "end" && message.Event != "error" {
					continue
				}

				var data summary
				if err := json.Unmarshal(message.Data, &data); err != nil {
					parseErr = err

					cancel()

					break
				}

				if data.Error != "" {
					protocolError = data.Error
				}

				if message.Event == "end" {
					final = &data
				} else if message.Event == "interval" && data.Sum != nil && data.Sum.Bytes > 0 {
					reported += data.Sum.Bytes
					counter.Add(data.Sum.Bytes)
				}
			}

			waitErr := cmd.Wait()

			if final != nil {
				count := final.SumSent
				if job.Direction == transfer.Download {
					count = final.SumReceived
				}

				if count != nil && count.Bytes > reported {
					counter.Add(count.Bytes - reported)
					reported = count.Bytes
				}
			}

			if err := ctx.Err(); err != nil {
				return err
			}

			if parseErr != nil {
				return parseErr
			}

			if attemptCtx.Err() != nil {
				return fmt.Errorf("iperf3 operation timed out: %w", attemptCtx.Err())
			}

			if protocolError != "" {
				return fmt.Errorf("iperf3: %s", protocolError)
			}

			if waitErr != nil {
				return fmt.Errorf("iperf3: %w: %.8192s", waitErr, strings.TrimSpace(string(stderr.Data)))
			}

			if final == nil || reported == 0 || (!job.Loop && reported < job.Bytes) {
				return errors.New("iperf3 did not report a completed transfer matching the byte goal")
			}

			if job.Direction == transfer.Upload && final.SumReceived != nil {
				meter.ConfirmedUpload.Add(min(reported, max(0, final.SumReceived.Bytes)))
			}

			return nil
		}()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}

			meter.Failures.Add(1)

			return err
		}

		if !job.Loop {
			return nil
		}
	}

	return ctx.Err()
}
