package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/Sn0wo2/caelum"
	trafficHTTP "github.com/Sn0wo2/entropia/internal/http"
	"github.com/Sn0wo2/entropia/internal/iperf3"
	"github.com/Sn0wo2/entropia/internal/speedtest"
	"github.com/Sn0wo2/entropia/internal/transfer"
	"github.com/Sn0wo2/entropia/internal/utils"

	"github.com/urfave/cli/v3"
)

func Execute(ctx context.Context, args []string, logger *caelum.Logger) error {
	return (&cli.Command{
		Name:                      "entropia",
		Writer:                    io.Discard,
		ErrWriter:                 io.Discard,
		DisableSliceFlagSeparator: true,
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "type", Category: "Target", Value: "http", Usage: "protocol: http, speedtest (experimental), iperf3 (external, approximate quota)"},
			&cli.StringFlag{Name: "url", Category: "Target", Usage: "target URL"},
			&cli.StringFlag{Name: "node", Category: "Target", Usage: "Speedtest node ID (alternative to URL)"},
			&cli.StringFlag{Name: "iperf-path", Category: "Target", Value: "iperf3", Usage: "iperf3 executable path"},

			&cli.StringFlag{Name: "download", Category: "Transfer", Value: "0", Usage: "download amount in MB, or a size such as 500MiB"},
			&cli.StringFlag{Name: "upload", Category: "Transfer", Value: "0", Usage: "upload amount in MB, or a size such as 100MiB"},
			&cli.BoolFlag{Name: "loop", Category: "Transfer", Usage: "repeat until interrupted or duration expires"},
			&cli.StringFlag{Name: "direction", Category: "Transfer", Usage: "loop direction: download or upload (otherwise inferred)"},
			&cli.DurationFlag{Name: "duration", Category: "Transfer", Usage: "overall duration limit, e.g. 30s or 10m"},
			&cli.DurationFlag{Name: "timeout", Category: "Transfer", Value: 2 * time.Minute, Usage: "timeout for each HTTP request or iperf3 operation"},
			&cli.IntFlag{Name: "concurrency", Category: "Transfer", Value: 1, Usage: "parallel HTTP workers (default 1)"},
			&cli.StringFlag{Name: "rate", Category: "Transfer", Value: "0", Usage: "aggregate rate in bytes/s, e.g. 10MB/s; 0 disables limiting"},

			&cli.StringFlag{Name: "download-url", Category: "HTTP", Usage: "HTTP download URL override"},
			&cli.StringFlag{Name: "upload-url", Category: "HTTP", Usage: "HTTP upload URL override"},
			&cli.StringFlag{Name: "method", Category: "HTTP", Usage: "HTTP method (default GET for download, POST for upload)"},
			&cli.StringFlag{Name: "body", Category: "HTTP", Usage: "literal body, repeated whole; final request may exceed upload target"},
			&cli.StringFlag{Name: "body-file", Category: "HTTP", Usage: "stream and repeat a whole file body; final request may exceed upload target"},
			&cli.StringFlag{Name: "chunk", Category: "HTTP", Value: "8MiB", Usage: "maximum generated upload body or download bytes per request"},
			&cli.StringSliceFlag{Name: "header", Category: "HTTP", Usage: "HTTP header 'Name: value'; repeatable"},

			&cli.BoolFlag{Name: "quiet", Category: "Output", Usage: "disable progress logs"},
		},
		Commands: []*cli.Command{{
			Name: "nodes",
			Flags: []cli.Flag{
				&cli.StringFlag{Name: "type", Value: "speedtest", Usage: "node discovery type (speedtest)"},
			},
			Action: func(ctx context.Context, cmd *cli.Command) error {
				if cmd.String("type") != "speedtest" || cmd.Args().Len() != 0 {
					return errors.New("nodes supports only --type speedtest and no positional arguments")
				}

				ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
				defer cancel()

				nodes, err := speedtest.Discover(ctx)
				if err != nil {
					return err
				}

				logger.Info("speedtest nodes", "nodes", nodes)

				return nil
			},
		}},
		Action: func(ctx context.Context, cmd *cli.Command) error {
			return executeTransfer(ctx, cmd, logger)
		},
	}).Run(ctx, append([]string{"entropia"}, args...))
}

type transferOptions struct {
	plan     transfer.Plan
	chunk    int64
	method   string
	headers  http.Header
	progress func(transfer.Snapshot)
}

func executeTransfer(ctx context.Context, cmd *cli.Command, logger *caelum.Logger) error {
	options, err := parseTransferOptions(cmd, logger)
	if err != nil {
		return err
	}

	engine, err := createEngine(cmd, options)
	if err != nil {
		return err
	}

	if err := engine.Init(); err != nil {
		return err
	}

	report, err := transfer.Run(ctx, engine, options.plan, options.progress)
	logger.Info("transfer result", "report", report)

	return err
}

func parseTransferOptions(cmd *cli.Command, logger *caelum.Logger) (transferOptions, error) {
	if cmd.Args().Len() != 0 {
		return transferOptions{}, fmt.Errorf("unexpected positional argument %q", cmd.Args().First())
	}

	headers := http.Header{}

	for _, value := range cmd.StringSlice("header") {
		name, headerValue, ok := strings.Cut(value, ":")
		if !ok {
			return transferOptions{}, errors.New("header must be 'Name: value'")
		}

		headers.Add(strings.TrimSpace(name), strings.TrimSpace(headerValue))
	}

	if cmd.IsSet("body") && cmd.String("body-file") != "" {
		return transferOptions{}, errors.New("--body and --body-file are mutually exclusive")
	}

	plan := transfer.Plan{
		Loop: cmd.Bool("loop"), Concurrency: cmd.Int("concurrency"),
		Duration: cmd.Duration("duration"), Timeout: cmd.Duration("timeout"),
	}

	var err error
	if plan.Download, err = utils.ParseSize(cmd.String("download"), "MB"); err != nil {
		return transferOptions{}, fmt.Errorf("--download: %w", err)
	}

	if plan.Upload, err = utils.ParseSize(cmd.String("upload"), "MB"); err != nil {
		return transferOptions{}, fmt.Errorf("--upload: %w", err)
	}

	if plan.Rate, err = utils.ParseSize(strings.TrimSuffix(strings.ToUpper(strings.TrimSpace(cmd.String("rate"))), "/S"), "B"); err != nil {
		return transferOptions{}, fmt.Errorf("--rate: %w", err)
	}

	chunk, err := utils.ParseSize(cmd.String("chunk"), "B")
	if err != nil || chunk == 0 {
		return transferOptions{}, errors.New("--chunk must be a positive byte size")
	}

	method := strings.ToUpper(strings.TrimSpace(cmd.String("method")))

	plan.Direction = transfer.Direction(cmd.String("direction"))
	if plan.Loop && plan.Direction == "" {
		plan.Direction = transfer.Download
		if cmd.IsSet("body") || cmd.String("body-file") != "" || (method != "" && method != http.MethodGet && method != http.MethodHead) {
			plan.Direction = transfer.Upload
		}
	}

	if cmd.IsSet("direction") && !plan.Loop {
		return transferOptions{}, errors.New("--direction requires --loop")
	}

	if err := plan.Validate(); err != nil {
		return transferOptions{}, err
	}

	var progress func(transfer.Snapshot)
	if !cmd.Bool("quiet") {
		progress = func(s transfer.Snapshot) {
			logger.Info("transfer progress", "download_bytes", s.Downloaded, "upload_bytes", s.Uploaded, "elapsed_seconds", s.Elapsed.Seconds())
		}
	}

	return transferOptions{plan: plan, chunk: chunk, method: method, headers: headers, progress: progress}, nil
}

func createEngine(cmd *cli.Command, options transferOptions) (transfer.Engine, error) {
	switch cmd.String("type") {
	case "http":
		if cmd.IsSet("node") || cmd.IsSet("iperf-path") {
			return nil, errors.New("--node requires speedtest; --iperf-path requires iperf3")
		}

		return &trafficHTTP.Engine{
			URL: cmd.String("url"), DownloadURL: cmd.String("download-url"), UploadURL: cmd.String("upload-url"),
			Method: options.method, Headers: options.headers, Body: cmd.String("body"), BodySet: cmd.IsSet("body"),
			BodyFile: cmd.String("body-file"), ChunkSize: options.chunk,
		}, nil
	case "speedtest":
		for _, name := range []string{"download-url", "upload-url", "method", "body", "body-file", "chunk", "iperf-path"} {
			if cmd.IsSet(name) {
				return nil, fmt.Errorf("--%s is a generic HTTP or iperf3 option; not supported by speedtest", name)
			}
		}

		return &speedtest.Engine{URL: cmd.String("url"), NodeID: cmd.String("node"), Headers: options.headers}, nil
	case "iperf3":
		for _, name := range []string{"download-url", "upload-url", "method", "body", "body-file", "header", "chunk", "node"} {
			if cmd.IsSet(name) {
				return nil, fmt.Errorf("--%s is not supported by iperf3", name)
			}
		}

		return &iperf3.Engine{URL: cmd.String("url"), Path: cmd.String("iperf-path")}, nil
	default:
		return nil, fmt.Errorf("unsupported --type %q; use http, speedtest, or iperf3", cmd.String("type"))
	}
}
