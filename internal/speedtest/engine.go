package speedtest

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"sync"

	trafficHTTP "github.com/Sn0wo2/entropia/internal/http"
	"github.com/Sn0wo2/entropia/internal/transfer"
)

type Engine struct {
	URL     string
	NodeID  string
	Headers http.Header

	mu    sync.Mutex
	inner *trafficHTTP.Engine
}

func (e *Engine) Init() error {
	e.URL = strings.TrimSpace(e.URL)
	if e.URL != "" && e.NodeID != "" {
		return errors.New("speedtest legacy HTTP nodes are unofficial and experimental: --node and --url are mutually exclusive")
	}
	if e.URL != "" {
		u, err := url.Parse(e.URL)
		if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.Fragment != "" {
			return errors.New("speedtest legacy HTTP nodes are unofficial and experimental: --url must be an absolute HTTP node upload endpoint")
		}
	}
	e.Headers = e.Headers.Clone()
	return nil
}

func (e *Engine) Transfer(ctx context.Context, job transfer.Job, meter *transfer.Meter) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.inner == nil {
		upload := e.URL
		if upload == "" {
			discoveryCtx, cancel := context.WithTimeout(ctx, job.Timeout)
			nodes, err := Discover(discoveryCtx)
			cancel()
			if err != nil {
				return err
			}
			for _, node := range nodes {
				u, err := url.Parse(node.URL)
				if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") || u.Fragment != "" {
					continue
				}
				if e.NodeID != "" {
					if node.ID == e.NodeID {
						upload = node.URL
						break
					}
				} else {
					if upload == "" || u.Scheme == "https" {
						upload = node.URL
					}
					if u.Scheme == "https" {
						break
					}
				}
			}
			if upload == "" {
				return errors.New("speedtest legacy HTTP nodes are unofficial and experimental: no matching HTTP node in discovery results; specify --url")
			}
		}
		u, _ := url.Parse(upload)
		dir := u.Path
		if dir == "" {
			dir = "/"
		}
		if !strings.HasSuffix(dir, "/") {
			if i := strings.LastIndexByte(dir, '/'); i >= 0 && strings.Contains(dir[i+1:], ".") {
				dir = dir[:i+1]
			} else {
				dir += "/"
			}
		}
		u.Path, u.RawPath, u.RawQuery, u.Fragment = dir+"random4000x4000.jpg", "", "", ""
		inner := &trafficHTTP.Engine{DownloadURL: u.String(), UploadURL: upload, Headers: e.Headers}
		if err := inner.Init(); err != nil {
			return fmt.Errorf("speedtest legacy HTTP nodes are unofficial and experimental: HTTP setup: %w", err)
		}
		e.inner = inner
	}
	if err := e.inner.Transfer(ctx, job, meter); err != nil {
		return fmt.Errorf("speedtest legacy HTTP nodes are unofficial and experimental: %s: %w", job.Direction, err)
	}
	return nil
}
