package http

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Sn0wo2/entropia/internal/transfer"
	"github.com/Sn0wo2/entropia/internal/utils"

	"golang.org/x/net/http/httpguts"
	"golang.org/x/time/rate"
)

type Engine struct {
	URL         string
	DownloadURL string
	UploadURL   string
	Method      string
	Headers     http.Header
	Body        string
	BodySet     bool
	BodyFile    string
	ChunkSize   int64

	length int64
}

func (e *Engine) Init() error {
	if e.DownloadURL == "" {
		e.DownloadURL = e.URL
	}
	if e.UploadURL == "" {
		e.UploadURL = e.URL
	}
	if e.DownloadURL == "" && e.UploadURL == "" {
		return errors.New("specify --url, --download-url, or --upload-url")
	}
	e.Method = strings.ToUpper(strings.TrimSpace(e.Method))
	for _, raw := range []string{e.DownloadURL, e.UploadURL} {
		if raw == "" {
			continue
		}
		u, err := url.Parse(raw)
		if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.Fragment != "" {
			return fmt.Errorf("invalid HTTP URL %q", raw)
		}
	}
	if e.BodySet && e.BodyFile != "" {
		return errors.New("--body and --body-file are mutually exclusive")
	}
	if e.ChunkSize == 0 {
		e.ChunkSize = 8 * 1024 * 1024
	}
	if e.ChunkSize < 1 {
		return errors.New("chunk size must be positive")
	}
	e.Headers = e.Headers.Clone()
	for name, values := range e.Headers {
		if !httpguts.ValidHeaderFieldName(name) {
			return fmt.Errorf("invalid header name %q", name)
		}
		if strings.EqualFold(name, "Content-Length") || strings.EqualFold(name, "Transfer-Encoding") {
			return fmt.Errorf("%s is managed by the streaming body", name)
		}
		for _, value := range values {
			if !httpguts.ValidHeaderFieldValue(value) {
				return fmt.Errorf("invalid header value for %s", name)
			}
		}
	}
	e.length = int64(len(e.Body))
	if e.BodyFile != "" {
		info, err := os.Stat(e.BodyFile)
		if err != nil {
			return fmt.Errorf("body file: %w", err)
		}
		if !info.Mode().IsRegular() {
			return errors.New("body file must be a regular file")
		}
		e.length = info.Size()
	}
	return nil
}

func (e *Engine) Transfer(ctx context.Context, job transfer.Job, meter *transfer.Meter) error {
	if job.Direction == transfer.Download && e.Method == http.MethodHead {
		return errors.New("HEAD has no response body and cannot consume download payload")
	}
	endpoint := e.DownloadURL
	if job.Direction == transfer.Upload {
		endpoint = e.UploadURL
		if (e.BodySet || e.BodyFile != "") && e.length == 0 {
			return errors.New("an empty custom body cannot consume upload traffic")
		}
	}
	if endpoint == "" {
		return fmt.Errorf("missing URL for %s", job.Direction)
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.DialContext = (&net.Dialer{Timeout: job.Timeout, KeepAlive: 30 * time.Second}).DialContext
	transport.MaxConnsPerHost, transport.MaxIdleConnsPerHost = job.Concurrency, job.Concurrency
	transport.MaxIdleConns = job.Concurrency
	transport.MaxResponseHeaderBytes = 64 * 1024
	transport.DisableCompression = true
	transport.ForceAttemptHTTP2 = true
	transport.ResponseHeaderTimeout = job.Timeout
	transport.TLSHandshakeTimeout = job.Timeout
	defer transport.CloseIdleConnections()
	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return errors.New("too many redirects")
			}
			if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
				return errors.New("unsupported redirect scheme")
			}
			if job.Direction == transfer.Upload && req.Method != via[0].Method {
				return errors.New("upload redirect would discard the body; use the final upload URL")
			}
			first := via[0].URL
			if req.URL.Scheme != first.Scheme || !strings.EqualFold(req.URL.Host, first.Host) {
				for name := range req.Header {
					switch http.CanonicalHeaderKey(name) {
					case "Accept", "Accept-Encoding", "Accept-Language", "User-Agent", "Range", "If-Range":
					default:
						req.Header.Del(name)
					}
				}
				req.Host = ""
			}
			return nil
		},
	}
	parentCtx := ctx
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	remaining := job.Bytes
	var budgetMu sync.Mutex
	var limiter *rate.Limiter
	if job.Rate > 0 {
		burst := int(min(32*1024, max(1, job.Rate/10)))
		limiter = rate.NewLimiter(rate.Limit(job.Rate), burst)
		limiter.AllowN(time.Now(), burst)
	}
	whole := job.Direction == transfer.Upload && (e.BodySet || e.BodyFile != "")
	maximum := e.ChunkSize
	if whole {
		maximum = e.length
	} else if !job.Loop {
		maximum = min(maximum, max(1, job.Bytes/int64(job.Concurrency)))
	}
	var wg sync.WaitGroup
	var once sync.Once
	var result error
	for range job.Concurrency {
		wg.Go(func() {
			buffer := make([]byte, 32*1024)
			if !e.BodySet && e.BodyFile == "" && job.Direction == transfer.Upload {
				if _, err := rand.Read(buffer); err != nil {
					once.Do(func() { result = err; cancel() })
					return
				}
			}
			failures := 0
			for ctx.Err() == nil {
				reserved := maximum
				if !job.Loop {
					budgetMu.Lock()
					if remaining <= 0 {
						reserved = 0
					} else {
						if !whole {
							reserved = min(reserved, remaining)
						}
						remaining -= reserved
					}
					budgetMu.Unlock()
				}
				if reserved == 0 {
					return
				}
				actual, retry, err := func() (int64, bool, error) {
					ctx, cancel := context.WithTimeout(ctx, job.Timeout)
					defer cancel()
					method := e.Method
					if method == "" {
						method = http.MethodGet
						if job.Direction == transfer.Upload {
							method = http.MethodPost
						}
					}
					var rawBody io.ReadCloser
					length := int64(0)
					if e.BodyFile != "" {
						file, err := os.Open(e.BodyFile)
						if err != nil {
							return 0, false, fmt.Errorf("open body file: %w", err)
						}
						info, err := file.Stat()
						if err != nil || info.Size() != e.length {
							file.Close()
							return 0, false, errors.New("body file size changed or cannot be read")
						}
						rawBody, length = file, e.length
					} else if e.BodySet {
						rawBody, length = io.NopCloser(strings.NewReader(e.Body)), e.length
					} else if job.Direction == transfer.Upload {
						remaining, position := reserved, 0
						rawBody, length = io.NopCloser(utils.ReaderFunc(func(p []byte) (int, error) {
							if remaining == 0 {
								return 0, io.EOF
							}
							n := int(min(int64(len(p)), remaining))
							for offset := 0; offset < n; {
								copied := copy(p[offset:n], buffer[position:])
								offset += copied
								position = (position + copied) % len(buffer)
							}
							remaining -= int64(n)
							return n, nil
						})), reserved
					}
					var sent atomic.Int64
					var body io.ReadCloser
					if rawBody != nil {
						bodyCtx, bodyCancel := context.WithCancel(ctx)
						var once sync.Once
						var mu sync.Mutex
						var closeErr error
						body = struct {
							io.Reader
							io.Closer
						}{
							Reader: utils.ReaderFunc(func(p []byte) (int, error) {
								mu.Lock()
								defer mu.Unlock()
								if err := bodyCtx.Err(); err != nil {
									return 0, err
								}
								n, err := rawBody.Read(p[:utils.Rate{}.LimitSize(limiter, len(p))])
								if n > 0 {
									if waitErr := (utils.Rate{}).WaitBytes(bodyCtx, limiter, n); waitErr != nil {
										return 0, waitErr
									}
									sent.Add(int64(n))
									meter.Add(transfer.Upload, int64(n))
								}
								return n, err
							}),
							Closer: utils.CloserFunc(func() error {
								bodyCancel()
								once.Do(func() {
									mu.Lock()
									defer mu.Unlock()
									closeErr = rawBody.Close()
								})
								return closeErr
							}),
						}
						defer body.Close()
					}
					req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
					if err != nil {
						return 0, false, err
					}
					req.ContentLength = length
					req.Header = e.Headers.Clone()
					if req.Header == nil {
						req.Header = make(http.Header)
					}
					if _, exists := req.Header["Accept-Encoding"]; !exists {
						req.Header.Set("Accept-Encoding", "identity")
					}
					if _, exists := req.Header["User-Agent"]; !exists {
						req.Header.Set("User-Agent", "entropia/0.1")
					}
					if host := req.Header.Get("Host"); host != "" {
						req.Host = host
						req.Header.Del("Host")
					}
					meter.Attempt()
					resp, err := client.Do(req)
					if err != nil {
						if resp != nil && resp.Body != nil {
							resp.Body.Close()
						}
						if body != nil {
							body.Close()
						}
						actual := int64(0)
						if job.Direction == transfer.Upload {
							actual = sent.Load()
						}
						return actual, true, fmt.Errorf("HTTP request failed: %w", err)
					}
					defer resp.Body.Close()
					if body != nil {
						body.Close()
					}
					success := resp.StatusCode >= 200 && resp.StatusCode < 300
					limit := reserved
					if job.Direction == transfer.Upload {
						limit = 64 * 1024
					}
					if !success {
						limit = min(limit, 4096)
					}
					var received int64
					var readErr error
					for received < limit {
						n, err := resp.Body.Read(buffer[:min(int64(utils.Rate{}.LimitSize(limiter, len(buffer))), limit-received)])
						if n > 0 {
							received += int64(n)
							meter.Add(transfer.Download, int64(n))
							if waitErr := (utils.Rate{}).WaitBytes(ctx, limiter, n); waitErr != nil {
								readErr = waitErr
								break
							}
						}
						if err != nil {
							if !errors.Is(err, io.EOF) {
								readErr = err
							}
							break
						}
						if n == 0 {
							readErr = io.ErrNoProgress
							break
						}
					}
					actual := received
					if job.Direction == transfer.Upload {
						actual = sent.Load()
					}
					if !success {
						retry := resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500
						return actual, retry, fmt.Errorf("HTTP endpoint returned %s", resp.Status)
					}
					if readErr != nil {
						return actual, true, fmt.Errorf("read HTTP response: %w", readErr)
					}
					if job.Direction == transfer.Upload {
						meter.ConfirmUpload(actual)
					}
					return actual, false, nil
				}()
				if !job.Loop {
					budgetMu.Lock()
					remaining += reserved - actual
					budgetMu.Unlock()
				}
				if err == nil && actual > 0 {
					failures = 0
					continue
				}
				if err == nil {
					err, retry = fmt.Errorf("endpoint returned no %s payload", job.Direction), true
				}
				if ctx.Err() != nil {
					return
				}
				meter.Failure()
				failures++
				exhausted := false
				if !job.Loop {
					budgetMu.Lock()
					exhausted = remaining <= 0
					budgetMu.Unlock()
				}
				if !retry || failures >= 6 || exhausted {
					once.Do(func() { result = err; cancel() })
					return
				}
				timer := time.NewTimer(time.Duration(1<<min(failures-1, 4)) * time.Second)
				select {
				case <-ctx.Done():
					timer.Stop()
					return
				case <-timer.C:
				}
			}
		})
	}
	wg.Wait()
	if err := parentCtx.Err(); err != nil {
		return err
	}
	if result != nil {
		return result
	}
	return ctx.Err()
}
