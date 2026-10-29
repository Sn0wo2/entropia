package http

import (
	"cmp"
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
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
	e.DownloadURL = cmp.Or(e.DownloadURL, e.URL)
	e.UploadURL = cmp.Or(e.UploadURL, e.URL)

	if e.DownloadURL == "" && e.UploadURL == "" {
		return errors.New("specify --url, --download-url, or --upload-url")
	}

	e.Method = strings.ToUpper(strings.TrimSpace(e.Method))
	for _, raw := range []string{e.DownloadURL, e.UploadURL} {
		if raw == "" {
			continue
		}

		if _, err := utils.ParseHTTPURL(raw); err != nil {
			return err
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

	endpoint, method, err := e.endpointAndMethod(job)
	if err != nil {
		return err
	}

	client, closeIdle, err := e.newClient(job)
	if err != nil {
		return err
	}
	defer closeIdle()

	parentCtx := ctx

	ctx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	limiter := rate.NewLimiter(rate.Inf, 32*1024)

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

	state := &transferState{
		ctx: ctx, cancel: cancel, engine: e, job: job, meter: meter,
		endpoint: endpoint, method: method, client: client, limiter: limiter,
		remaining: job.Bytes, whole: whole, maximum: maximum,
	}

	var wg sync.WaitGroup
	for range job.Concurrency {
		wg.Go(state.run)
	}

	wg.Wait()

	if err := parentCtx.Err(); err != nil {
		return err
	}

	return context.Cause(ctx)
}

func (e *Engine) endpointAndMethod(job transfer.Job) (string, string, error) {
	endpoint, method := e.DownloadURL, cmp.Or(e.Method, http.MethodGet)
	if job.Direction == transfer.Upload {
		endpoint, method = e.UploadURL, cmp.Or(e.Method, http.MethodPost)
		if (e.BodySet || e.BodyFile != "") && e.length == 0 {
			return "", "", errors.New("an empty custom body cannot consume upload traffic")
		}
	}

	if endpoint == "" {
		return "", "", fmt.Errorf("missing URL for %s", job.Direction)
	}

	return endpoint, method, nil
}

func (e *Engine) newClient(job transfer.Job) (*http.Client, func(), error) {
	defaultTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return nil, nil, errors.New("default HTTP transport is not an *http.Transport")
	}

	transport := defaultTransport.Clone()
	transport.DialContext = (&net.Dialer{Timeout: job.Timeout, KeepAlive: 30 * time.Second}).DialContext
	transport.MaxConnsPerHost, transport.MaxIdleConnsPerHost = job.Concurrency, job.Concurrency
	transport.MaxIdleConns = job.Concurrency
	transport.MaxResponseHeaderBytes = 64 * 1024
	transport.DisableCompression = true
	transport.ForceAttemptHTTP2 = true
	transport.ResponseHeaderTimeout = job.Timeout
	transport.TLSHandshakeTimeout = job.Timeout

	client := &http.Client{
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return checkRedirect(job, req, via)
		},
	}

	return client, transport.CloseIdleConnections, nil
}

func checkRedirect(job transfer.Job, req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return errors.New("too many redirects")
	}

	if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
		return errors.New("unsupported redirect scheme")
	}

	if job.Direction == transfer.Upload && req.Method != via[0].Method {
		return errors.New("upload redirect would discard the body; use the final upload URL")
	}

	if req.URL.Scheme != via[0].URL.Scheme || !strings.EqualFold(req.URL.Host, via[0].URL.Host) {
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
}

type transferState struct {
	ctx       context.Context
	cancel    context.CancelCauseFunc
	engine    *Engine
	job       transfer.Job
	meter     *transfer.Meter
	endpoint  string
	method    string
	client    *http.Client
	limiter   *rate.Limiter
	remaining int64
	budgetMu  sync.Mutex
	whole     bool
	maximum   int64
}

func (s *transferState) reserve() (int64, bool) {
	reserved := s.maximum
	if s.ctx.Err() != nil {
		reserved = 0
	} else if !s.job.Loop {
		s.budgetMu.Lock()
		if s.remaining <= 0 {
			reserved = 0
		} else {
			if !s.whole {
				reserved = min(reserved, s.remaining)
			}

			s.remaining -= reserved
		}
		s.budgetMu.Unlock()
	}

	return reserved, reserved > 0
}

func (s *transferState) restore(reserved, actual int64) {
	if s.job.Loop {
		return
	}

	s.budgetMu.Lock()
	s.remaining += reserved - actual
	s.budgetMu.Unlock()
}

func (s *transferState) exhausted() bool {
	if s.job.Loop {
		return false
	}

	s.budgetMu.Lock()
	defer s.budgetMu.Unlock()

	return s.remaining <= 0
}

func (s *transferState) run() {
	buffer := make([]byte, 32*1024)
	if !s.engine.BodySet && s.engine.BodyFile == "" && s.job.Direction == transfer.Upload {
		rand.Read(buffer)
	}

	failures := 0

	for reserved, ok := s.reserve(); ok; reserved, ok = s.reserve() {
		actual, retry, err := s.request(buffer, reserved)
		s.restore(reserved, actual)

		if err == nil && actual > 0 {
			failures = 0

			continue
		}

		if err == nil {
			err, retry = fmt.Errorf("endpoint returned no %s payload", s.job.Direction), true
		}

		if s.ctx.Err() != nil {
			return
		}

		s.meter.Failures.Add(1)

		failures++
		exhausted := s.exhausted()

		if !retry || failures >= 6 || exhausted {
			s.cancel(err)

			return
		}

		select {
		case <-s.ctx.Done():
			return
		case <-time.After(time.Duration(1<<min(failures-1, 4)) * time.Second):
		}
	}
}

func (s *transferState) request(buffer []byte, reserved int64) (int64, bool, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s.job.Timeout)
	defer cancel()

	body, length, sent, err := s.newBody(ctx, buffer, reserved)
	if err != nil {
		return 0, false, err
	}

	if body != nil {
		defer func() { _ = body.Close() }()
	}

	req, err := http.NewRequestWithContext(ctx, s.method, s.endpoint, body)
	if err != nil {
		return 0, false, err
	}

	req.ContentLength = length

	req.Header = s.engine.Headers.Clone()
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

	s.meter.Attempts.Add(1)

	resp, err := s.client.Do(req)

	if body != nil {
		_ = body.Close()
	}

	if err != nil {
		actual := int64(0)
		if s.job.Direction == transfer.Upload {
			actual = sent.Load()
		}

		return actual, true, fmt.Errorf("HTTP request failed: %w", err)
	}

	defer func() { _ = resp.Body.Close() }()

	return s.readResponse(ctx, resp, buffer, reserved, sent)
}

func (s *transferState) newBody(ctx context.Context, buffer []byte, reserved int64) (io.ReadCloser, int64, *atomic.Int64, error) {
	var rawBody io.ReadCloser

	length := int64(0)

	switch {
	case s.engine.BodyFile != "":
		file, err := os.Open(s.engine.BodyFile)
		if err != nil {
			return nil, 0, nil, fmt.Errorf("open body file: %w", err)
		}

		info, err := file.Stat()
		if err != nil || info.Size() != s.engine.length {
			_ = file.Close()

			return nil, 0, nil, errors.New("body file size changed or cannot be read")
		}

		rawBody, length = file, s.engine.length
	case s.engine.BodySet:
		rawBody, length = io.NopCloser(strings.NewReader(s.engine.Body)), s.engine.length
	case s.job.Direction == transfer.Upload:
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

	sent := new(atomic.Int64)

	var body io.ReadCloser

	if rawBody != nil {
		bodyCtx, bodyCancel := context.WithCancel(ctx)

		var mu sync.Mutex

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

				n, err := rawBody.Read(p[:min(len(p), s.limiter.Burst())])
				if n > 0 {
					if waitErr := utils.WaitBytes(bodyCtx, s.limiter, n); waitErr != nil {
						return 0, waitErr
					}

					sent.Add(int64(n))
					s.meter.Uploaded.Add(int64(n))
				}

				return n, err
			}),
			Closer: utils.CloserFunc(sync.OnceValue(func() error {
				bodyCancel()
				mu.Lock()
				defer mu.Unlock()

				return rawBody.Close()
			})),
		}
	}

	return body, length, sent, nil
}

func (s *transferState) readResponse(ctx context.Context, resp *http.Response, buffer []byte, reserved int64, sent *atomic.Int64) (int64, bool, error) {
	success := resp.StatusCode >= 200 && resp.StatusCode < 300

	limit := reserved
	if s.job.Direction == transfer.Upload {
		limit = 64 * 1024
	}

	if !success {
		limit = min(limit, 4096)
	}

	var received int64

	var readErr error

	for received < limit {
		n, err := resp.Body.Read(buffer[:min(int64(len(buffer)), int64(s.limiter.Burst()), limit-received)])
		if n > 0 {
			received += int64(n)
			s.meter.Downloaded.Add(int64(n))

			if waitErr := utils.WaitBytes(ctx, s.limiter, n); waitErr != nil {
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
	if s.job.Direction == transfer.Upload {
		actual = sent.Load()
	}

	if !success {
		retry := resp.StatusCode == http.StatusRequestTimeout || resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500

		return actual, retry, fmt.Errorf("HTTP endpoint returned %s", resp.Status)
	}

	if readErr != nil {
		return actual, true, fmt.Errorf("read HTTP response: %w", readErr)
	}

	if s.job.Direction == transfer.Upload {
		s.meter.ConfirmedUpload.Add(actual)
	}

	return actual, false, nil
}
