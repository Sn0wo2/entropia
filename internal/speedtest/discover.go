package speedtest

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	"github.com/Sn0wo2/entropia/internal/utils"
)

type Node struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Country string `json:"country"`
	Host    string `json:"host"`
	URL     string `json:"url"`
}

func Discover(ctx context.Context) ([]Node, error) {
	client := &http.Client{Timeout: 15 * time.Second, Transport: &http.Transport{
		Proxy:             http.ProxyFromEnvironment,
		DialContext:       (&net.Dialer{Timeout: 5 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		ForceAttemptHTTP2: true, DisableCompression: true,
		MaxConnsPerHost: 1, MaxIdleConns: 1, MaxIdleConnsPerHost: 1,
		MaxResponseHeaderBytes: 64 * 1024, TLSHandshakeTimeout: 5 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
	}}
	defer client.CloseIdleConnections()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://www.speedtest.net/api/js/servers?limit=100", nil)
	if err != nil {
		return nil, fmt.Errorf("speedtest legacy HTTP nodes are unofficial and experimental: discovery request: %w", err)
	}

	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
	req.Header.Set("Accept-Encoding", "identity")
	req.Header.Set("Accept", "application/json,text/plain,*/*")

	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("speedtest legacy HTTP nodes are unofficial and experimental: discovery: %w", err)
	}

	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("speedtest legacy HTTP nodes are unofficial and experimental: discovery status %s", resp.Status)
	}

	decoder := json.NewDecoder(io.LimitReader(resp.Body, 2<<20))
	decoder.UseNumber()

	start, err := decoder.Token()
	if err != nil {
		return nil, fmt.Errorf("speedtest legacy HTTP nodes are unofficial and experimental: discovery parse: %w", err)
	}

	if start != json.Delim('[') {
		return nil, errors.New("speedtest legacy HTTP nodes are unofficial and experimental: discovery response must be a JSON array")
	}

	nodes := make([]Node, 0, 100)

	count := 0
	for ; count < 100 && decoder.More(); count++ {
		var entry map[string]any
		if err := decoder.Decode(&entry); err != nil {
			return nil, fmt.Errorf("speedtest legacy HTTP nodes are unofficial and experimental: discovery parse: %w", err)
		}

		node := Node{
			ID:      cmp.Or(utils.String(entry["id"]), utils.String(entry["idstr"])),
			Name:    cmp.Or(utils.String(entry["sponsor"]), utils.String(entry["name"])),
			Country: cmp.Or(utils.String(entry["country"]), utils.String(entry["cc"])),
			Host:    utils.String(entry["host"]), URL: cmp.Or(utils.String(entry["url"]), utils.String(entry["uri"])),
		}
		if node.ID != "" && node.URL != "" {
			nodes = append(nodes, node)
		}
	}

	if count < 100 {
		end, err := decoder.Token()
		if err != nil {
			return nil, fmt.Errorf("speedtest legacy HTTP nodes are unofficial and experimental: discovery parse: %w", err)
		}

		if end != json.Delim(']') {
			return nil, errors.New("speedtest legacy HTTP nodes are unofficial and experimental: invalid discovery array")
		}
	}

	return nodes, nil
}
