package utils

import (
	"fmt"
	"net/url"
)

func ParseHTTPURL(raw string) (*url.URL, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.Fragment != "" {
		return nil, fmt.Errorf("invalid HTTP URL %q", raw)
	}

	return u, nil
}
