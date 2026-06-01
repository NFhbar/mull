package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

type Log struct {
	BlockNumber uint64
	TxHash      string
	LogIndex    uint
	Address     string
	Topics      []string
	Data        string
}

type Header struct {
	Number     uint64
	Hash       string
	ParentHash string
}

type Client interface {
	BlockNumber(ctx context.Context) (uint64, error)
	GetLogs(ctx context.Context, from, to uint64, address string, topics []string) ([]Log, error)
	BlockByNumber(ctx context.Context, tag string) (Header, error)
	BlockByHash(ctx context.Context, hash string) (Header, error)
}

type HTTPClient struct {
	url   string
	http  *http.Client
	retry RetryPolicy
}

func NewHTTPClient(url string, hc *http.Client, policy RetryPolicy) *HTTPClient {
	if hc == nil {
		hc = http.DefaultClient
	}
	applyRetryDefaults(&policy)
	return &HTTPClient{url: url, http: hc, retry: policy}
}

type rpcRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Method  string `json:"method"`
	Params  []any  `json:"params"`
}

type rpcResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *rpcError       `json:"error,omitempty"`
}

type rpcError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *rpcError) Error() string {
	return fmt.Sprintf("rpc error %d: %s", e.Code, e.Message)
}

func (c *HTTPClient) call(ctx context.Context, method string, params []any, out any) error {
	return retry(ctx, c.retry, func(ctx context.Context) error {
		return c.callOnce(ctx, method, params, out)
	})
}

func (c *HTTPClient) callOnce(ctx context.Context, method string, params []any, out any) error {
	body, err := json.Marshal(rpcRequest{JSONRPC: "2.0", ID: 1, Method: method, Params: params})
	if err != nil {
		return fmt.Errorf("marshal request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := c.http.Do(req)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &transientError{err: fmt.Errorf("do request: %w", err)}
	}
	defer resp.Body.Close()

	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		return &transientError{err: fmt.Errorf("read response: %w", err)}
	}
	if resp.StatusCode != http.StatusOK {
		httpErr := fmt.Errorf("http %d: %s", resp.StatusCode, raw)
		if resp.StatusCode == http.StatusTooManyRequests {
			return &transientError{
				err:           httpErr,
				overrideDelay: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
			}
		}
		if resp.StatusCode >= 500 {
			return &transientError{err: httpErr}
		}
		return httpErr
	}

	var r rpcResponse
	if err := json.Unmarshal(raw, &r); err != nil {
		return fmt.Errorf("decode response: %w", err)
	}
	// JSON-RPC errors are terminal: providers that surface transient
	// conditions (e.g. Alchemy -32005 "limit exceeded", Infura -32603) as an
	// rpc.error payload rather than HTTP 429/5xx will not be retried by this
	// layer. Allow-listing transient JSON-RPC codes is a possible follow-up.
	if r.Error != nil {
		return r.Error
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(r.Result, out); err != nil {
		return fmt.Errorf("decode result: %w", err)
	}
	return nil
}

func (c *HTTPClient) BlockNumber(ctx context.Context) (uint64, error) {
	var hex string
	if err := c.call(ctx, "eth_blockNumber", []any{}, &hex); err != nil {
		return 0, err
	}
	return ParseHexUint64(hex)
}

type rawBlockHeader struct {
	Number     string `json:"number"`
	Hash       string `json:"hash"`
	ParentHash string `json:"parentHash"`
}

func decodeHeader(raw *rawBlockHeader) (Header, error) {
	n, err := ParseHexUint64(raw.Number)
	if err != nil {
		return Header{}, fmt.Errorf("parse block number %q: %w", raw.Number, err)
	}
	return Header{Number: n, Hash: raw.Hash, ParentHash: raw.ParentHash}, nil
}

func (c *HTTPClient) BlockByNumber(ctx context.Context, tag string) (Header, error) {
	var raw *rawBlockHeader
	if err := c.call(ctx, "eth_getBlockByNumber", []any{tag, false}, &raw); err != nil {
		return Header{}, err
	}
	if raw == nil {
		return Header{}, fmt.Errorf("block not found: %s", tag)
	}
	return decodeHeader(raw)
}

func (c *HTTPClient) BlockByHash(ctx context.Context, hash string) (Header, error) {
	var raw *rawBlockHeader
	if err := c.call(ctx, "eth_getBlockByHash", []any{hash, false}, &raw); err != nil {
		return Header{}, err
	}
	if raw == nil {
		return Header{}, fmt.Errorf("block not found: %s", hash)
	}
	return decodeHeader(raw)
}

type logFilter struct {
	FromBlock string   `json:"fromBlock"`
	ToBlock   string   `json:"toBlock"`
	Address   string   `json:"address,omitempty"`
	Topics    []string `json:"topics,omitempty"`
}

type rawLog struct {
	BlockNumber string   `json:"blockNumber"`
	TxHash      string   `json:"transactionHash"`
	LogIndex    string   `json:"logIndex"`
	Address     string   `json:"address"`
	Topics      []string `json:"topics"`
	Data        string   `json:"data"`
}

func (c *HTTPClient) GetLogs(ctx context.Context, from, to uint64, address string, topics []string) ([]Log, error) {
	filter := logFilter{
		FromBlock: HexUint64(from),
		ToBlock:   HexUint64(to),
		Address:   address,
		Topics:    topics,
	}
	var raws []rawLog
	if err := c.call(ctx, "eth_getLogs", []any{filter}, &raws); err != nil {
		return nil, err
	}
	logs := make([]Log, 0, len(raws))
	for _, r := range raws {
		bn, err := ParseHexUint64(r.BlockNumber)
		if err != nil {
			return nil, fmt.Errorf("parse block number %q: %w", r.BlockNumber, err)
		}
		li, err := ParseHexUint64(r.LogIndex)
		if err != nil {
			return nil, fmt.Errorf("parse log index %q: %w", r.LogIndex, err)
		}
		logs = append(logs, Log{
			BlockNumber: bn,
			TxHash:      r.TxHash,
			LogIndex:    uint(li),
			Address:     r.Address,
			Topics:      r.Topics,
			Data:        r.Data,
		})
	}
	return logs, nil
}

func HexUint64(n uint64) string {
	return "0x" + strconv.FormatUint(n, 16)
}

func ParseHexUint64(s string) (uint64, error) {
	return strconv.ParseUint(strings.TrimPrefix(s, "0x"), 16, 64)
}

// parseRetryAfter parses an HTTP Retry-After header. RFC 7231 permits either
// delta-seconds (an integer) or an HTTP-date. Returns 0 when the header is
// absent, in the past, or unparseable — signalling the caller to fall back to
// its normal backoff strategy.
func parseRetryAfter(v string, now time.Time) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		if secs <= 0 {
			return 0
		}
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := t.Sub(now); d > 0 {
			return d
		}
	}
	return 0
}
