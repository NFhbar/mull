package rpc

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

type rpcEnvelope struct {
	Method string          `json:"method"`
	Params json.RawMessage `json:"params"`
}

func newTestServer(t *testing.T, handle func(method string, params json.RawMessage) (any, *rpcError)) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var env rpcEnvelope
		if err := json.Unmarshal(body, &env); err != nil {
			t.Errorf("bad request body: %v", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		result, rpcErr := handle(env.Method, env.Params)
		resp := map[string]any{"jsonrpc": "2.0", "id": 1}
		if rpcErr != nil {
			resp["error"] = rpcErr
		} else {
			resp["result"] = result
		}
		_ = json.NewEncoder(w).Encode(resp)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func TestBlockNumber(t *testing.T) {
	srv := newTestServer(t, func(method string, _ json.RawMessage) (any, *rpcError) {
		if method != "eth_blockNumber" {
			t.Errorf("method = %q, want eth_blockNumber", method)
		}
		return "0x10", nil
	})
	c := NewHTTPClient(srv.URL, nil, RetryPolicy{})
	got, err := c.BlockNumber(context.Background())
	if err != nil {
		t.Fatalf("BlockNumber: %v", err)
	}
	if got != 16 {
		t.Fatalf("BlockNumber = %d, want 16", got)
	}
}

func TestGetLogsDecodesHexFields(t *testing.T) {
	srv := newTestServer(t, func(method string, params json.RawMessage) (any, *rpcError) {
		if method != "eth_getLogs" {
			t.Errorf("method = %q, want eth_getLogs", method)
		}
		// Sanity check params contain hex-encoded block bounds.
		if !strings.Contains(string(params), `"fromBlock":"0x1"`) {
			t.Errorf("params missing fromBlock: %s", params)
		}
		return []rawLog{{
			BlockNumber: "0x2a",
			TxHash:      "0xdeadbeef",
			LogIndex:    "0x3",
			Address:     "0xabc",
			Topics:      []string{"0xtopic"},
			Data:        "0xdata",
		}}, nil
	})
	c := NewHTTPClient(srv.URL, nil, RetryPolicy{})
	logs, err := c.GetLogs(context.Background(), 1, 10, "0xabc", nil)
	if err != nil {
		t.Fatalf("GetLogs: %v", err)
	}
	if len(logs) != 1 {
		t.Fatalf("len(logs) = %d, want 1", len(logs))
	}
	got := logs[0]
	if got.BlockNumber != 42 || got.LogIndex != 3 {
		t.Fatalf("decoded log = %+v, want block 42 / index 3", got)
	}
}

func TestBlockByNumberDecodesHeader(t *testing.T) {
	srv := newTestServer(t, func(method string, params json.RawMessage) (any, *rpcError) {
		if method != "eth_getBlockByNumber" {
			t.Errorf("method = %q, want eth_getBlockByNumber", method)
		}
		if !strings.Contains(string(params), `"latest"`) {
			t.Errorf("params missing tag: %s", params)
		}
		if !strings.Contains(string(params), `false`) {
			t.Errorf("params missing false (header-only): %s", params)
		}
		return map[string]any{
			"number":     "0x2a",
			"hash":       "0xabc",
			"parentHash": "0xdef",
		}, nil
	})
	c := NewHTTPClient(srv.URL, nil, RetryPolicy{})
	h, err := c.BlockByNumber(context.Background(), "latest")
	if err != nil {
		t.Fatalf("BlockByNumber: %v", err)
	}
	if h.Number != 42 || h.Hash != "0xabc" || h.ParentHash != "0xdef" {
		t.Fatalf("header = %+v, want {42, 0xabc, 0xdef}", h)
	}
}

func TestBlockByHashDecodesHeader(t *testing.T) {
	srv := newTestServer(t, func(method string, params json.RawMessage) (any, *rpcError) {
		if method != "eth_getBlockByHash" {
			t.Errorf("method = %q, want eth_getBlockByHash", method)
		}
		if !strings.Contains(string(params), `"0xabc"`) {
			t.Errorf("params missing hash: %s", params)
		}
		return map[string]any{
			"number":     "0x10",
			"hash":       "0xabc",
			"parentHash": "0xparent",
		}, nil
	})
	c := NewHTTPClient(srv.URL, nil, RetryPolicy{})
	h, err := c.BlockByHash(context.Background(), "0xabc")
	if err != nil {
		t.Fatalf("BlockByHash: %v", err)
	}
	if h.Number != 16 || h.Hash != "0xabc" || h.ParentHash != "0xparent" {
		t.Fatalf("header = %+v, want {16, 0xabc, 0xparent}", h)
	}
}

func TestBlockByHashNullResult(t *testing.T) {
	srv := newTestServer(t, func(method string, params json.RawMessage) (any, *rpcError) {
		return nil, nil
	})
	c := NewHTTPClient(srv.URL, nil, RetryPolicy{})
	_, err := c.BlockByHash(context.Background(), "0xdeadbeef")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "block not found") {
		t.Fatalf("err = %v, want 'block not found'", err)
	}
	if !strings.Contains(err.Error(), "0xdeadbeef") {
		t.Fatalf("err = %v, want hash in message", err)
	}
}

func TestRPCError(t *testing.T) {
	srv := newTestServer(t, func(string, json.RawMessage) (any, *rpcError) {
		return nil, &rpcError{Code: -32000, Message: "boom"}
	})
	c := NewHTTPClient(srv.URL, nil, RetryPolicy{})
	_, err := c.BlockNumber(context.Background())
	if err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("err = %v, want rpc error containing 'boom'", err)
	}
}
