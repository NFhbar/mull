// Package serve exposes the indexed events table over a small read-only
// HTTP/JSON API. It is the consumer-facing counterpart to `mull index`:
// frontends, bots, and analytics pipelines that can't speak SQLite
// directly can hit `/events`, `/checkpoint`, and `/healthz` instead.
package serve

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"

	"github.com/NFhbar/mull/internal/config"
	"github.com/NFhbar/mull/internal/store"
)

// Server is the HTTP layer over a store.Store. Construct with NewServer
// and mount Handler() under any net/http listener.
type Server struct {
	store  store.Store
	logger *slog.Logger
}

// NewServer wires a Server with its store and logger. The logger is
// load-bearing: every handler logs decode failures at Warn and store
// errors at Error, so callers should pass a configured *slog.Logger.
// A nil logger is replaced with slog.Default to keep the constructor
// from being a footgun in tests that don't care about output.
func NewServer(st store.Store, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.Default()
	}
	return &Server{store: st, logger: logger}
}

// Handler returns the mux wiring the three routes (/healthz, /checkpoint,
// /events). All routes are GET-only; other methods get 405.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/checkpoint", s.handleCheckpoint)
	mux.HandleFunc("/events", s.handleEvents)
	return mux
}

func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
}

// checkpointResponse is the uniform shape returned by /checkpoint regardless
// of `?source=` presence. v1 returned {"checkpoint": <n>}; multi-source makes
// that ambiguous, so the response is ALWAYS {"checkpoints": {<src>: <n>, …}}.
// Clients must read body.checkpoints[<source>]. This is a documented breaking
// change — see MIGRATION.md.
type checkpointResponse struct {
	Checkpoints map[string]uint64 `json:"checkpoints"`
}

func (s *Server) handleCheckpoint(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	if q.Has("source") {
		src := q.Get("source")
		if err := config.ValidateSourceName(src); err != nil {
			s.logger.Warn("checkpoint query rejected", "param", "source", "value", src, "err", err.Error())
			http.Error(w, "bad query param \"source\": "+err.Error(), http.StatusBadRequest)
			return
		}
		// Read from the multi-row map rather than store.Checkpoint so an unknown
		// source returns `{"checkpoints":{}}` instead of `{"checkpoints":{src:0}}`
		// — same shape as the no-?source= branch for unindexed sources, which
		// lets clients diff "indexed at 0" from "never indexed."
		cps, err := s.store.Checkpoints(r.Context())
		if err != nil {
			s.logger.Error("checkpoints read failed", "path", r.URL.Path, "err", err.Error())
			http.Error(w, "store error", http.StatusInternalServerError)
			return
		}
		out := map[string]uint64{}
		if cp, ok := cps[src]; ok {
			out[src] = cp
		}
		writeJSON(w, http.StatusOK, checkpointResponse{Checkpoints: out})
		return
	}
	cps, err := s.store.Checkpoints(r.Context())
	if err != nil {
		s.logger.Error("checkpoints read failed", "path", r.URL.Path, "err", err.Error())
		http.Error(w, "store error", http.StatusInternalServerError)
		return
	}
	if cps == nil {
		cps = map[string]uint64{}
	}
	writeJSON(w, http.StatusOK, checkpointResponse{Checkpoints: cps})
}

type eventsResponse struct {
	Events     []eventJSON `json:"events"`
	NextCursor string      `json:"next_cursor"`
}

type eventJSON struct {
	Source      string   `json:"source"`
	BlockNumber uint64   `json:"block_number"`
	TxHash      string   `json:"tx_hash"`
	LogIndex    uint     `json:"log_index"`
	Address     string   `json:"address"`
	Topics      []string `json:"topics"`
	Data        string   `json:"data"`
}

func (s *Server) handleEvents(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	filter, err := parseQuery(r.URL.Query())
	if err != nil {
		s.logger.Warn("events query decode failed", "param", err.param, "value", err.value, "err", err.cause.Error())
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	events, next, qerr := s.store.Query(r.Context(), filter)
	if qerr != nil {
		// Treat ctx cancellation as a quiet client-disconnect rather than a
		// 500 — surfaces in access logs via the request log, not the error log.
		if errors.Is(qerr, context.Canceled) || errors.Is(qerr, context.DeadlineExceeded) {
			return
		}
		s.logger.Error("events query failed", "path", r.URL.Path, "query", r.URL.RawQuery, "err", qerr.Error())
		http.Error(w, "store error", http.StatusInternalServerError)
		return
	}

	out := eventsResponse{Events: make([]eventJSON, 0, len(events))}
	for _, e := range events {
		out.Events = append(out.Events, eventJSON{
			Source:      e.Source,
			BlockNumber: e.BlockNumber,
			TxHash:      e.TxHash,
			LogIndex:    e.LogIndex,
			Address:     e.Address,
			Topics:      e.Topics,
			Data:        e.Data,
		})
	}
	if next != nil {
		out.NextCursor = encodeCursor(*next)
	}
	writeJSON(w, http.StatusOK, out)
}

// decodeError carries enough structure for the handler to log the failing
// param + value cleanly; the user sees only Error().
type decodeError struct {
	param string
	value string
	cause error
}

func (e *decodeError) Error() string {
	return fmt.Sprintf("bad query param %q: %s", e.param, e.cause.Error())
}

func parseQuery(q url.Values) (store.QueryFilter, *decodeError) {
	var f store.QueryFilter
	f.Contract = q.Get("contract")
	if q.Has("source") {
		src := q.Get("source")
		if err := config.ValidateSourceName(src); err != nil {
			return f, &decodeError{param: "source", value: src, cause: err}
		}
		f.Source = &src
	}

	for i, name := range []string{"topic0", "topic1", "topic2", "topic3"} {
		v := parseTopic(q, name)
		switch i {
		case 0:
			f.Topic0 = v
		case 1:
			f.Topic1 = v
		case 2:
			f.Topic2 = v
		case 3:
			f.Topic3 = v
		}
	}

	if q.Has("from") {
		v, err := strconv.ParseUint(q.Get("from"), 10, 64)
		if err != nil {
			return f, &decodeError{param: "from", value: q.Get("from"), cause: err}
		}
		f.FromBlock = &v
	}
	if q.Has("to") {
		v, err := strconv.ParseUint(q.Get("to"), 10, 64)
		if err != nil {
			return f, &decodeError{param: "to", value: q.Get("to"), cause: err}
		}
		f.ToBlock = &v
	}
	if q.Has("limit") {
		raw := q.Get("limit")
		v, err := strconv.Atoi(raw)
		if err != nil {
			return f, &decodeError{param: "limit", value: raw, cause: err}
		}
		f.Limit = v
	}
	if q.Has("cursor") {
		raw := q.Get("cursor")
		cur, err := decodeCursor(raw)
		if err != nil {
			return f, &decodeError{param: "cursor", value: raw, cause: err}
		}
		f.After = &cur
	}
	return f, nil
}

// parseTopic distinguishes three states for a topic-N query param:
//
//   - absent           → nil    (no filter)
//   - present, empty   → &""    (filter on the literal empty topic)
//   - present, valued  → &value
//
// The repo's existing patterns use plain Get; we use Has here to preserve
// the absent-vs-empty distinction so README's "decode rules" stay honest.
func parseTopic(q url.Values, k string) *string {
	if !q.Has(k) {
		return nil
	}
	v := q.Get(k)
	return &v
}

// cursorPayload is the on-the-wire shape of an /events cursor. The S
// (source) field is new in v2; it disambiguates events that share
// (block, log_index) across sources. A legacy cursor encoded against v1
// decodes with S == "" — the empty string sorts strictly before any real
// source name (ASCII), so paging resumes from a deterministic boundary
// (one event may re-emit at the transition; see MIGRATION.md).
type cursorPayload struct {
	B uint64 `json:"b"`
	L uint   `json:"l"`
	S string `json:"s,omitempty"`
}

func encodeCursor(c store.EventCursor) string {
	b, _ := json.Marshal(cursorPayload{B: c.Block, L: c.LogIndex, S: c.Source})
	return base64.URLEncoding.EncodeToString(b)
}

func decodeCursor(s string) (store.EventCursor, error) {
	raw, err := base64.URLEncoding.DecodeString(s)
	if err != nil {
		return store.EventCursor{}, fmt.Errorf("invalid cursor encoding: %w", err)
	}
	var p cursorPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return store.EventCursor{}, fmt.Errorf("invalid cursor payload: %w", err)
	}
	return store.EventCursor{Block: p.B, LogIndex: p.L, Source: p.S}, nil
}

func writeJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}
