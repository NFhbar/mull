package store

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
)

// RebuildSpec carries the generated artifacts the rebuild engine needs,
// parameterized so the engine is testable without running codegen:
//   - DDL is the full generated SchemaDDL (CREATE TABLE IF NOT EXISTS per
//     table). The rebuild engine extracts and executes only the statement
//     for the table being rebuilt — executing the full DDL would durably
//     create absent sibling tables as a side effect (see rebuildTable).
//   - Signatures maps each generated table to its expected column
//     signature (generated SchemaVersions).
//   - Topics maps each generated table to the topic0 of the event feeding
//     it (generated SchemaTopics).
//   - NewSinks constructs the generated sinks over an Execer so replay
//     writes land inside the rebuild transaction.
type RebuildSpec struct {
	DDL        string
	Signatures map[string]string
	Topics     map[string]string
	NewSinks   func(Execer) []EventSink
}

// rebuildTableName guards the one place a table identifier is interpolated
// into SQL text (DROP TABLE — identifiers can't be parameterized).
// Defense-in-depth: names come from codegen-sanitized SchemaVersions keys.
var rebuildTableName = regexp.MustCompile(`^[A-Za-z0-9_]+$`)

// RebuildDriftedTables rebuilds every generated typed table whose on-disk
// column shape mismatches its expected signature — including tables that
// are absent entirely (hand-dropped under the old manual remedy, or never
// created), for which the PRAGMA-derived signature is empty.
//
// Per drifted table, inside one transaction: drop, recreate from spec.DDL,
// restamp gen_schema_versions, then replay matching raw events through the
// generated sink in (block_number, log_index, source) order. The raw events
// table is the source of truth; typed tables are pure derivations, so a
// committed rebuild loses nothing. Tables stamped in gen_schema_versions
// but absent from spec.Signatures (orphans) are never touched.
//
// The drifted set is snapshotted before any rebuild runs, and each rebuild
// executes only its own table's CREATE statement extracted from spec.DDL.
// Both guards target the same hazard: creating an absent sibling empty at
// the fresh shape would make a later signature scan — this loop's, or a
// re-run's after a mid-replay failure (rebuilds commit per table) — see a
// match and silently skip that sibling's replay, losing its history.
//
// Returns the names of the tables rebuilt, in sorted order.
func (s *SQLite) RebuildDriftedTables(ctx context.Context, spec RebuildSpec) ([]string, error) {
	tables := make([]string, 0, len(spec.Signatures))
	for t := range spec.Signatures {
		tables = append(tables, t)
	}
	sort.Strings(tables)

	var drifted []string
	for _, table := range tables {
		actual, err := tableColumnSignature(ctx, s.db, table)
		if err != nil {
			return nil, err
		}
		if actual != spec.Signatures[table] {
			drifted = append(drifted, table)
		}
	}

	var rebuilt []string
	for _, table := range drifted {
		if err := s.rebuildTable(ctx, spec, table); err != nil {
			return rebuilt, fmt.Errorf("rebuild %s: %w", table, err)
		}
		rebuilt = append(rebuilt, table)
	}
	return rebuilt, nil
}

func (s *SQLite) rebuildTable(ctx context.Context, spec RebuildSpec, table string) error {
	if !rebuildTableName.MatchString(table) {
		return fmt.Errorf("unsafe table name %q", table)
	}
	topic0, ok := spec.Topics[table]
	if !ok {
		return fmt.Errorf("no topic0 mapping for table")
	}
	ddl, err := tableDDLStatement(spec.DDL, table)
	if err != nil {
		return err
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var sink EventSink
	for _, candidate := range spec.NewSinks(tx) {
		if candidate.Topic0() == topic0 {
			sink = candidate
			break
		}
	}
	if sink == nil {
		return fmt.Errorf("no generated sink matches topic0 %s", topic0)
	}

	if _, err := tx.ExecContext(ctx, `DROP TABLE IF EXISTS "`+table+`"`); err != nil {
		return fmt.Errorf("drop %s: %w", table, err)
	}
	if _, err := tx.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("recreate %s: %w", table, err)
	}
	if _, err := tx.ExecContext(ctx, createGenSchemaVersionsDDL); err != nil {
		return fmt.Errorf("create gen_schema_versions: %w", err)
	}
	if _, err := tx.ExecContext(ctx, upsertGenSchemaVersionSQL, table, spec.Signatures[table]); err != nil {
		return fmt.Errorf("stamp %s: %w", table, err)
	}
	if err := s.eventsByTopic0(ctx, topic0, func(e Event) error {
		return sink.Handle(ctx, e)
	}); err != nil {
		return fmt.Errorf("replay %s: %w", table, err)
	}
	return tx.Commit()
}

// tableDDLStatement extracts the one CREATE TABLE statement for table from
// the full generated DDL, so a rebuild has no sibling side effects. The
// codegen template emits each statement starting at column 0 and terminated
// by ");" at column 0, and table is pre-validated against rebuildTableName
// (alphanumeric + underscore only), so the interpolation is regex-safe.
func tableDDLStatement(ddl, table string) (string, error) {
	re := regexp.MustCompile(`(?ms)^CREATE TABLE IF NOT EXISTS ` + table + ` \(.*?^\);`)
	stmt := re.FindString(ddl)
	if stmt == "" {
		return "", fmt.Errorf("no CREATE TABLE statement for %s in DDL", table)
	}
	return stmt, nil
}

// eventsByTopic0 streams raw events whose topic0 matches, in the canonical
// (block_number, log_index, source) order, calling fn per event. The topic0
// pushdown semantics are identical to Query's: exact match when topic0 is
// the only topic, escaped LIKE prefix otherwise. Reads run on a pooled
// connection — safe alongside the rebuild transaction's writer because WAL
// is asserted at open, and the events table is never written during a
// rebuild so the snapshot is exact.
func (s *SQLite) eventsByTopic0(ctx context.Context, topic0 string, fn func(Event) error) error {
	rows, err := s.db.QueryContext(ctx,
		`SELECT source, block_number, tx_hash, log_index, address, topics, data FROM events
		 WHERE (topics = ? OR topics LIKE ? ESCAPE '\')
		 ORDER BY block_number ASC, log_index ASC, source ASC`,
		topic0, escapeLikePattern(topic0)+",%")
	if err != nil {
		return fmt.Errorf("query events: %w", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var (
			e      Event
			topics string
		)
		if err := rows.Scan(&e.Source, &e.BlockNumber, &e.TxHash, &e.LogIndex, &e.Address, &topics, &e.Data); err != nil {
			return fmt.Errorf("scan event: %w", err)
		}
		if topics != "" {
			e.Topics = strings.Split(topics, ",")
		}
		if err := fn(e); err != nil {
			return err
		}
	}
	return rows.Err()
}
