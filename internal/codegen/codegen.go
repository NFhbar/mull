// Package codegen generates typed event structs, SQLite migrations, and sinks
// from a Solidity contract ABI. The output lives under cfg.OutDir (canonically
// internal/gen/) and is committed alongside the indexer source — the indexer
// imports the package directly so a build picks up the latest generated code
// without a separate bootstrap step.
package codegen

import (
	"bytes"
	"fmt"
	"go/format"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"text/template"
	"unicode"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/crypto"
)

type GenerateConfig struct {
	AbiPath string
	OutDir  string
	// ContractAlias namespaces generated SQLite tables so two ABIs sharing an
	// event name don't collide (e.g. two ERC-20s both emitting Transfer). When
	// empty, falls back to the ABI file's basename stripped of extension.
	ContractAlias string
}

type eventPlan struct {
	Name           string
	FileBase       string
	TableName      string
	SigText        string
	SigHash        string
	Signature      string
	IndexedArgs    []fieldPlan
	NonIndexedArgs []fieldPlan
	AllFields      []fieldPlan
	HasBigInt      bool
	HasCommon      bool
	HasBytes       bool
}

type fieldPlan struct {
	Name      string // exported Go name
	ColName   string // snake_case
	GoType    string
	SQLType   string
	SolType   string
	TopicExpr string // for indexed args: expression to decode log.Topics[N]
	TopicN    int    // for indexed args: which topic
	UnpackIdx int    // for non-indexed args: position in abi.Arguments.Unpack result
	IsBigInt  bool
	IsCommon  bool
	IsBytes   bool
	BindExpr  string // expression to convert struct field into a SQL bind value
}

// Generate reads the ABI at cfg.AbiPath, plans emissions per event, and writes
// the standard fileset (doc.go, schema.go, sinks.go, helpers.go, <event>.go)
// under cfg.OutDir. All output is gofmt-clean. Returns the number of events
// emitted.
func Generate(cfg GenerateConfig) (int, error) {
	raw, err := os.ReadFile(cfg.AbiPath)
	if err != nil {
		return 0, fmt.Errorf("read abi: %w", err)
	}
	parsed, err := abi.JSON(bytes.NewReader(raw))
	if err != nil {
		return 0, fmt.Errorf("parse abi: %w", err)
	}
	if cfg.OutDir == "" {
		return 0, fmt.Errorf("out dir is required")
	}
	if err := os.MkdirAll(cfg.OutDir, 0o755); err != nil {
		return 0, fmt.Errorf("mkdir out: %w", err)
	}

	alias := contractAlias(cfg)

	plans := make([]eventPlan, 0, len(parsed.Events))
	seenTables := make(map[string]string, len(parsed.Events))
	for _, ev := range parsed.Events {
		p, err := planEvent(ev, alias)
		if err != nil {
			return 0, fmt.Errorf("event %s: %w", ev.Name, err)
		}
		if prev, ok := seenTables[p.TableName]; ok {
			return 0, fmt.Errorf("event %s: table name %q collides with event %s (try a distinct contract_alias)", ev.Name, p.TableName, prev)
		}
		seenTables[p.TableName] = ev.Name
		plans = append(plans, p)
	}
	sort.Slice(plans, func(i, j int) bool { return plans[i].Name < plans[j].Name })

	files := map[string]string{
		"doc.go":    docTemplate,
		"schema.go": schemaTemplate,
		"sinks.go":  sinksTemplate,
	}
	if len(plans) > 0 {
		files["helpers.go"] = helpersTemplate
	}

	for name, tmplStr := range files {
		out, err := renderTemplate(name, tmplStr, plans)
		if err != nil {
			return 0, fmt.Errorf("render %s: %w", name, err)
		}
		if err := writeFile(filepath.Join(cfg.OutDir, name), out); err != nil {
			return 0, err
		}
	}
	for _, p := range plans {
		out, err := renderTemplate(p.FileBase+".go", eventTemplate, p)
		if err != nil {
			return 0, fmt.Errorf("render event %s: %w", p.Name, err)
		}
		if err := writeFile(filepath.Join(cfg.OutDir, p.FileBase+".go"), out); err != nil {
			return 0, err
		}
	}
	return len(plans), nil
}

func planEvent(ev abi.Event, alias string) (eventPlan, error) {
	p := eventPlan{
		Name:      ev.Name,
		FileBase:  strings.ToLower(ev.Name),
		TableName: "events_" + alias + "_" + snakeCase(ev.Name),
		SigText:   eventSig(ev),
	}
	p.SigHash = crypto.Keccak256Hash([]byte(p.SigText)).Hex()

	nonIdx := 0
	for argIdx, arg := range ev.Inputs {
		mapping, err := mapType(arg.Type)
		if err != nil {
			return eventPlan{}, fmt.Errorf("arg %d (%s): %w", argIdx, displayName(arg.Name, argIdx), err)
		}
		field := fieldPlan{
			Name:    exportedName(arg.Name, argIdx),
			ColName: snakeCase(displayName(arg.Name, argIdx)),
			GoType:  mapping.GoType,
			SQLType: mapping.SQLType,
			SolType: arg.Type.String(),
		}
		field.IsBigInt = mapping.NeedsBigInt
		field.IsCommon = mapping.NeedsCommon
		field.IsBytes = mapping.IsBytes
		field.BindExpr = goValueExpr(mapping, "v."+field.Name)
		if arg.Indexed {
			if mapping.TopicExpr == "" {
				return eventPlan{}, fmt.Errorf("arg %d (%s): indexed dynamic types not supported (sol=%s)", argIdx, displayName(arg.Name, argIdx), arg.Type.String())
			}
			topicN := len(p.IndexedArgs) + 1
			field.TopicExpr = fmt.Sprintf(mapping.TopicExpr, fmt.Sprintf("log.Topics[%d]", topicN))
			field.TopicN = topicN
			p.IndexedArgs = append(p.IndexedArgs, field)
		} else {
			field.UnpackIdx = nonIdx
			nonIdx++
			p.NonIndexedArgs = append(p.NonIndexedArgs, field)
		}
		p.AllFields = append(p.AllFields, field)
		if field.IsBigInt {
			p.HasBigInt = true
		}
		if field.IsCommon {
			p.HasCommon = true
		}
	}
	p.Signature = columnSignature(p.AllFields)
	return p, nil
}

// columnSignature renders the table's full column shape in DDL order as a
// verbatim "colname:SQLTYPE" list — byte-identical to a signature derived
// from PRAGMA table_info, so drift detection survives cosmetic DDL churn
// that a whole-DDL hash would falsely flag.
func columnSignature(fields []fieldPlan) string {
	parts := make([]string, 0, len(fields)+4)
	parts = append(parts, "source:TEXT", "block_number:INTEGER", "tx_hash:TEXT", "log_index:INTEGER")
	for _, f := range fields {
		parts = append(parts, f.ColName+":"+f.SQLType)
	}
	return strings.Join(parts, ",")
}

func eventSig(ev abi.Event) string {
	parts := make([]string, 0, len(ev.Inputs))
	for _, in := range ev.Inputs {
		parts = append(parts, in.Type.String())
	}
	return fmt.Sprintf("%s(%s)", ev.Name, strings.Join(parts, ","))
}

func displayName(name string, idx int) string {
	if name != "" {
		return name
	}
	return fmt.Sprintf("arg%d", idx)
}

func exportedName(name string, idx int) string {
	if name == "" {
		return fmt.Sprintf("Arg%d", idx)
	}
	runes := []rune(name)
	runes[0] = unicode.ToUpper(runes[0])
	return string(runes)
}

func snakeCase(s string) string {
	var b strings.Builder
	for i, r := range s {
		if unicode.IsUpper(r) {
			if i > 0 {
				b.WriteByte('_')
			}
			b.WriteRune(unicode.ToLower(r))
			continue
		}
		b.WriteRune(r)
	}
	return b.String()
}

// contractAlias resolves the SQL-table namespace prefix from cfg, falling back
// to the ABI file's basename stripped of extension when ContractAlias is empty.
// The result is lowercased and stripped of non-identifier characters so the
// emitted table identifier is safe without quoting.
func contractAlias(cfg GenerateConfig) string {
	raw := cfg.ContractAlias
	if raw == "" {
		base := filepath.Base(cfg.AbiPath)
		if i := strings.IndexByte(base, '.'); i >= 0 {
			base = base[:i]
		}
		raw = base
	}
	var b strings.Builder
	for _, r := range raw {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			b.WriteRune(unicode.ToLower(r))
		case r == '_' || r == '-':
			b.WriteByte('_')
		}
	}
	out := b.String()
	if out == "" {
		out = "contract"
	}
	return out
}

func renderTemplate(name, tmplStr string, data any) ([]byte, error) {
	t, err := template.New(name).Funcs(template.FuncMap{
		"join":  strings.Join,
		"plus1": func(n int) int { return n + 1 },
	}).Parse(tmplStr)
	if err != nil {
		return nil, fmt.Errorf("parse template: %w", err)
	}
	var buf bytes.Buffer
	if err := t.Execute(&buf, data); err != nil {
		return nil, fmt.Errorf("execute template: %w", err)
	}
	formatted, err := format.Source(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("format source: %w\n---\n%s", err, buf.String())
	}
	return formatted, nil
}

func writeFile(path string, content []byte) error {
	if err := os.WriteFile(path, content, 0o644); err != nil {
		return fmt.Errorf("write %s: %w", path, err)
	}
	return nil
}
