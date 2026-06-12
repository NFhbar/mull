package codegen

import (
	"bytes"
	"flag"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
)

var updateGolden = flag.Bool("update", false, "regenerate testdata/golden/ from current codegen output")

func TestGenerate_Golden(t *testing.T) {
	tmp := t.TempDir()
	n, err := Generate(GenerateConfig{
		AbiPath: filepath.Join("testdata", "erc20.abi.json"),
		OutDir:  tmp,
	})
	if err != nil {
		t.Fatalf("Generate: %v", err)
	}
	if n != 2 {
		t.Fatalf("event count = %d, want 2", n)
	}

	goldenDir := filepath.Join("testdata", "golden")
	if *updateGolden {
		if err := os.RemoveAll(goldenDir); err != nil {
			t.Fatalf("clean golden: %v", err)
		}
		if err := os.MkdirAll(goldenDir, 0o755); err != nil {
			t.Fatalf("mkdir golden: %v", err)
		}
		entries, err := os.ReadDir(tmp)
		if err != nil {
			t.Fatalf("read tmp: %v", err)
		}
		for _, e := range entries {
			data, err := os.ReadFile(filepath.Join(tmp, e.Name()))
			if err != nil {
				t.Fatalf("read %s: %v", e.Name(), err)
			}
			if err := os.WriteFile(filepath.Join(goldenDir, e.Name()), data, 0o644); err != nil {
				t.Fatalf("write golden %s: %v", e.Name(), err)
			}
		}
		t.Logf("updated %d golden files", len(entries))
		return
	}

	gotFiles := listFiles(t, tmp)
	wantFiles := listFiles(t, goldenDir)
	if !equalSorted(gotFiles, wantFiles) {
		t.Fatalf("file set differs\n got: %v\nwant: %v", gotFiles, wantFiles)
	}
	for _, name := range gotFiles {
		got, err := os.ReadFile(filepath.Join(tmp, name))
		if err != nil {
			t.Fatalf("read got %s: %v", name, err)
		}
		want, err := os.ReadFile(filepath.Join(goldenDir, name))
		if err != nil {
			t.Fatalf("read want %s: %v", name, err)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("%s differs from golden (run with -update to refresh)\n---got---\n%s\n---want---\n%s", name, got, want)
		}
	}
}

func TestGenerate_RejectsMissingABI(t *testing.T) {
	_, err := Generate(GenerateConfig{
		AbiPath: filepath.Join("testdata", "does-not-exist.json"),
		OutDir:  t.TempDir(),
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.HasPrefix(err.Error(), "read abi:") {
		t.Fatalf("error = %v, want prefix 'read abi:'", err)
	}
}

func TestGenerate_RejectsTupleType(t *testing.T) {
	abiJSON := `[
		{
			"anonymous": false,
			"inputs": [
				{"indexed": false, "name": "data", "type": "tuple", "components": [
					{"name": "a", "type": "uint256"},
					{"name": "b", "type": "uint256"}
				]}
			],
			"name": "Bad",
			"type": "event"
		}
	]`
	dir := t.TempDir()
	abiPath := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(abiPath, []byte(abiJSON), 0o644); err != nil {
		t.Fatalf("setup: %v", err)
	}
	_, err := Generate(GenerateConfig{
		AbiPath: abiPath,
		OutDir:  filepath.Join(dir, "out"),
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "unsupported abi type") {
		t.Fatalf("error = %v, want 'unsupported abi type'", err)
	}
}

// TestGenerate_IndexedBytesN_NoMissingHelper verifies that an indexed bytesN
// arg generates syntactically valid Go that doesn't reference the missing
// decodeBytesNTopic family (regression for the Pass 2 bug where the emitted
// helper name had no matching definition). The parser walk catches the broad
// shape of that failure — emission referencing an undeclared function still
// parses, but the file as a whole must at least be syntactically valid Go.
func TestGenerate_IndexedBytesN_NoMissingHelper(t *testing.T) {
	abiJSON := `[
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "key", "type": "bytes32"},
				{"indexed": false, "name": "value", "type": "uint256"}
			],
			"name": "Pinged",
			"type": "event"
		}
	]`
	dir := t.TempDir()
	abiPath := filepath.Join(dir, "pinged.json")
	if err := os.WriteFile(abiPath, []byte(abiJSON), 0o644); err != nil {
		t.Fatalf("setup: %v", err)
	}
	outDir := filepath.Join(dir, "out")
	if _, err := Generate(GenerateConfig{AbiPath: abiPath, OutDir: outDir}); err != nil {
		t.Fatalf("Generate: %v", err)
	}
	for _, name := range []string{"pinged.go", "helpers.go"} {
		path := filepath.Join(outDir, name)
		got, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("read %s: %v", name, err)
		}
		if _, err := parser.ParseFile(token.NewFileSet(), path, got, parser.AllErrors); err != nil {
			t.Fatalf("%s does not parse: %v\n---src---\n%s", name, err, got)
		}
	}
	pinged, err := os.ReadFile(filepath.Join(outDir, "pinged.go"))
	if err != nil {
		t.Fatalf("read pinged.go: %v", err)
	}
	helpers, err := os.ReadFile(filepath.Join(outDir, "helpers.go"))
	if err != nil {
		t.Fatalf("read helpers.go: %v", err)
	}
	// Pass 2 bug shape: the emitter referenced a helper named decodeBytesNTopic
	// (or any decodeBytes<digits>Topic flavor) that helpersTemplate did not
	// define. Guard against regressions in that specific shape.
	for _, name := range []string{"decodeBytes32Topic", "decodeBytesNTopic"} {
		if bytes.Contains(pinged, []byte(name)) {
			t.Fatalf("generated pinged.go references missing helper %q:\n%s", name, pinged)
		}
	}
	// The current emission must use decodeFixedBytesTopic (defined in
	// helpersTemplate) — assert both the use site and the definition.
	if !bytes.Contains(pinged, []byte("decodeFixedBytesTopic(log.Topics[1], 32)")) {
		t.Fatalf("generated pinged.go missing expected decodeFixedBytesTopic call:\n%s", pinged)
	}
	if !bytes.Contains(helpers, []byte("func decodeFixedBytesTopic(")) {
		t.Fatalf("generated helpers.go missing decodeFixedBytesTopic definition:\n%s", helpers)
	}
}

func TestPlanEvent_ColumnSignature(t *testing.T) {
	abiJSON := `[
		{
			"anonymous": false,
			"inputs": [
				{"indexed": true, "name": "from", "type": "address"},
				{"indexed": true, "name": "to", "type": "address"},
				{"indexed": false, "name": "value", "type": "uint256"}
			],
			"name": "Transfer",
			"type": "event"
		}
	]`
	parsed, err := abi.JSON(strings.NewReader(abiJSON))
	if err != nil {
		t.Fatalf("parse abi: %v", err)
	}
	p, err := planEvent(parsed.Events["Transfer"], "erc20")
	if err != nil {
		t.Fatalf("planEvent: %v", err)
	}
	want := "source:TEXT,block_number:INTEGER,tx_hash:TEXT,log_index:INTEGER,from:TEXT,to:TEXT,value:TEXT"
	if p.Signature != want {
		t.Fatalf("Signature = %q, want %q", p.Signature, want)
	}
}

func TestGenerate_RequiresOutDir(t *testing.T) {
	_, err := Generate(GenerateConfig{
		AbiPath: filepath.Join("testdata", "erc20.abi.json"),
		OutDir:  "",
	})
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "out dir is required") {
		t.Fatalf("error = %v, want 'out dir is required'", err)
	}
}

func listFiles(t *testing.T, dir string) []string {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read dir %s: %v", dir, err)
	}
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		out = append(out, e.Name())
	}
	sort.Strings(out)
	return out
}

func equalSorted(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
