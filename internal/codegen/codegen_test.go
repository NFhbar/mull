package codegen

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
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
