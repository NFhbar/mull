package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/NFhbar/mull/internal/codegen"
	"github.com/NFhbar/mull/internal/config"
)

var codegenOutDir string

var codegenCmd = &cobra.Command{
	Use:   "codegen",
	Short: "Generate typed event structs, SQLite migrations, and sinks from a contract ABI",
	Long: `Reads the ABI JSON at config.abi_path and emits, for each event:
  - a typed Go struct
  - a SQLite CREATE TABLE migration with typed columns
  - a decoder + EventSink implementation

Output is written under --out (default: internal/gen, resolved against the
current working directory). The indexer imports internal/gen directly, so
once the generated files are committed the next build picks them up.`,
	SilenceUsage: true,
	RunE: func(cmd *cobra.Command, args []string) error {
		cfg, err := config.Load(cfgFile)
		if err != nil {
			return err
		}
		if cfg.AbiPath == "" {
			return fmt.Errorf("abi_path is required in %s for codegen", cfgFile)
		}
		if info, err := os.Stat(cfg.AbiPath); err != nil {
			return fmt.Errorf("abi_path %q: %w", cfg.AbiPath, err)
		} else if info.IsDir() {
			return fmt.Errorf("abi_path %q: is a directory, want a JSON file", cfg.AbiPath)
		}
		n, err := codegen.Generate(codegen.GenerateConfig{
			AbiPath: cfg.AbiPath,
			OutDir:  codegenOutDir,
		})
		if err != nil {
			return err
		}
		fmt.Fprintf(os.Stdout, "generated %d event(s) → %s\n", n, codegenOutDir)
		return nil
	},
}

func init() {
	codegenCmd.Flags().StringVar(&codegenOutDir, "out", "internal/gen", "output directory for generated files (resolved against CWD)")
	rootCmd.AddCommand(codegenCmd)
}
