package cmd

import "testing"

func TestServeCommandRegistration(t *testing.T) {
	if serveCmd.Use != "serve" {
		t.Fatalf("serveCmd.Use = %q, want %q", serveCmd.Use, "serve")
	}
	flag := serveCmd.Flags().Lookup("addr")
	if flag == nil {
		t.Fatal("--addr flag not registered")
	}
	if flag.DefValue != ":8080" {
		t.Fatalf("--addr default = %q, want %q", flag.DefValue, ":8080")
	}

	for _, c := range rootCmd.Commands() {
		if c == serveCmd {
			return
		}
	}
	t.Fatal("serveCmd not registered on rootCmd")
}
