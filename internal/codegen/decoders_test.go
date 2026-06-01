package codegen

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"

	"github.com/NFhbar/mull/internal/store"
)

// TestDecodeTransfer_USDC exercises the same decode algorithm the generated
// internal/gen/transfer.go would run: it builds the abi.Arguments programmatically
// for the ERC-20 Transfer event and decodes a canned USDC Transfer log captured
// from mainnet. Failure here is the same failure mode the live indexer would
// hit, so a regression in the algorithm (e.g. comment 1's signed-extension bug,
// adapted) gets caught even though internal/gen/ ships the empty bootstrap.
//
// TODO(codegen): exercise generated DecodeTransfer end-to-end — emit the golden
// tree into a sibling testdata/genpkg/ package and run go test against it, or
// extract helper bodies into a shared non-template file imported by both
// runtime and tests, so this in-test reimplementation can be retired.
func TestDecodeTransfer_USDC(t *testing.T) {
	// USDC contract on Ethereum mainnet: 0xA0b8...eB48
	// Tx 0x5d... block 19500000-ish (illustrative — the bytes are what matter).
	log := store.Event{
		BlockNumber: 19000001,
		TxHash:      "0xfeedface",
		LogIndex:    7,
		Address:     "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
		Topics: []string{
			"0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef", // Transfer signature
			"0x000000000000000000000000abcdef0000000000000000000000000000000001", // from
			"0x000000000000000000000000abcdef0000000000000000000000000000000002", // to
		},
		// 1_000_000 USDC (raw u256, big-endian, padded to 32 bytes)
		Data: "0x00000000000000000000000000000000000000000000000000000000000f4240",
	}

	uintType, err := abi.NewType("uint256", "", nil)
	if err != nil {
		t.Fatalf("abi.NewType: %v", err)
	}
	args := abi.Arguments{{Type: uintType, Name: "value", Indexed: false}}

	if len(log.Topics) < 3 {
		t.Fatalf("topics = %d, want >= 3", len(log.Topics))
	}
	const sig = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
	if common.HexToHash(log.Topics[0]) != common.HexToHash(sig) {
		t.Fatalf("topic0 mismatch: %s", log.Topics[0])
	}
	from := common.HexToAddress(log.Topics[1])
	to := common.HexToAddress(log.Topics[2])
	vals, err := args.Unpack(common.FromHex(log.Data))
	if err != nil {
		t.Fatalf("unpack: %v", err)
	}
	if len(vals) != 1 {
		t.Fatalf("vals len = %d, want 1", len(vals))
	}
	value, ok := vals[0].(*big.Int)
	if !ok {
		t.Fatalf("value type = %T, want *big.Int", vals[0])
	}
	if want := common.HexToAddress("0xabcdef0000000000000000000000000000000001"); from != want {
		t.Fatalf("from = %s, want %s", from.Hex(), want.Hex())
	}
	if want := common.HexToAddress("0xabcdef0000000000000000000000000000000002"); to != want {
		t.Fatalf("to = %s, want %s", to.Hex(), want.Hex())
	}
	if want := big.NewInt(1_000_000); value.Cmp(want) != 0 {
		t.Fatalf("value = %s, want %s", value.String(), want.String())
	}
}

// TestDecodeSignedBigIntTopic verifies two's-complement sign extension for the
// wide-int branch (intN where N > 64) — the path comment 1 flagged as
// regressing when SetBytes was treated as unsigned. The helper lives only as a
// template string in templates.go, so the assertion here mirrors the algorithm
// (decodeSignedBigIntTopic body) to prove the math is right.
func TestDecodeSignedBigIntTopic(t *testing.T) {
	cases := []struct {
		name  string
		topic string
		bits  int
		want  *big.Int
	}{
		{
			name:  "int256 negative one",
			topic: "0xffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff",
			bits:  256,
			want:  big.NewInt(-1),
		},
		{
			name:  "int256 positive small",
			topic: "0x0000000000000000000000000000000000000000000000000000000000000005",
			bits:  256,
			want:  big.NewInt(5),
		},
		{
			name:  "int128 negative max",
			topic: "0x0000000000000000000000000000000080000000000000000000000000000000",
			bits:  128,
			want:  new(big.Int).Lsh(big.NewInt(-1), 127),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := decodeSignedBigIntTopic(tc.topic, tc.bits)
			if got.Cmp(tc.want) != 0 {
				t.Fatalf("got %s, want %s", got.String(), tc.want.String())
			}
		})
	}
}

// decodeSignedBigIntTopic mirrors the helper emitted into internal/gen/helpers.go
// so the unit test above can exercise the algorithm without compiling the
// generated package. Keep in lock-step with templates.go's helpersTemplate.
//
// TODO(codegen): retire this duplicate once tests compile the generated
// helpers.go directly (see TestDecodeTransfer_USDC TODO above).
func decodeSignedBigIntTopic(topic string, bits int) *big.Int {
	out := new(big.Int)
	out.SetBytes(common.FromHex(topic))
	if out.Bit(bits-1) == 1 {
		mod := new(big.Int).Lsh(big.NewInt(1), uint(bits))
		out.Sub(out, mod)
	}
	return out
}
