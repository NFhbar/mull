package codegen

import (
	"fmt"

	"github.com/ethereum/go-ethereum/accounts/abi"
)

// typeMapping describes how a Solidity type is rendered in Go and SQLite, and
// how an indexed topic word is decoded into the Go value.
type typeMapping struct {
	GoType  string
	SQLType string
	// TopicExpr is a Go expression template that takes the variable name of the
	// topic string (e.g. "log.Topics[1]") and returns the Go value. Empty when
	// the type is not legal as an indexed parameter (e.g. dynamic types).
	TopicExpr string
	// NeedsBigInt is true when the Go value is *big.Int.
	NeedsBigInt bool
	// NeedsCommon is true when the Go value is common.Address / common.Hash.
	NeedsCommon bool
}

// mapType returns the rendering metadata for a Solidity ABI type. v1 supports
// scalar value types; arrays, slices, and tuples are explicitly rejected.
func mapType(t abi.Type) (typeMapping, error) {
	switch t.T {
	case abi.AddressTy:
		return typeMapping{
			GoType:      "common.Address",
			SQLType:     "TEXT",
			TopicExpr:   "common.HexToAddress(%s)",
			NeedsCommon: true,
		}, nil
	case abi.BoolTy:
		return typeMapping{
			GoType:    "bool",
			SQLType:   "INTEGER",
			TopicExpr: "decodeBoolTopic(%s)",
		}, nil
	case abi.UintTy:
		if t.Size <= 64 {
			return typeMapping{
				GoType:    fmt.Sprintf("uint%d", t.Size),
				SQLType:   "INTEGER",
				TopicExpr: fmt.Sprintf("uint%d(decodeBigIntTopic(%%s).Uint64())", t.Size),
			}, nil
		}
		return typeMapping{
			GoType:      "*big.Int",
			SQLType:     "TEXT",
			TopicExpr:   "decodeBigIntTopic(%s)",
			NeedsBigInt: true,
		}, nil
	case abi.IntTy:
		if t.Size <= 64 {
			return typeMapping{
				GoType:    fmt.Sprintf("int%d", t.Size),
				SQLType:   "INTEGER",
				TopicExpr: fmt.Sprintf("int%d(decodeBigIntTopic(%%s).Int64())", t.Size),
			}, nil
		}
		return typeMapping{
			GoType:      "*big.Int",
			SQLType:     "TEXT",
			TopicExpr:   "decodeBigIntTopic(%s)",
			NeedsBigInt: true,
		}, nil
	case abi.FixedBytesTy:
		return typeMapping{
			GoType:    fmt.Sprintf("[%d]byte", t.Size),
			SQLType:   "TEXT",
			TopicExpr: fmt.Sprintf("decodeBytes%dTopic(%%s)", t.Size),
		}, nil
	case abi.BytesTy:
		return typeMapping{
			GoType:  "[]byte",
			SQLType: "BLOB",
			// Indexed dynamic types are stored as keccak hashes, not values.
			TopicExpr: "",
		}, nil
	case abi.StringTy:
		return typeMapping{
			GoType:    "string",
			SQLType:   "TEXT",
			TopicExpr: "",
		}, nil
	}
	return typeMapping{}, fmt.Errorf("unsupported abi type %q (kind=%d)", t.String(), t.T)
}

// goValueExpr renders a Go expression that converts a struct field to its
// SQLite-bind value. *big.Int → decimal string; common.Address → checksummed
// hex; [N]byte → hex-encoded; everything else passes through.
func goValueExpr(m typeMapping, fieldRef string) string {
	switch {
	case m.NeedsBigInt:
		return fmt.Sprintf("bigIntStr(%s)", fieldRef)
	case m.NeedsCommon:
		return fmt.Sprintf("%s.Hex()", fieldRef)
	case m.GoType == "[]byte":
		return fieldRef
	case len(m.GoType) >= 7 && m.GoType[0] == '[' && m.GoType[1] >= '0' && m.GoType[1] <= '9':
		return fmt.Sprintf("hexEncode(%s[:])", fieldRef)
	}
	return fieldRef
}
