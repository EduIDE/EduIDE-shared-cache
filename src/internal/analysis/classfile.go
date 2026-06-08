package analysis

import (
	"encoding/binary"
	"fmt"
)

// extractConstantPoolStrings parses a Java class file and returns every UTF-8
// string from its constant pool. Class names, method names, field names, and
// type descriptors all appear there verbatim, so scanning these strings is
// sufficient to detect forbidden API references without executing any code.
func extractConstantPoolStrings(data []byte) ([]string, error) {
	if len(data) < 10 {
		return nil, fmt.Errorf("class file too short (%d bytes)", len(data))
	}

	// Validate magic bytes: 0xCAFEBABE
	if data[0] != 0xCA || data[1] != 0xFE || data[2] != 0xBA || data[3] != 0xBE {
		return nil, fmt.Errorf("missing class file magic bytes")
	}

	// Skip magic (4) + minor_version (2) + major_version (2)
	pos := 8

	if pos+2 > len(data) {
		return nil, fmt.Errorf("truncated before constant_pool_count")
	}
	cpCount := int(binary.BigEndian.Uint16(data[pos : pos+2]))
	pos += 2

	var result []string

	// Pool indices run from 1 to cpCount-1.
	for i := 1; i < cpCount; i++ {
		if pos >= len(data) {
			return nil, fmt.Errorf("unexpected end of constant pool at entry %d", i)
		}

		tag := data[pos]
		pos++

		switch tag {
		case 1: // CONSTANT_Utf8 — variable length string
			if pos+2 > len(data) {
				return nil, fmt.Errorf("truncated Utf8 length at entry %d", i)
			}
			length := int(binary.BigEndian.Uint16(data[pos : pos+2]))
			pos += 2
			if pos+length > len(data) {
				return nil, fmt.Errorf("truncated Utf8 data at entry %d", i)
			}
			result = append(result, string(data[pos:pos+length]))
			pos += length

		case 3, 4: // CONSTANT_Integer, CONSTANT_Float
			pos += 4

		case 5, 6: // CONSTANT_Long, CONSTANT_Double — consume two pool slots
			pos += 8
			i++

		case 7, 8, 16, 19, 20: // Class, String, MethodType, Module, Package
			pos += 2

		case 9, 10, 11, 12, 17, 18: // Fieldref, Methodref, InterfaceMethodref, NameAndType, Dynamic, InvokeDynamic
			pos += 4

		case 15: // CONSTANT_MethodHandle
			pos += 3

		default:
			return nil, fmt.Errorf("unknown constant pool tag %d at entry %d", tag, i)
		}
	}

	return result, nil
}
