package analysis

import (
	"archive/zip"
	"bytes"
	"fmt"
	"io"
	"strings"

	"github.com/kevingruber/gradle-cache/internal/config"
)

// Violation describes a single forbidden API reference found in a class file.
type Violation struct {
	Class    string `json:"class"`    // e.g. "com/student/Homework"
	Matched  string `json:"matched"`  // e.g. "java/net/Socket"
	Category string `json:"category"` // e.g. "network"
}

// Analyzer inspects JAR bytecode for forbidden API usage.
type Analyzer struct {
	rules []rule
}

type rule struct {
	category string
	patterns []string
}

// New creates an Analyzer whose active rules are determined by cfg.
func New(cfg config.StaticAnalysisConfig) *Analyzer {
	a := &Analyzer{}
	if cfg.CheckNetwork {
		a.rules = append(a.rules, rule{"network", networkPatterns})
	}
	if cfg.CheckExec {
		a.rules = append(a.rules, rule{"exec", execPatterns})
	}
	if cfg.CheckReflection {
		a.rules = append(a.rules, rule{"reflection", reflectionPatterns})
	}
	if cfg.CheckFilesystem {
		a.rules = append(a.rules, rule{"filesystem", filesystemPatterns})
	}
	return a
}

// Check inspects data as a JAR file and returns any forbidden API violations.
// Returns nil, nil when data is not a JAR (so non-JAR artifacts pass silently).
func (a *Analyzer) Check(data []byte) ([]Violation, error) {
	if !isJAR(data) {
		return nil, nil
	}

	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("failed to open JAR: %w", err)
	}

	seen := make(map[string]struct{})
	var violations []Violation

	for _, f := range zr.File {
		if !strings.HasSuffix(f.Name, ".class") {
			continue
		}

		rc, err := f.Open()
		if err != nil {
			// Skip unreadable entries rather than failing the whole check.
			continue
		}
		classData, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			continue
		}

		poolStrings, err := extractConstantPoolStrings(classData)
		if err != nil {
			// Malformed class file — skip it.
			continue
		}

		className := strings.TrimSuffix(f.Name, ".class")

		for _, s := range poolStrings {
			for _, r := range a.rules {
				for _, pattern := range r.patterns {
					if !strings.HasPrefix(s, pattern) {
						continue
					}
					key := className + "|" + s + "|" + r.category
					if _, dup := seen[key]; dup {
						continue
					}
					seen[key] = struct{}{}
					violations = append(violations, Violation{
						Class:    className,
						Matched:  s,
						Category: r.category,
					})
				}
			}
		}
	}

	return violations, nil
}

// isJAR returns true when data begins with the ZIP local file header signature.
// JARs are ZIP files, so this signature (PK\x03\x04) is the correct check.
func isJAR(data []byte) bool {
	return len(data) >= 4 &&
		data[0] == 0x50 && data[1] == 0x4B &&
		data[2] == 0x03 && data[3] == 0x04
}
