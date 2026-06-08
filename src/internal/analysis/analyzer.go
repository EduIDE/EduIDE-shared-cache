package analysis

import (
	"archive/tar"
	"archive/zip"
	"bytes"
	"compress/gzip"
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

// Analyzer inspects Java bytecode for forbidden API usage.
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

// Check inspects data for forbidden API usage.
// Gradle cache entries are gzip-compressed tar archives — those are handled first.
// Plain ZIP/JAR files are also supported.
// Any other format is passed through silently (returns nil, nil).
func (a *Analyzer) Check(data []byte) ([]Violation, error) {
	switch {
	case isGzip(data):
		return a.checkGzipTar(data)
	case isZIP(data):
		seen := make(map[string]struct{})
		return a.checkZIP(data, seen)
	default:
		return nil, nil
	}
}

// checkGzipTar decompresses a gzip stream and scans the tar entries inside.
// .class files are scanned directly; .jar files inside the tar are opened as
// ZIPs and their .class files scanned one level deep.
func (a *Analyzer) checkGzipTar(data []byte) ([]Violation, error) {
	gr, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to open gzip stream: %w", err)
	}
	defer gr.Close()

	tr := tar.NewReader(gr)
	seen := make(map[string]struct{})
	var violations []Violation

	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read tar entry: %w", err)
		}

		entryData, err := io.ReadAll(tr)
		if err != nil {
			continue
		}

		switch {
		case strings.HasSuffix(hdr.Name, ".class"):
			// Direct class file — common for compileJava task output.
			className := strings.TrimSuffix(hdr.Name, ".class")
			v := a.scanClassBytes(className, entryData, seen)
			violations = append(violations, v...)

		case strings.HasSuffix(hdr.Name, ".jar"):
			// JAR inside the tar — common for the jar task output.
			v, err := a.checkZIP(entryData, seen)
			if err != nil {
				continue
			}
			violations = append(violations, v...)
		}
	}

	return violations, nil
}

// checkZIP opens data as a ZIP archive and scans every .class file inside.
func (a *Analyzer) checkZIP(data []byte, seen map[string]struct{}) ([]Violation, error) {
	zr, err := zip.NewReader(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("failed to open ZIP: %w", err)
	}

	var violations []Violation

	for _, f := range zr.File {
		if !strings.HasSuffix(f.Name, ".class") {
			continue
		}

		rc, err := f.Open()
		if err != nil {
			continue
		}
		classData, err := io.ReadAll(rc)
		rc.Close()
		if err != nil {
			continue
		}

		className := strings.TrimSuffix(f.Name, ".class")
		v := a.scanClassBytes(className, classData, seen)
		violations = append(violations, v...)
	}

	return violations, nil
}

// scanClassBytes parses a single .class file and returns any violations.
// seen deduplicates across multiple calls (shared within one Check invocation).
func (a *Analyzer) scanClassBytes(className string, data []byte, seen map[string]struct{}) []Violation {
	poolStrings, err := extractConstantPoolStrings(data)
	if err != nil {
		return nil
	}

	var violations []Violation

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

	return violations
}

// isGzip returns true when data begins with the gzip magic bytes.
func isGzip(data []byte) bool {
	return len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b
}

// isZIP returns true when data begins with the ZIP local file header signature.
func isZIP(data []byte) bool {
	return len(data) >= 4 &&
		data[0] == 0x50 && data[1] == 0x4B &&
		data[2] == 0x03 && data[3] == 0x04
}
