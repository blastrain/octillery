package transposer

import (
	"regexp"

	"github.com/pkg/errors"
)

// Transposer replace import statement
type Transposer struct {
	Inspector *Inspector
	Rewriter  *Rewriter
}

// Transpose replace import statement and save it.
func (t *Transposer) Transpose(matchPattern *regexp.Regexp, searchRoot string, ignorePaths []string, transposeFunc func(packageName string) string) error {
	inspectResults, err := t.Inspector.Inspect(matchPattern, searchRoot, ignorePaths)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(t.Rewriter.Rewrite(inspectResults, false, transposeFunc))
}

// TransposeDryRun print diff to replace import statement ( not overwriting )
func (t *Transposer) TransposeDryRun(matchPattern *regexp.Regexp, searchRoot string, ignorePaths []string, transposeFunc func(packageName string) string) error {
	inspectResults, err := t.Inspector.Inspect(matchPattern, searchRoot, ignorePaths)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(t.Rewriter.Rewrite(inspectResults, true, transposeFunc))
}

// New creates instance of Transposer
func New() *Transposer {
	return &Transposer{
		Inspector: NewInspector(),
		Rewriter:  NewRewriter(),
	}
}
