package transposer

import (
	"regexp"

	"github.com/pkg/errors"
)

type Transposer struct {
	Inspector *Inspector
	Rewriter  *Rewriter
}

func (t *Transposer) Transpose(matchPattern *regexp.Regexp, searchRoot string, ignorePaths []string, transposeFunc func(packageName string) string) error {
	inspectResults, err := t.Inspector.Inspect(matchPattern, searchRoot, ignorePaths)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(t.Rewriter.Rewrite(inspectResults, false, transposeFunc))
}

func (t *Transposer) TransposeDryRun(matchPattern *regexp.Regexp, searchRoot string, ignorePaths []string, transposeFunc func(packageName string) string) error {
	inspectResults, err := t.Inspector.Inspect(matchPattern, searchRoot, ignorePaths)
	if err != nil {
		return errors.WithStack(err)
	}
	return errors.WithStack(t.Rewriter.Rewrite(inspectResults, true, transposeFunc))
}

func New() *Transposer {
	return &Transposer{
		Inspector: NewInspector(),
		Rewriter:  NewRewriter(),
	}
}
