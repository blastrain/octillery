package transposer

import (
	"testing"

	"github.com/fatih/color"
	"github.com/sergi/go-diff/diffmatchpatch"
)

var deleteDiff = diffmatchpatch.Diff{
	Type: diffmatchpatch.DiffDelete,
	Text: "ABCDEFG",
}

var insertDiff = diffmatchpatch.Diff{
	Type: diffmatchpatch.DiffInsert,
	Text: "ABCDEFG",
}

var equalDiff = diffmatchpatch.Diff{
	Type: diffmatchpatch.DiffEqual,
	Text: "ABCDEFG",
}

func TestDiffColor(t *testing.T) {
	r := NewRewriter()
	c := r.diffColor(&deleteDiff)
	if !c.Equals(color.New(color.FgRed)) {
		t.Error("cannot get color from delete diff")
	}
	c = r.diffColor(&insertDiff)
	if !c.Equals(color.New(color.FgGreen)) {
		t.Error("cannot get color from insert diff")
	}
	c = r.diffColor(&equalDiff)
	if !c.Equals(color.New(color.FgWhite)) {
		t.Error("cannot get color from equal diff")
	}
}

func TestDiffPrefix(t *testing.T) {
	r := NewRewriter()
	prefix := r.diffPrefix(&deleteDiff)
	if prefix != "- " {
		t.Error("cannot get prefix from delete diff")
	}
	prefix = r.diffPrefix(&insertDiff)
	if prefix != "+ " {
		t.Error("cannot get prefix from insert diff")
	}
	prefix = r.diffPrefix(&equalDiff)
	if prefix != "" {
		t.Error("cannot get prefix from equal diff")
	}
}

var textA = `
import (
  "fmt"
  "regexp"
  "testing"
  "time"
)
`
var textB = `
import (
  "fmt"
  "github.com/user/extend_regexp/regexp"
  "testing"
  "time"
)
`

func TestGetDiff(t *testing.T) {
	diffs := NewRewriter().getDiff(textA, textB)
	foundDiff := false
	for _, diff := range diffs {
		if diff.Type != diffmatchpatch.DiffEqual {
			foundDiff = true
		}
	}
	if !foundDiff {
		t.Error("cannot get diff")
	}
}

func getDiffContext() *Rewriter {
	r := NewRewriter()
	diffs := r.getDiff(textA, textB)
	r.ctx.Diffs = diffs
	for idx, diff := range diffs {
		if diff.Type != diffmatchpatch.DiffEqual {
			r.ctx.CurrentDiffIdx = idx
			break
		}
	}
	return r
}

func TestSplitCurrentDiffLines(t *testing.T) {
	r := getDiffContext()
	lines := r.splitCurrentDiffLines()
	if len(lines) == 1 && lines[0] == "regexp" {
		t.Error("cannot split diff text by '\n'", lines)
	}
}

func TestBeforeAroundLines(t *testing.T) {
	r := getDiffContext()
	lines := r.ctx.beforeAroundLines()
	if len(lines) != 3 {
		t.Error("cannot exactly get around lines", lines)
	}
}

func TestAfterAroundLiens(t *testing.T) {
	r := getDiffContext()
	lines := r.ctx.afterAroundLines()
	if len(lines) != 0 {
		t.Error("cannot exactly get around lines", lines)
	}
}
