package transposer

import (
	"fmt"
	"io/ioutil"
	"os"
	"strings"

	"github.com/fatih/color"
	"github.com/pkg/errors"
	"github.com/sergi/go-diff/diffmatchpatch"
)

type RewriterContext struct {
	FilePath       string
	Diffs          []diffmatchpatch.Diff
	CurrentDiffIdx int
}

func (ctx *RewriterContext) nextDiff() *diffmatchpatch.Diff {
	if ctx.CurrentDiffIdx+1 < len(ctx.Diffs) {
		return &ctx.Diffs[ctx.CurrentDiffIdx+1]
	}
	return nil
}

func (ctx *RewriterContext) currentDiff() *diffmatchpatch.Diff {
	return &ctx.Diffs[ctx.CurrentDiffIdx]
}

func (ctx *RewriterContext) previousDiff() *diffmatchpatch.Diff {
	if ctx.CurrentDiffIdx > 0 {
		return &ctx.Diffs[ctx.CurrentDiffIdx-1]
	}
	return nil
}

func (ctx *RewriterContext) previousDiffText() string {
	return ctx.previousDiff().Text
}

func (ctx *RewriterContext) currentDiffText() string {
	return ctx.currentDiff().Text
}

func (ctx *RewriterContext) nextDiffText() string {
	return ctx.nextDiff().Text
}

func (ctx *RewriterContext) isPreviousDiffTypeEqual() bool {
	diff := ctx.previousDiff()
	if diff != nil && diff.Type == diffmatchpatch.DiffEqual {
		return true
	}
	return false
}

func (ctx *RewriterContext) isCurrentDiffTypeEqual() bool {
	diff := ctx.currentDiff()
	if diff != nil && diff.Type == diffmatchpatch.DiffEqual {
		return true
	}
	return false
}

func (ctx *RewriterContext) isNextDiffTypeEqual() bool {
	diff := ctx.nextDiff()
	if diff != nil && diff.Type == diffmatchpatch.DiffEqual {
		return true
	}
	return false
}

func (ctx *RewriterContext) beforeAroundLines() []string {
	if !ctx.isPreviousDiffTypeEqual() {
		return []string{}
	}

	lines := strings.Split(ctx.previousDiffText(), "\n")
	lineLen := len(lines)
	if lineLen > 3 {
		return lines[lineLen-4 : lineLen-1]
	} else if lineLen > 2 {
		return lines[lineLen-3 : lineLen-1]
	} else if lineLen > 1 {
		return lines[lineLen-2 : lineLen-1]
	}
	return []string{}
}

func (ctx *RewriterContext) afterAroundLines() []string {
	if !ctx.isNextDiffTypeEqual() {
		return []string{}
	}

	lines := strings.Split(ctx.nextDiffText(), "\n")
	lineLen := len(lines)
	if lineLen > 3 {
		return lines[0:3]
	} else if lineLen > 2 {
		return lines[0:2]
	} else if lineLen > 1 {
		return lines[0:1]
	}
	return []string{}
}

func (ctx *RewriterContext) splitCurrentDiffLines() []string {
	lines := strings.Split(ctx.currentDiffText(), "\n")
	if len(lines) > 1 {
		return lines[0 : len(lines)-1]
	}
	return lines
}

type Rewriter struct {
	Ctx *RewriterContext
}

func (*Rewriter) getDiff(fileData string, newFileData string) []diffmatchpatch.Diff {
	dmp := diffmatchpatch.New()
	textA, textB, lines := dmp.DiffLinesToChars(fileData, newFileData)
	diffs := dmp.DiffMain(textA, textB, false)
	return dmp.DiffCharsToLines(diffs, lines)
}

func (*Rewriter) printFilePath(filePath string) {
	color.New(color.FgYellow).Println(filePath)
	fmt.Println("")
}

func (r *Rewriter) splitCurrentDiffLines() []string {
	lines := strings.Split(r.Ctx.currentDiffText(), "\n")
	if len(lines) > 1 {
		return lines[0 : len(lines)-1]
	}
	return lines
}

func (*Rewriter) diffColor(diff *diffmatchpatch.Diff) *color.Color {
	if diff.Type == diffmatchpatch.DiffDelete {
		return color.New(color.FgRed)
	} else if diff.Type == diffmatchpatch.DiffInsert {
		return color.New(color.FgGreen)
	}
	return color.New(color.FgWhite)
}

func (*Rewriter) diffPrefix(diff *diffmatchpatch.Diff) string {
	if diff.Type == diffmatchpatch.DiffDelete {
		return "- "
	} else if diff.Type == diffmatchpatch.DiffInsert {
		return "+ "
	}
	return ""
}

func (r *Rewriter) printDiffForCurrentLine() {

	ctx := r.Ctx
	if ctx.isCurrentDiffTypeEqual() {
		return
	}

	for _, line := range ctx.beforeAroundLines() {
		fmt.Println(line)
	}

	diff := ctx.currentDiff()
	color := r.diffColor(diff)
	prefix := r.diffPrefix(diff)
	for _, line := range ctx.splitCurrentDiffLines() {
		color.Println(prefix, line)
	}

	for _, line := range ctx.afterAroundLines() {
		fmt.Println(line)
	}
}

func (r *Rewriter) printAllDiff(fileData string, newFileData string, filePath string) {
	diffs := r.getDiff(fileData, newFileData)
	r.printFilePath(filePath)
	r.Ctx.FilePath = filePath
	r.Ctx.Diffs = diffs
	for idx := range diffs {
		r.Ctx.CurrentDiffIdx = idx
		r.printDiffForCurrentLine()
	}
	fmt.Println("")
}

func (r *Rewriter) rewriteFile(inspectResult *InspectResult, isDryRun bool, transposeFunc func(packageName string) string) error {
	fileData, err := ioutil.ReadFile(inspectResult.Path)
	if err != nil {
		return errors.WithStack(err)
	}
	newFileData := make([]byte, 0)
	importedResults := inspectResult.ImportedResults
	importedResult := importedResults[0]
	startPos := importedResult.Start - 1
	fileSize := len(fileData)
	for pos := 0; pos < fileSize; pos++ {
		if pos == int(startPos) {
			transposedPackageName := transposeFunc(importedResult.PackageName)
			startDelim := "\""
			endDelim := "\"\n"
			packageNameWithDelim := startDelim + transposedPackageName + endDelim
			importStatement := packageNameWithDelim
			importLength := int(importedResult.End) - int(importedResult.Start)
			if importedResult.PackageAliasName != "" {
				importStatement = importedResult.PackageAliasName + " " + packageNameWithDelim
			}
			for _, byte := range []byte(importStatement) {
				newFileData = append(newFileData, byte)
			}
			pos += importLength
			if len(importedResults) > 1 {
				importedResults = importedResults[1:]
				importedResult = importedResults[0]
				startPos = importedResult.Start - 1
			}
		} else {
			newFileData = append(newFileData, fileData[pos])
		}
	}
	if isDryRun {
		r.printAllDiff(string(fileData), string(newFileData), inspectResult.Path)
	} else {
		ioutil.WriteFile(inspectResult.Path, newFileData, os.ModePerm)
	}
	return nil
}

func NewRewriter() *Rewriter {
	return &Rewriter{
		Ctx: &RewriterContext{},
	}
}

func (r *Rewriter) Rewrite(inspectResults []*InspectResult, isDryRun bool, transposeFunc func(packageName string) string) error {
	for _, inspectResult := range inspectResults {
		if err := r.rewriteFile(inspectResult, isDryRun, transposeFunc); err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}
