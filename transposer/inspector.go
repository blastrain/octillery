package transposer

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"

	"github.com/pkg/errors"
)

// InspectResult has slice of InspectImportedResult
type InspectResult struct {
	Path            string
	ImportedResults []InspectImportedResult
}

// InspectImportedResult has information of import statement
type InspectImportedResult struct {
	Path             string
	PackageName      string
	PackageAliasName string
	Start            token.Pos
	End              token.Pos
}

// Inspector inspects import statement
type Inspector struct {
	ignorePaths []*regexp.Regexp
}

var (
	gitDirPattern                 = regexp.MustCompile("^.git")
	goTestSourcePattern           = regexp.MustCompile("_test.go$")
	goSourcePattern               = regexp.MustCompile("\\.go$")
	octilleryIgnoreSourcePatterns = importDatabaseSQLPackagePatterns()
)

func importDatabaseSQLPackagePatterns() []*regexp.Regexp {
	patterns := []*regexp.Regexp{}
	basePath := filepath.Join("go.knocknote.io", "octillery.*")
	for _, path := range []string{
		"algorithm",
		"connection",
		"database",
		"exec",
		"migrator",
		"octillery\\.go",
		"plugin",
		"printer",
		"cmd",
	} {
		patterns = append(patterns, regexp.MustCompile(filepath.Join(basePath, path)))
	}
	return patterns
}

func (*Inspector) isInspectTargetGoSource(path string) bool {
	if gitDirPattern.MatchString(path) {
		return false
	}
	if goTestSourcePattern.MatchString(path) {
		return false
	}
	if !goSourcePattern.MatchString(path) {
		return false
	}
	for _, pattern := range octilleryIgnoreSourcePatterns {
		if pattern.MatchString(path) {
			return false
		}
	}
	return true
}

func (i *Inspector) inspectForPath(matchPattern *regexp.Regexp, path string) *InspectResult {
	if !i.isInspectTargetGoSource(path) {
		return nil
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, path, nil, parser.ImportsOnly)
	if err != nil {
		// ignore if invalid go source
		return nil
	}
	inspectImportedResults := make([]InspectImportedResult, 0)
	for _, imported := range file.Imports {
		packageNameWithDoubleQuotation := imported.Path.Value
		packageNameOnly := packageNameWithDoubleQuotation[1 : len(packageNameWithDoubleQuotation)-1]
		if matchPattern.MatchString(packageNameOnly) {
			packageAliasName := ""
			if imported.Name != nil {
				packageAliasName = imported.Name.Name
			}
			inspectImportedResults = append(inspectImportedResults, InspectImportedResult{
				Path:             path,
				PackageAliasName: packageAliasName,
				PackageName:      packageNameOnly,
				Start:            imported.Pos(),
				End:              imported.End(),
			})
		}
	}
	if len(inspectImportedResults) > 0 {
		return &InspectResult{
			Path:            path,
			ImportedResults: inspectImportedResults,
		}
	}
	return nil
}

// NewInspector creates instance of Inspector
func NewInspector() *Inspector {
	return &Inspector{
		ignorePaths: []*regexp.Regexp{},
	}
}

func (i *Inspector) isIgnorePath(path string) bool {
	if len(i.ignorePaths) == 0 {
		return false
	}
	for _, ignorePath := range i.ignorePaths {
		if ignorePath.MatchString(path) {
			return true
		}
	}
	return false
}

func (i *Inspector) setupIgnorePaths(paths []string) error {
	for _, path := range paths {
		absPath, err := filepath.Abs(path)
		if err != nil {
			return errors.WithStack(err)
		}
		i.ignorePaths = append(i.ignorePaths, regexp.MustCompile(absPath))
	}
	return nil
}

// Inspect inspect import statement of go file under the specified path.
func (i *Inspector) Inspect(matchPattern *regexp.Regexp, searchRoot string, ignorePaths []string) ([]*InspectResult, error) {
	if err := i.setupIgnorePaths(ignorePaths); err != nil {
		return nil, errors.WithStack(err)
	}
	inspectResults := []*InspectResult{}
	if err := filepath.Walk(searchRoot, func(path string, info os.FileInfo, err error) error {
		absPath, err := filepath.Abs(path)
		if err != nil {
			return errors.WithStack(err)
		}
		if i.isIgnorePath(absPath) {
			return nil
		}
		inspectResult := i.inspectForPath(matchPattern, absPath)
		if inspectResult != nil {
			inspectResults = append(inspectResults, inspectResult)
		}
		return nil
	}); err != nil {
		return nil, errors.WithStack(err)
	}
	return inspectResults, nil
}
