package transposer

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"testing"
)

func TestTrasposer(t *testing.T) {
	instance := New()
	t.Run("inspect invalid go source", func(t *testing.T) {
		tmpfile, err := os.Create("invalid.go")
		if err != nil {
			t.Fatalf("%+v\n", err)
		}
		defer func() {
			tmpfile.Close()
			if err := os.Remove(tmpfile.Name()); err != nil {
				t.Fatalf("%+v\n", err)
			}
		}()
		if err := ioutil.WriteFile(tmpfile.Name(), []byte("invalid go source"), 0644); err != nil {
			t.Fatalf("%+v\n", err)
		}
		if err := instance.TransposeDryRun(regexp.MustCompile("^regexp"), ".", nil, func(packageName string) string {
			return packageName
		}); err != nil {
			t.Fatalf("%+v\n", err)
		}
	})
	t.Run("inspect multiple packages", func(t *testing.T) {
		tmpfile, err := os.Create("multiple.go")
		if err != nil {
			t.Fatalf("%+v\n", err)
		}
		defer func() {
			tmpfile.Close()
			if err := os.Remove(tmpfile.Name()); err != nil {
				t.Fatalf("%+v\n", err)
			}
		}()
		multiplePackageWithAliasSource := `
package hoge

import (
    sql "database/sql"
    sqldriver "database/sql/driver"
)
`
		if err := ioutil.WriteFile(tmpfile.Name(), []byte(multiplePackageWithAliasSource), 0644); err != nil {
			t.Fatalf("%+v\n", err)
		}
		if err := instance.TransposeDryRun(regexp.MustCompile("^database/sql"), ".", nil, func(packageName string) string {
			fmt.Println(packageName)
			return filepath.Join("go.knocknote.io/octillery", packageName)
		}); err != nil {
			t.Fatalf("%+v\n", err)
		}
	})
	t.Run("dry run", func(t *testing.T) {
		foundPackages := []string{}
		if err := instance.TransposeDryRun(regexp.MustCompile("^regexp"), ".", nil, func(packageName string) string {
			foundPackages = append(foundPackages, packageName)
			return packageName
		}); err != nil {
			t.Fatalf("%+v\n", err)
		}
		if len(foundPackages) == 0 {
			t.Fatal("cannot inspect imported package")
		}
	})
	t.Run("overwrite", func(t *testing.T) {
		if err := instance.Transpose(regexp.MustCompile("^unknown"), ".", []string{"tmp"}, func(packageName string) string {
			return packageName
		}); err != nil {
			t.Fatalf("%+v\n", err)
		}
	})
}
