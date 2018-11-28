package path

import (
	"path/filepath"
	"runtime"
)

// ThisFilePath returns path of file that this called.
func ThisFilePath() string {
	_, file, _, _ := runtime.Caller(1)
	return file
}

// ThisDirPath returns directory path includes file that this called.
func ThisDirPath() string {
	_, file, _, _ := runtime.Caller(1)
	return filepath.Dir(file)
}
