package path

import (
	"path/filepath"
	"runtime"
)

func ThisFilePath() string {
	_, file, _, _ := runtime.Caller(1)
	return file
}

func ThisDirPath() string {
	_, file, _, _ := runtime.Caller(1)
	return filepath.Dir(file)
}
