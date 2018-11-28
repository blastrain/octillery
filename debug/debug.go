package debug

import (
	"fmt"
	"os"
	"runtime"
)

// DEBUG variable for DEBUG mode
var DEBUG bool

// SetDebug set whether debug mode or not.
//
// If set true, print to console raw SQL or sharding database.
func SetDebug(isDebug bool) {
	DEBUG = isDebug
}

// Printf print message if DEBUG mode
func Printf(format string, args ...interface{}) {
	if DEBUG {
		_, file, line, _ := runtime.Caller(1)
		debugHeader := fmt.Sprintf("[DEBUG:(%s:%d)]", file, line)
		debugMsg := fmt.Sprintf("%s %s\n", debugHeader, format)
		fmt.Fprintf(os.Stdout, debugMsg, args...)
	}
}
