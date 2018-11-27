package internal

var loadedPlugin = map[string]bool{}

// IsLoadedPlugin used by plugin and connectin/adapter/plugin package to escape duplicated loading.
func IsLoadedPlugin(plugin string) bool {
	return loadedPlugin[plugin]
}

// SetLoadedPlugin used by plugin and connectin/adapter/plugin package to escape duplicated loading.
func SetLoadedPlugin(plugin string) {
	loadedPlugin[plugin] = true
}
