package internal

var loadedPlugin = map[string]bool{}

func IsLoadedPlugin(plugin string) bool {
	return loadedPlugin[plugin]
}

func SetLoadedPlugin(plugin string) {
	loadedPlugin[plugin] = true
}
