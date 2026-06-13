package slogseq

import "strings"

// dottedToNested converts a flat map with dotted keys ("a.b.c") into a
// nested map structure. Used for ResourceAttributes encoding.
func dottedToNested(props map[string]any) map[string]any {
	out := make(map[string]any, len(props))

	for k, v := range props {
		path := strings.Split(k, ".")
		addNested(out, path, v)
	}

	return out
}

func addNested(dst map[string]any, path []string, val any) {
	if len(path) == 1 {
		dst[path[0]] = val

		return
	}

	head := path[0]
	child, ok := dst[head].(map[string]any)
	if !ok {
		child = make(map[string]any)
		dst[head] = child
	}

	addNested(child, path[1:], val)
}
