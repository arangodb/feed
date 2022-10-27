package operations

import (
	"github.com/arangodb/feed/pkg/feedlang"
)

// init sets up the various operations and links them to feedlang
func Init() {
	if feedlang.Atoms == nil {
		feedlang.Atoms = make(map[string]feedlang.Maker, 100)
	}
	feedlang.Atoms["normal"] = NewNormalProg
}
