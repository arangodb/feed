package operations

import (
    "github.com/neunhoef/feed/feedlang"
)

// init sets up the various operations and links them to feedlang
func init() {
  if feedlang.Atoms == nil {
      feedlang.Atoms = make(map[string]feedlang.Maker, 100)
  }
  feedlang.Atoms["normal"] = NewNormalProg
}
