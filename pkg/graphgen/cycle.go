package graphgen

import (
	"strconv"

	"github.com/arangodb/feed/pkg/datagen"
)

type CycleGraphParameters struct {
	Length uint64
}

func (c *CycleGraphParameters) MakeGraphGenerator() GraphGenerator {
	V := make(chan *datagen.Doc, batchSize())
	E := make(chan *datagen.Doc, batchSize())

	go func() { // Sender for vertices
		// Has access to c because it is a closure
		var i int64
		for i = 1; uint64(i) <= c.Length; i += 1 {
			var d datagen.Doc
			d.Label = strconv.Itoa(int(i))
			d.Label = datagen.KeyFromIndex(i)
			V <- &d
		}
		close(V)
	}()

	go func() { // Sender for edges
		// Has access to c because it is a closure
		var i int64
		for i = 1; uint64(i) <= c.Length; i += 1 {
			var d datagen.Doc
			d.Label = strconv.Itoa(int(i))
			d.From = datagen.KeyFromIndex(i)
			to := i + 1
			if uint64(to) > c.Length {
				to = 1
			}
			d.To = datagen.KeyFromIndex(to)
			E <- &d
		}
		close(E)
	}()

	return &GraphGeneratorData{V: V, E: E,
		numberVertices: c.Length, numberEdges: c.Length}
}
