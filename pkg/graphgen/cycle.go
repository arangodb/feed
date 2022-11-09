package graphgen

import (
	"strconv"

	"github.com/arangodb/feed/pkg/datagen"
)

type CycleGraphParameters struct {
	Length uint64
	Prefix string
}

func (c *CycleGraphParameters) MakeGraphGenerator() (GraphGenerator, error) {
	V := make(chan *datagen.Doc, batchSize())
	E := make(chan *datagen.Doc, batchSize())

	if c.Prefix != "" {
		c.Prefix += "_"
	}

	go func() { // Sender for vertices
		// Has access to c because it is a closure
		var i uint64
		for i = 0; uint64(i) < c.Length; i += 1 {
			var d datagen.Doc
			d.Index = strconv.Itoa(int(i))
			d.Key = datagen.KeyFromIndex(i)
			d.Label = c.Prefix + d.Index
			V <- &d
		}
		close(V)
	}()

	go func() { // Sender for edges
		// Has access to c because it is a closure
		var i uint64
		for i = 0; uint64(i) < c.Length; i += 1 {
			var d datagen.Doc
			d.Index = strconv.Itoa(int(i))
			d.Key = datagen.KeyFromIndex(i)
			d.From = datagen.KeyFromIndex(i)
			to := (i + 1) % c.Length
			d.To = datagen.KeyFromIndex(to)
			E <- &d
		}
		close(E)
	}()

	return &GraphGeneratorData{V: V, E: E,
		numberVertices: c.Length, numberEdges: c.Length}, nil
}
