package datagen

import (
// 	"crypto/sha256"
// 	"fmt"
// 	"math/rand"
	"strconv"
	"github.com/arangodb/feed/pkg/datagen"
)

type GraphGenerator interface {
	VertexChannel() chan *datagen.Doc
	EdgeChannel() chan *datagen.Doc
}

type Cyclic struct {
	n int64 // Number of vertices
	V chan *datagen.Doc
	E chan *datagen.Doc
}

func (c *Cyclic) VertexChannel() chan *datagen.Doc {
	return c.V
}

func (c *Cyclic) EdgeChannel() chan *datagen.Doc {
	return c.E
}

func NewCyclicGraph(n int64) GraphGenerator {
	// Will automatically be generated on the heap by escape analysis:
	c := Cyclic{n: n, V: make(chan *datagen.Doc, 1000), E: make(chan *datagen.Doc, 1000)}

	go func() { // Sender for vertices
		// Has access to c because it is a closure
		var i int64
		for i = 1; i <= c.n; i += 1 {
			var d datagen.Doc
			d.Label = strconv.Itoa(int(i))
			c.V <- &d
		}
		close(c.V)
	}()

	go func() { // Sender for edges
		// Has access to c because it is a closure
		var i int64
		for i = 1; i <= c.n; i += 1 {
			var d datagen.Doc
			d.Label = strconv.Itoa(int(i))
			d.From = datagen.KeyFromIndex(i)
			to := i + 1
			if to > c.n {
				to = 1
			}
			d.To = datagen.KeyFromIndex(to)
			c.E <- &d
		}
		close(c.E)
	}()

	return &c
}
