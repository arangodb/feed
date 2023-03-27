package datagen

import (
	"crypto/sha256"
	"fmt"
	"math/rand"
	"strconv"
)

var (
	wordList = []string{
		"Aldi Süd",
		"Aldi Nord",
		"Lidl",
		"Edeka",
		"Tengelmann",
		"Grosso",
		"allkauf",
		"neukauf",
		"Rewe",
		"Holdrio",
		"real",
		"Globus",
		"Norma",
		"Del Haize",
		"Spar",
		"Tesco",
		"Morrison",
	}
)

type Point []float64

type Poly struct {
	Type        string  `json:"type"`
	Coordinates []Point `json:"coordinates"`
}

type Doc struct {
	// Key is for the database, it is the hash value of Index
	Key string `json:"_key"`
	// Index: from 0 to some n-1, unique and global in the whole graph
	// For vertices only.
	Index uint64 `json:"-"`
	Sha   string `json:"sha"`
	// Label == prefix + "_" + localIndex (or == localIndex if prefix is empty)
	// Here prefix encodes the position
	// in the graph parse tree and local index is from 0 to some n-1 and
	// local w.r.t. the leaf in the parse tree where the vertex/edge
	// is constructed.
	// If prefix is empty, then Label == local
	Label string `json:"label,omitempty"`
	// For edges only, the hash value of Index of the from vertex
	From      string `json:"_from,omitempty"`
	FromIndex uint64 `json:"-"`
	ToIndex   uint64 `json:"-"`
	// For edges only, the hash value of Index of the to vertex
	To string `json:"_to,omitempty"`
	// For edges only, the Label of the from vertex
	FromLabel string `json:"fromId,omitempty"`
	// For edges only, the Label of the to vertex
	ToLabel  string `json:"toId,omitempty"`
	Payload0 string `json:"payload0"`
	Payload1 string `json:"payload1,omitempty"`
	Payload2 string `json:"payload2,omitempty"`
	Payload3 string `json:"payload3,omitempty"`
	Payload4 string `json:"payload4,omitempty"`
	Payload5 string `json:"payload5,omitempty"`
	Payload6 string `json:"payload6,omitempty"`
	Payload7 string `json:"payload7,omitempty"`
	Payload8 string `json:"payload8,omitempty"`
	Payload9 string `json:"payload9,omitempty"`
	Payloada string `json:"payloada,omitempty"`
	Payloadb string `json:"payloadb,omitempty"`
	Payloadc string `json:"payloadc,omitempty"`
	Payloadd string `json:"payloadd,omitempty"`
	Payloade string `json:"payloade,omitempty"`
	Payloadf string `json:"payloadf,omitempty"`
	Geo      *Poly  `Json:"geo,omitempty"`
	Words    string `json:"words,omitempty"`
}

// makeRandomPolygon makes a random GeoJson polygon.
func makeRandomPolygon(source *rand.Rand) *Poly {
	ret := Poly{Type: "polygon", Coordinates: make([]Point, 0, 5)}
	for i := 1; i <= 4; i += 1 {
		ret.Coordinates = append(ret.Coordinates,
			Point{source.Float64()*300.0 - 90.0, source.Float64()*160.0 - 80.0})
	}
	return &ret
}

// MakeRandomString creates slice of bytes for the provided length.
// Each byte is in range from 33 to 123. There are no spaces
func MakeRandomString(length int, source *rand.Rand) string {
	b := make([]byte, length, length)
	for i := 0; i < length; i++ {
		s := source.Int()%52 + 65
		if s >= 91 {
			s += 6
		}
		b[i] = byte(s)
	}
	return string(b)
}

// MakeRandomStringWithSpaces creates slice of bytes for the provided length.
// Each byte is in range from 33 to 123.
func MakeRandomStringWithSpaces(length int, source *rand.Rand) string {
	b := make([]byte, length, length)

	wordlen := source.Int()%17 + 3
	for i := 0; i < length; i++ {
		wordlen -= 1
		if wordlen == 0 {
			wordlen = source.Int()%17 + 3
			b[i] = byte(32)
		} else {
			s := source.Int()%52 + 65
			if s >= 91 {
				s += 6
			}
			b[i] = byte(s)
		}
	}
	return string(b)
}

func makeRandomWords(nr int, source *rand.Rand) string {
	b := make([]byte, 0, 15*nr)
	for i := 1; i <= nr; i += 1 {
		if i > 1 {
			b = append(b, ' ')
		}
		b = append(b, []byte(wordList[source.Int()%len(wordList)])...)
	}
	return string(b)
}

func KeyFromIndex(index uint64) string {
	x := fmt.Sprintf("%d", index)
	return fmt.Sprintf("%x", sha256.Sum256([]byte(x)))
}

func KeyFromLabel(label string) string {
	return fmt.Sprintf("%x", sha256.Sum256([]byte(label)))
}

func LabelFromIndex(prefix string, index uint64) string {
	return fmt.Sprintf("%s_%d", prefix, index)
}

func (doc *Doc) ShaKey(index int64, keySize int) {
	doc.Sha = KeyFromIndex(uint64(index))
	doc.Key = doc.Sha[0:keySize]
}

type DocumentConfig struct {
	SizePerDoc   int64
	Size         int64
	WithGeo      bool
	WithWords    int64
	KeySize      int64
	NumberFields int64
}

func (doc *Doc) FillData(docConfig *DocumentConfig, source *rand.Rand) {
	if docConfig.NumberFields > 16 {
		docConfig.NumberFields = 16
	} else if docConfig.NumberFields < 1 {
		docConfig.NumberFields = 1
	}
	payloadSize := (docConfig.SizePerDoc - docConfig.KeySize - 106) / docConfig.NumberFields
	// 106 is the approximate overhead for _id, _rev and structures
	if payloadSize < 0 {
		payloadSize = int64(5)
	}
	doc.Payload0 = MakeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.WithGeo {
		doc.Geo = makeRandomPolygon(source)
	}
	if docConfig.WithWords > 0 {
		doc.Words = makeRandomWords(int(docConfig.WithWords), source)
	}
	if docConfig.NumberFields < 2 {
		return
	}
	doc.Payload1 = MakeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 3 {
		return
	}
	doc.Payload2 = MakeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 4 {
		return
	}
	doc.Payload3 = MakeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 5 {
		return
	}
	doc.Payload4 = MakeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 6 {
		return
	}
	doc.Payload5 = MakeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 7 {
		return
	}
	doc.Payload6 = MakeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 8 {
		return
	}
	doc.Payload7 = MakeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 9 {
		return
	}
	doc.Payload8 = MakeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 10 {
		return
	}
	doc.Payload9 = MakeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 11 {
		return
	}
	doc.Payloada = MakeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 12 {
		return
	}
	doc.Payloadb = MakeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 13 {
		return
	}
	doc.Payloadc = MakeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 14 {
		return
	}
	doc.Payloadd = MakeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 15 {
		return
	}
	doc.Payloade = MakeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 16 {
		return
	}
	doc.Payloadf = MakeRandomStringWithSpaces(int(payloadSize), source)
}

type GraphGenerator interface {
	VertexChannel() chan *Doc
	EdgeChannel() chan *Doc
}

type Cyclic struct {
	n uint64 // Number of vertices
	V chan *Doc
	E chan *Doc
}

func (c *Cyclic) VertexChannel() chan *Doc {
	return c.V
}

func (c *Cyclic) EdgeChannel() chan *Doc {
	return c.E
}

func NewCyclicGraph(n uint64) GraphGenerator {
	// Will automatically be generated on the heap by escape analysis:
	c := Cyclic{n: n, V: make(chan *Doc, 1000), E: make(chan *Doc, 1000)}

	go func() { // Sender for vertices
		// Has access to c because it is a closure
		var i uint64
		for i = 1; i <= c.n; i += 1 {
			var d Doc
			d.Label = strconv.Itoa(int(i))
			c.V <- &d
		}
		close(c.V)
	}()

	go func() { // Sender for edges
		// Has access to c because it is a closure
		var i uint64
		for i = 1; uint64(i) <= c.n; i += 1 {
			var d Doc
			d.Label = strconv.Itoa(int(i))
			d.From = KeyFromIndex(i)
			to := i + 1
			if uint64(to) > c.n {
				to = 1
			}
			d.To = KeyFromIndex(to)
			c.E <- &d
		}
		close(c.E)
	}()

	return &c
}
