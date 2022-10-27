package datagen

import (
	"crypto/sha256"
	"fmt"
	"math/rand"
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
	Key     string `json:"_key"`
	Sha     string `json:"sha"`
	Payload string `json:"payload"`
	Geo     *Poly  `Json:"geo,omitempty"`
	Words   string `json:"words,omitempty"`
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

// makeRandomStringWithSpaces creates slice of bytes for the provided length.
// Each byte is in range from 33 to 123.
func makeRandomStringWithSpaces(length int, source *rand.Rand) string {
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

func KeyFromIndex(index int64) string {
	x := fmt.Sprintf("%d", index)
	return fmt.Sprintf("%x", sha256.Sum256([]byte(x)))
}

func (doc *Doc) ShaKey(index int64, keySize int) {
	doc.Sha = KeyFromIndex(index)
	doc.Key = doc.Sha[0:keySize]
}

func (doc *Doc) FillData(payloadSize int64, withGeo bool, withWords int, source *rand.Rand) {
	doc.Payload = makeRandomStringWithSpaces(int(payloadSize), source)
	if withGeo {
		doc.Geo = makeRandomPolygon(source)
	}
	if withWords > 0 {
		doc.Words = makeRandomWords(withWords, source)
	}
}
