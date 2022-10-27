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
	Key      string `json:"_key"`
	Sha      string `json:"sha"`
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
	doc.Payload0 = makeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.WithGeo {
		doc.Geo = makeRandomPolygon(source)
	}
	if docConfig.WithWords > 0 {
		doc.Words = makeRandomWords(int(docConfig.WithWords), source)
	}
	if docConfig.NumberFields < 2 {
		return
	}
	doc.Payload1 = makeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 3 {
		return
	}
	doc.Payload2 = makeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 4 {
		return
	}
	doc.Payload3 = makeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 5 {
		return
	}
	doc.Payload4 = makeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 6 {
		return
	}
	doc.Payload5 = makeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 7 {
		return
	}
	doc.Payload6 = makeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 8 {
		return
	}
	doc.Payload7 = makeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 9 {
		return
	}
	doc.Payload8 = makeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 10 {
		return
	}
	doc.Payload9 = makeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 11 {
		return
	}
	doc.Payloada = makeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 12 {
		return
	}
	doc.Payloadb = makeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 13 {
		return
	}
	doc.Payloadc = makeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 14 {
		return
	}
	doc.Payloadd = makeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 15 {
		return
	}
	doc.Payloade = makeRandomStringWithSpaces(int(payloadSize), source)
	if docConfig.NumberFields < 16 {
		return
	}
	doc.Payloadf = makeRandomStringWithSpaces(int(payloadSize), source)
}
