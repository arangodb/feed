package graphgen

import (
	"testing"
)

func TestPow(t *testing.T) {
	if Pow(10, 0) != 1 ||
		Pow(10, 1) != 10 ||
		Pow(10, 2) != 100 ||
		Pow(10, 9) != 1000000000 {
		t.Errorf("Wrong Pow result with basis 10.")
	}
	if Pow(2, 0) != 1 ||
		Pow(2, 1) != 2 ||
		Pow(2, 2) != 4 ||
		Pow(2, 15) != 32768 ||
		Pow(2, 16) != 65536 {
		t.Errorf("Wrong Pow result with basis 2.")
	}
	if Pow(1, 0) != 1 ||
		Pow(1, 1) != 1 ||
		Pow(1, 2) != 1 ||
		Pow(1, 7) != 1 ||
		Pow(1, 16) != 1 {
		t.Errorf("Wrong Pow result with basis 1.")
	}
	if Pow(0, 0) != 1 ||
		Pow(0, 1) != 0 ||
		Pow(0, 2) != 0 ||
		Pow(0, 7) != 0 ||
		Pow(0, 16) != 0 {
		t.Errorf("Wrong Pow result with basis 0.")
	}
}
