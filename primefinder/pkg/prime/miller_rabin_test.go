package prime

import "testing"

func TestIsPrime(t *testing.T) {
	// known primes
	primes := []uint64{
		2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37,
		97, 101, 104729,
		// large 64-bit prime
		18446744073709551557,
	}
	for _, p := range primes {
		if !IsPrime(p) {
			t.Errorf("IsPrime(%d) = false, want true", p)
		}
	}

	// known composites
	composites := []uint64{
		0, 1, 4, 6, 8, 9, 10, 15, 100,
		18446744073709551615, // 2^64-1 = 3 × 5 × 17 × 257 × ...
	}
	for _, c := range composites {
		if IsPrime(c) {
			t.Errorf("IsPrime(%d) = true, want false", c)
		}
	}
}
