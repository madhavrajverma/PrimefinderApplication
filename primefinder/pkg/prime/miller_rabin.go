package prime

// IsPrime tests whether n is prime.
// Uses deterministic Miller-Rabin with 12 witnesses —
// correct for ALL 64-bit unsigned integers.
func IsPrime(n uint64) bool {
	if n < 2 {
		return false
	}
	// small primes fast path
	if n == 2 || n == 3 || n == 5 || n == 7 {
		return true
	}
	if n%2 == 0 || n%3 == 0 || n%5 == 0 {
		return false
	}

	// write n-1 as 2^r * d  where d is odd
	d := n - 1
	r := uint64(0)
	for d%2 == 0 {
		d /= 2
		r++
	}

	// deterministic witnesses sufficient for n < 3.3 × 10^24
	// covers entire uint64 range
	witnesses := []uint64{2, 3, 5, 7, 11, 13, 17, 19, 23, 29, 31, 37}

	for _, a := range witnesses {
		if a >= n {
			continue
		}

		x := modpow(a, d, n)
		if x == 1 || x == n-1 {
			continue
		}

		composite := true
		for i := uint64(0); i < r-1; i++ {
			x = mulmod(x, x, n)
			if x == n-1 {
				composite = false
				break
			}
		}
		if composite {
			return false
		}
	}
	return true
}

// mulmod computes (a * b) % m safely without uint64 overflow.
// Uses binary multiplication (Russian peasant method).
func mulmod(a, b, m uint64) uint64 {
	result := uint64(0)
	a %= m
	for b > 0 {
		if b&1 == 1 {
			result = addmod(result, a, m)
		}
		a = addmod(a, a, m)
		b >>= 1
	}
	return result
}

// addmod computes (a + b) % m safely without overflow.
func addmod(a, b, m uint64) uint64 {
	a %= m
	b %= m
	if a >= m-b {
		return a - (m - b)
	}
	return a + b
}

// modpow computes base^exp % m using fast exponentiation.
func modpow(base, exp, m uint64) uint64 {
	if m == 1 {
		return 0
	}
	result := uint64(1)
	base %= m
	for exp > 0 {
		if exp&1 == 1 {
			result = mulmod(result, base, m)
		}
		exp >>= 1
		base = mulmod(base, base, m)
	}
	return result
}
