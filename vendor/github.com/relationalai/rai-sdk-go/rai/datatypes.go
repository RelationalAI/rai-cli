// Copyright 2022 RelationalAI, Inc.

package rai

// Support for Rel data types that don't have a native golang equivalent.

import (
	"math/big"
	"time"

	"github.com/shopspring/decimal"
)

// Milliseconds in a day
const dayMillis = 1 * 24 * 60 * 60 * 1000

// Start of epoch time in days since 1AD (Rata Die)
//
//	https://en.wikipedia.org/wiki/Rata_Die
const epochStartDays int64 = 719163

// Start of epoch time in milliseconds since 1AD
const epochStartMillis int64 = epochStartDays * dayMillis

// Returns the epoch millis corresponding to the given Rata Die day number.
func DateFromRataDie(d int64) time.Time {
	d = d - epochStartDays // epoch day
	m := d * dayMillis     // epoch millis
	return time.UnixMilli(m).UTC()
}

// Returns the epoch millis corresponding to the given millis since 1AD.
func DateFromRataMillis(d int64) time.Time {
	d = d - epochStartMillis // millis since epoch
	return time.UnixMilli(d).UTC()
}

func NewBigInt128(lo, hi uint64) *big.Int {
	result := new(big.Int).SetBits([]big.Word{big.Word(lo), big.Word(hi)})
	if int64(hi) < 0 {
		result.Neg(result)
	}
	return result
}

func NewBigUint128(lo, hi uint64) *big.Int {
	return new(big.Int).SetBits([]big.Word{big.Word(lo), big.Word(hi)})
}

func NewDecimal128(lo, hi uint64, digits int32) decimal.Decimal {
	b := NewBigInt128(lo, hi)
	return decimal.NewFromBigInt(b, int32(digits))
}

func NewRational128(n, d *big.Int) *big.Rat {
	bn := new(big.Rat).SetInt(n)
	bd := new(big.Rat).SetInt(d)
	return new(big.Rat).Mul(bn, bd.Inv(bd))
}
