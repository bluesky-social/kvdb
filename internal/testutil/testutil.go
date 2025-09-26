package testutil

import (
	"math/rand/v2"
)

func RandString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"

	str := ""
	for range length {
		str += string(charset[rand.IntN(len(charset))])
	}
	return str
}
