package redis

import (
	"golang.org/x/crypto/bcrypt"
)

var (
	bcryptCost = bcrypt.DefaultCost
)

func hashPassword(password string) ([]byte, error) {
	return bcrypt.GenerateFromPassword([]byte(password), bcryptCost)
}

func comparePasswords(hashedPassword []byte, password string) error {
	return bcrypt.CompareHashAndPassword(hashedPassword, []byte(password))
}
