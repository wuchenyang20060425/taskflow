package tasks

import (
	"encoding/hex"
	"strings"
)

func ValidResult(id, filename string) bool {
	if !ValidID(id) || !strings.HasPrefix(filename, id+"-") || !strings.HasSuffix(filename, ".txt") {
		return false
	}
	token := strings.TrimSuffix(strings.TrimPrefix(filename, id+"-"), ".txt")
	if len(token) != 24 {
		return false
	}
	_, err := hex.DecodeString(token)
	return err == nil
}
