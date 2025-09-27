package env

import (
	"fmt"
	"log/slog"
	"net/http"
)

const unset = "unset"

var Version = unset

func VersionHandler(w http.ResponseWriter, r *http.Request) {
	_, err := fmt.Fprintf(w, "%s\n", Version)
	if err != nil {
		slog.Default().Warn("failed to write version to client", "err", err)
	}
}

func IsProd() bool {
	return Version != unset
}
