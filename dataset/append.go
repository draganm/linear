package dataset

import (
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/draganm/statemate"
)

func (d *Dataset) Append(w http.ResponseWriter, r *http.Request) {
	log := d.log.With("method", r.Method, "path", r.URL.Path)
	indexString := r.PathValue("index")

	if indexString == "" {
		log.Error("index not provided")
		http.Error(w, "index not provided", http.StatusBadRequest)
		return
	}

	index, err := strconv.ParseUint(indexString, 10, 64)
	if err != nil {
		log.Error("failed to parse index", "error", err)
		http.Error(w, "invalid index", http.StatusBadRequest)
		return
	}

	data, err := io.ReadAll(r.Body)
	if err != nil {
		log.Error("failed to read body", "error", err)
		http.Error(w, "failed to read body", http.StatusBadRequest)
		return
	}

	err = d.head.Append(index, data)

	switch err {
	case statemate.ErrIndexGapsAreNotAllowed, statemate.ErrIndexMustBeIncreasing:
		log.Error("failed to append", "error", err)
		http.Error(w, fmt.Sprintf("failed to append: %v", err), http.StatusBadRequest)
	default:
		log.Error("failed to append", "error", err)
		http.Error(w, "failed to append", http.StatusInternalServerError)
	case nil:
		w.WriteHeader(http.StatusNoContent)
	}
}
