package dataset

import (
	"net/http"
	"strconv"

	"github.com/draganm/statemate"
)

func (d *Dataset) Get(w http.ResponseWriter, r *http.Request) {
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

	err = d.head.Read(index, func(data []byte) error {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, err := w.Write(data)
		return err
	})

	if err == statemate.ErrNotFound {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
}
