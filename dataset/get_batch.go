package dataset

import (
	"encoding/binary"
	"fmt"
	"net/http"
	"strconv"

	"github.com/draganm/statemate"
)

func (d *Dataset) GetBatch(w http.ResponseWriter, r *http.Request) {
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

	countString := r.PathValue("count")

	if countString == "" {
		log.Error("count not provided")
		http.Error(w, "count not provided", http.StatusBadRequest)
		return
	}

	count, err := strconv.ParseUint(countString, 10, 64)
	if err != nil {
		log.Error("failed to parse count", "error", err)
		http.Error(w, "invalid count", http.StatusBadRequest)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")

	for i := index; i < index+count; i++ {

		err = d.head.Read(i, func(data []byte) error {
			err := binary.Write(w, binary.BigEndian, uint64(i))
			if err != nil {
				return fmt.Errorf("failed to write index: %w", err)
			}
			size := uint64(len(data))

			err = binary.Write(w, binary.BigEndian, size)
			if err != nil {
				return fmt.Errorf("failed to write size: %w", err)
			}

			_, err = w.Write(data)
			if err != nil {
				return fmt.Errorf("failed to write data: %w", err)
			}

			return nil
		})

		if err != nil {
			log.Error("failed to access data", "error", err)
			http.Error(w, "failed to access data", http.StatusInternalServerError)
			return
		}
	}

	if err == statemate.ErrNotFound {
		http.Error(w, "not found", http.StatusNotFound)
		return
	}
}
