package dataset

import (
	"encoding/binary"
	"fmt"
	"io"
	"net/http"
)

func (d *Dataset) AppendMulti(w http.ResponseWriter, r *http.Request) {
	log := d.log.With("method", r.Method, "path", r.URL.Path)

	for {
		var index uint64
		err := binary.Read(r.Body, binary.BigEndian, &index)

		if err == io.EOF {
			break
		}

		if err != nil {
			log.Error("failed to read index", "error", err)
			http.Error(w, "failed to read index", http.StatusBadRequest)
			return
		}

		var length uint64
		err = binary.Read(r.Body, binary.BigEndian, &length)
		if err != nil {
			log.Error("failed to read length", "error", err)
			http.Error(w, "failed to read length", http.StatusBadRequest)
			return
		}

		data := make([]byte, length)
		_, err = io.ReadFull(r.Body, data)

		err = d.head.Append(index, data)
		if err != nil {
			log.Error("failed to append", "error", err)
			http.Error(w, fmt.Sprintf("failed to append: %v", err), http.StatusBadRequest)
			return
		}
	}

	w.WriteHeader(http.StatusNoContent)

}
