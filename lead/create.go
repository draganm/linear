package lead

import (
	"net/http"
	"time"
)

type CreateRequest struct {
	MaxArchiveSize uint64        `json:"max_archive_size"`
	MaxArchiveTime time.Duration `json:"max_archive_time"`
}

func (l *Lead) Create(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}
