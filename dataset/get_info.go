package dataset

import (
	"encoding/json"
	"net/http"
)

type DatasetInfo struct {
	Name         string        `json:"name"`
	Config       DatasetConfig `json:"config"`
	FirstIndex   uint64        `json:"first_index"`
	LastIndex    uint64        `json:"last_index"`
	StorageBytes uint64        `json:"bytes"`
}

func (d *Dataset) GetInfo(w http.ResponseWriter, r *http.Request) {

	i := DatasetInfo{
		Name:         d.name,
		Config:       d.config,
		FirstIndex:   d.head.GetFirstIndex(),
		LastIndex:    d.head.GetLastIndex(),
		StorageBytes: d.head.StorageStats().IndexSize + d.head.StorageStats().DataSize,
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(i)

}
