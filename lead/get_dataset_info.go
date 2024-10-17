package lead

import "net/http"

func (l *Lead) GetDatasetInfo(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}
