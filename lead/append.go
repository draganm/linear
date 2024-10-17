package lead

import "net/http"

func (l *Lead) AppendSingle(w http.ResponseWriter, r *http.Request) {
	w.WriteHeader(http.StatusOK)
}
