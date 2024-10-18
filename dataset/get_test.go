package dataset_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/draganm/linear/dataset"
	"github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/require"
)

func TestGet(t *testing.T) {
	t.Parallel()
	withDataset(t, func(ctx context.Context, ds *dataset.Dataset) {
		r := http.NewServeMux()

		r.HandleFunc("GET /dataset/{index}", ds.Get)
		r.HandleFunc("PUT /dataset/{index}", ds.Append)

		s := httptest.NewServer(r)
		defer s.Close()

		res, err := resty.New().R().SetBody([]byte{1, 2, 3}).Put(s.URL + "/dataset/0")
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, res.StatusCode())

		res, err = resty.New().R().Get(s.URL + "/dataset/0")
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode())
		require.Equal(t, []byte{1, 2, 3}, res.Body())

	})
}
