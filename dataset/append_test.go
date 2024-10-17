package dataset_test

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/draganm/linear/dataset"
	"github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/require"
)

func TestAppend(t *testing.T) {
	t.Parallel()
	withDataset(t, func(ctx context.Context, ds *dataset.Dataset) {
		r := http.NewServeMux()

		r.HandleFunc("GET /dataset", ds.GetInfo)
		r.HandleFunc("PUT /dataset/{index}", ds.Append)

		s := httptest.NewServer(r)
		defer s.Close()

		res, err := resty.New().R().SetBody([]byte{1, 2, 3}).Put(s.URL + "/dataset/0")
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, res.StatusCode())

		var info dataset.DatasetInfo

		res, err = resty.New().R().SetResult(&info).Get(s.URL + "/dataset")
		require.NoError(t, err)

		require.Equal(t, http.StatusOK, res.StatusCode())
		require.Equal(t,
			dataset.DatasetInfo{
				Name: "test-dataset",
				Config: dataset.DatasetConfig{
					MaxArchiveSize: 100,
					MaxArchiveTime: 24 * time.Hour,
				},
				FirstIndex:   0,
				LastIndex:    0,
				StorageBytes: 27,
			},
			info,
		)
	})
}
