package dataset_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/draganm/linear/dataset"
	"github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/require"
)

func TestAppend(t *testing.T) {
	t.Parallel()
	withDataset(t, func(ctx context.Context, url string) {

		res, err := resty.New().R().SetBody([]byte{1, 2, 3}).Put(url + "/dataset/0")
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, res.StatusCode())

		var info dataset.DatasetInfo

		res, err = resty.New().R().SetResult(&info).Get(url + "/dataset")
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
