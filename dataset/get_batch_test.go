package dataset_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/require"
)

func TestGetBatch(t *testing.T) {
	t.Parallel()
	withDataset(t, func(ctx context.Context, url string) {

		res, err := resty.New().R().SetBody([]byte{1, 2, 3}).Put(url + "/dataset/0")
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, res.StatusCode())

		res, err = resty.New().R().SetBody([]byte{4, 5, 6}).Put(url + "/dataset/1")
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, res.StatusCode())

		res, err = resty.New().R().Get(url + "/dataset/0/2")
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode())
		require.Equal(
			t, []byte{
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3,
				0x1, 0x2, 0x3,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1,
				0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3,
				0x4, 0x5, 0x6},
			res.Body())
	})
}
