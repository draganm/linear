package dataset_test

import (
	"context"
	"net/http"
	"testing"

	"github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/require"
)

func TestGet(t *testing.T) {
	t.Parallel()
	withDataset(t, func(ctx context.Context, url string) {

		res, err := resty.New().R().SetBody([]byte{1, 2, 3}).Put(url + "/dataset/0")
		require.NoError(t, err)
		require.Equal(t, http.StatusNoContent, res.StatusCode())

		res, err = resty.New().R().Get(url + "/dataset/0")
		require.NoError(t, err)
		require.Equal(t, http.StatusOK, res.StatusCode())
		require.Equal(t, []byte{1, 2, 3}, res.Body())

	})
}
