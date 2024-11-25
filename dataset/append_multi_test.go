package dataset_test

import (
	"bytes"
	"context"
	"encoding/binary"
	"net/http"
	"testing"
	"time"

	"github.com/draganm/linear/dataset"
	"github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/require"
)

func TestAppendMulti(t *testing.T) {
	t.Parallel()
	withDataset(t, func(ctx context.Context, url string) {

		var buf bytes.Buffer
		err := binary.Write(&buf, binary.BigEndian, uint64(0))
		require.NoError(t, err)
		err = binary.Write(&buf, binary.BigEndian, uint64(3))
		require.NoError(t, err)
		_, err = buf.Write([]byte{1, 2, 3})
		require.NoError(t, err)

		res, err := resty.New().R().SetBody(buf.Bytes()).Post(url + "/dataset")
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
