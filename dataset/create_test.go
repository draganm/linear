package dataset_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/draganm/linear/dataset"
	"github.com/draganm/linear/e2eutils"
	"github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/require"
)

func withDataset(t *testing.T, fn func(ctx context.Context, url string)) {

	e2eutils.WithMinioContainer(t, func(ctx context.Context, s3Client *s3.Client, bucketName string) {
		dataDir := t.TempDir()

		ds, err := dataset.Create(
			ctx,
			dataset.CreateOptions{
				Log:      slog.Default(),
				S3Client: s3Client,
				S3Bucket: "test-bucket",
				Config: dataset.DatasetConfig{
					MaxArchiveSize: 100,
					MaxArchiveTime: 24 * time.Hour,
				},
				Name:     "test-dataset",
				LocalDir: dataDir,
			},
		)

		require.NoError(t, err)

		defer ds.Close()

		r := http.NewServeMux()

		r.HandleFunc("GET /dataset", ds.GetInfo)
		r.HandleFunc("GET /dataset/{index}", ds.Get)
		r.HandleFunc("GET /dataset/{index}/{count}", ds.GetBatch)
		r.HandleFunc("PUT /dataset/{index}", ds.Append)
		r.HandleFunc("POST /dataset", ds.AppendMulti)

		s := httptest.NewServer(r)
		defer s.Close()

		fn(ctx, s.URL)
	})

}

func TestLead(t *testing.T) {
	t.Parallel()

	e2eutils.WithMinioContainer(t, func(ctx context.Context, s3Client *s3.Client, bucketName string) {
		dataDir := t.TempDir()

		ds, err := dataset.Create(
			ctx,
			dataset.CreateOptions{
				Log:      slog.Default(),
				S3Client: s3Client,
				S3Bucket: "test-bucket",
				Config: dataset.DatasetConfig{
					MaxArchiveSize: 100,
					MaxArchiveTime: 24 * time.Hour,
				},
				Name:     "test-dataset",
				LocalDir: dataDir,
			},
		)

		require.NoError(t, err)

		defer ds.Close()

		r := http.NewServeMux()

		r.HandleFunc("/dataset", ds.GetInfo)

		s := httptest.NewServer(r)
		defer s.Close()

		var info dataset.DatasetInfo

		res, err := resty.New().R().SetResult(&info).Get(s.URL + "/dataset")
		require.NoError(t, err)

		require.Equal(t, http.StatusOK, res.StatusCode())
		require.Equal(t,
			dataset.DatasetInfo{
				Name: "test-dataset",
				Config: dataset.DatasetConfig{
					MaxArchiveSize: 100,
					MaxArchiveTime: 24 * time.Hour,
				},
				FirstIndex:   0xffffffffffffffff,
				LastIndex:    0xffffffffffffffff,
				StorageBytes: 0x8,
			},
			info,
		)
	})
}
