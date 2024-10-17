package dataset_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/draganm/linear/dataset"
	"github.com/go-resty/resty/v2"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/minio"
)

func TestLead(t *testing.T) {

	ctx := context.Background()

	dataDir := t.TempDir()

	minioContainer, err := minio.Run(ctx, "minio/minio:RELEASE.2024-01-16T16-07-38Z")
	require.NoError(t, err)
	defer func() {
		minioContainer.Stop(ctx, nil)
	}()

	ep, err := minioContainer.Endpoint(ctx, "")
	require.NoError(t, err)

	endpoint := "http://" + ep
	accessKeyID := "minioadmin"
	secretAccessKey := "minioadmin"
	region := "us-east-1"

	cfg, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion(region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, "")),
	)

	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(endpoint)
		o.UsePathStyle = true
		o.EndpointOptions.DisableHTTPS = true
	})

	_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
		Bucket: aws.String("test-bucket"),
	})
	require.NoError(t, err)

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
}
