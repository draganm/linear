package lead_test

import (
	"context"
	"log/slog"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/draganm/linear/lead"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/minio"
)

func TestLead(t *testing.T) {

	ctx := context.Background()

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

	leadConfig := lead.Config{
		S3: lead.S3{
			Endpoint:        endpoint,
			AccessKeyID:     accessKeyID,
			SecretAccessKey: secretAccessKey,
			Region:          region,
			Bucket:          "test-bucket",
		},
		StateDir: "state",
	}

	ld, err := lead.New(
		ctx,
		slog.Default(),
		leadConfig,
	)

	require.NoError(t, err)

	s := httptest.NewServer(ld)

	defer s.Close()

}
