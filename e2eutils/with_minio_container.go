package e2eutils

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go/modules/minio"
)

func WithMinioContainer(t *testing.T, fn func(ctx context.Context, s3Client *s3.Client, bucketName string)) {
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

	fn(ctx, s3Client, "test-bucket")

}
