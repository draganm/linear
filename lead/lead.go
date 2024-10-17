package lead

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type S3 struct {
	Endpoint        string
	AccessKeyID     string
	SecretAccessKey string
	Region          string
	Bucket          string
}

type Config struct {
	S3       S3
	StateDir string
}

type Lead struct {
	http.Handler
	s3Client *s3.Client
}

type datasetInfo struct {
	Config       DatasetConfig `json:"config"`
	FirstIndex   uint64        `json:"first_index"`
	LastIndex    uint64        `json:"last_index"`
	StorageBytes uint64        `json:"bytes"`
}

type DatasetConfig struct {
	Name           string `json:"name"`
	MaxArchiveSize uint64 `json:"max_archive_size"`
	MaxArchiveTime uint64 `json:"max_archive_time"`
}

func New(
	ctx context.Context,
	log *slog.Logger,
	cfg Config,
) (*Lead, error) {
	r := http.NewServeMux()

	awsConfig, err := config.LoadDefaultConfig(
		ctx,
		config.WithRegion(cfg.S3.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.S3.AccessKeyID, cfg.S3.SecretAccessKey, "")),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to load AWS config: %w", err)
	}

	s3Client := s3.NewFromConfig(awsConfig, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.S3.Endpoint)
		o.UsePathStyle = true
		o.EndpointOptions.DisableHTTPS = true
	})

	l := &Lead{
		Handler:  r,
		s3Client: s3Client,
	}

	r.HandleFunc("PUT /api/append/{dataset}", l.Create)
	r.HandleFunc("POST /api/append/{dataset}", l.AppendSingle)
	r.HandleFunc("GET /api/append/{dataset}", l.AppendSingle)

	return l, nil
}
