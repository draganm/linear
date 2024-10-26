package archive

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type OpenOptions struct {
	S3Client *s3.Client
	S3Bucket string
	Name     string
	LocalDir string
}

type Archive struct {
	s3Client *s3.Client
	s3Bucket string
	name     string
	localDir string
	blobDir  string
}

func Open(
	ctx context.Context,
	log *slog.Logger,
	opts OpenOptions,
) (*Archive, error) {
	// cl := opts.S3Client
	// bucket := opts.S3Bucket
	// name := opts.Name

	// s3Client := opts.S3Client
	// res, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
	// 	Bucket:    &opts.S3Bucket,
	// 	Prefix:    &opts.Name,
	// 	Delimiter: aws.String("/"),
	// })

	blobdir := filepath.Join(opts.LocalDir, "blobs")

	err := os.MkdirAll(blobdir, 0755)
	if err != nil {
		return nil, err
	}

	return &Archive{
		s3Client: opts.S3Client,
		s3Bucket: opts.S3Bucket,
		name:     opts.Name,
		localDir: opts.LocalDir,
		blobDir:  blobdir,
	}, nil
}
