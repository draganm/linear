package dataset

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path"

	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type DatasetConfig struct {
	MaxArchiveSize uint64 `json:"max_archive_size"`
	MaxArchiveTime uint64 `json:"max_archive_time"`
}

type Dataset struct {
	config   DatasetConfig
	name     string
	localDir string
	s3Client *s3.Client
}

type OpenOptions struct {
	S3Client *s3.Client
	S3Bucket string
	Name     string
	LocalDir string
}

func Open(
	ctx context.Context,
	log *slog.Logger,
	opts OpenOptions,
) (*Dataset, error) {
	return nil, errors.New("not implemented")
}

type CreateOptions struct {
	Log      *slog.Logger
	S3Client *s3.Client
	S3Bucket string
	Config   DatasetConfig
	Name     string
	LocalDir string
}

func Create(
	ctx context.Context,
	opts CreateOptions,
) (*Dataset, error) {

	key := path.Join(opts.Name, "dataset.json")

	d, err := json.Marshal(opts.Config)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal dataset config: %w", err)
	}

	_, err = opts.S3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &opts.S3Bucket,
		Key:    &key,
		Body:   bytes.NewReader(d),
	})

	if err != nil {
		return nil, fmt.Errorf("failed to create dataset: %w", err)
	}

	// TODO: create statemate

	return &Dataset{
		config:   opts.Config,
		name:     opts.Name,
		localDir: opts.LocalDir,
		s3Client: opts.S3Client,
	}, nil

}
