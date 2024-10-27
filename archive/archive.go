package archive

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type OpenOptions struct {
	S3Client *s3.Client
	S3Bucket string
	Name     string
	LocalDir string
}

var blobRegexp = regexp.MustCompile(`^blob-(\d{20})-(\d{20})$`)

type Archive struct {
	s3Client *s3.Client
	s3Bucket string
	name     string
	localDir string
	blobDir  string
}

type archivedBlob struct {
	from uint64
	to   uint64
	key  string
	size uint64
}

func Open(
	ctx context.Context,
	log *slog.Logger,
	opts OpenOptions,
) (*Archive, error) {
	cl := opts.S3Client

	blobs := []archivedBlob{}
	var continuationToken *string

	prefix := path.Join(opts.Name, "blobs")

	for {

		res, err := cl.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket:            &opts.S3Bucket,
			Prefix:            aws.String(prefix),
			ContinuationToken: continuationToken,
		})

		if err != nil {
			return nil, err
		}

		for _, key := range res.Contents {
			name := path.Base(*key.Key)

			log.Info("blob", "key", *key.Key, "size", *key.Size, "name", name)

			m := blobRegexp.FindStringSubmatch(name)

			if m == nil {
				continue
			}

			from, err := strconv.ParseUint(m[1], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse blob from index %q: %w", m[1], err)
			}

			to, err := strconv.ParseUint(m[2], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse blob to index %q: %w", m[2], err)
			}

			blobs = append(blobs, archivedBlob{
				from: from,
				to:   to,
				key:  *key.Key,
				size: uint64(*key.Size),
			})

		}

		continuationToken = res.NextContinuationToken

		if continuationToken == nil {
			break
		}

	}

	slices.SortFunc(blobs, func(a, b archivedBlob) int {
		return int(a.from) - int(b.from)
	})

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
