package archive_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/draganm/blobmap"
	"github.com/draganm/linear/archive"
	"github.com/draganm/linear/e2eutils"
	"github.com/draganm/statemate"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/require"
)

func TestAppend(t *testing.T) {

	e2eutils.WithMinioContainer(t, func(ctx context.Context, s3Client *s3.Client, bucketName string) {

		statemateDir := t.TempDir()
		blobDir := t.TempDir()

		sm, err := statemate.Open[uint64](filepath.Join(statemateDir, "statemate"), statemate.Options{})
		require.NoError(t, err)

		for i := uint64(0); i < 100; i++ {
			err = sm.Append(i, []byte{1, 2, byte(i)})
			require.NoError(t, err)
		}

		log := slogt.New(t)
		ar, err := archive.Open(
			ctx,
			log,
			archive.OpenOptions{
				S3Client: s3Client,
				S3Bucket: bucketName,
				Name:     "test-archive",
				LocalDir: blobDir,
			},
		)
		require.NoError(t, err)

		err = ar.Append(ctx, sm)
		require.NoError(t, err)

		blobs := filepath.Join(blobDir, "blobs")

		files, err := os.ReadDir(blobs)
		require.NoError(t, err)

		require.Len(t, files, 1)

		log.Info("blob", "file", files[0].Name())

		bm, err := blobmap.Open(filepath.Join(blobs, files[0].Name()))
		require.NoError(t, err)

		require.Equal(t, uint64(100), bm.LastKey()-bm.FirstKey()+1)

		{
			d, err := bm.Read(0)
			require.NoError(t, err)

			require.Equal(t, []byte{1, 2, 0}, d)
		}

		{
			d, err := bm.Read(99)
			require.NoError(t, err)

			require.Equal(t, []byte{1, 2, 99}, d)
		}

		keys, err := s3Client.ListObjectsV2(
			ctx,
			&s3.ListObjectsV2Input{
				Bucket:  &bucketName,
				Prefix:  aws.String("test-archive"),
				MaxKeys: aws.Int32(1000),
			},
		)
		require.NoError(t, err)

		require.Len(t, keys.Contents, 1)

		_, err = archive.Open(
			ctx,
			log,
			archive.OpenOptions{
				S3Client: s3Client,
				S3Bucket: bucketName,
				Name:     "test-archive",
				LocalDir: blobDir,
			},
		)
		require.NoError(t, err)

	})
}
