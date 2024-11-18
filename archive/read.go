package archive

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/draganm/blobmap"
)

type readPlanStep struct {
	from       uint64
	count      uint64
	blobmapKey string
}

func (a *Archive) Read(
	ctx context.Context,
	from, count uint64,
	readFn func(ctx context.Context, index uint64, data []byte) error,
) error {
	a.readLock.RLock()

	readPlan := []readPlanStep{}

	for _, bm := range a.archivedBlobMaps {
		if from >= bm.from && from <= bm.to {
			if from+count-1 <= bm.to {
				readPlan = append(readPlan, readPlanStep{
					from:       from,
					count:      count,
					blobmapKey: bm.key,
				})
				break
			}
			readPlan = append(readPlan, readPlanStep{
				from:       from,
				count:      bm.to - from + 1,
				blobmapKey: bm.key,
			})
			from = bm.to + 1

		}
	}

	a.readLock.RUnlock()

	for _, step := range readPlan {

		err := a.blobMapsCache.WithBlobmap(
			ctx,
			step.blobmapKey,
			func(ctx context.Context, path string) error {
				res, err := a.s3Client.GetObject(
					ctx,
					&s3.GetObjectInput{
						Bucket: aws.String(a.s3Bucket),
						Key:    aws.String(step.blobmapKey),
					},
				)
				if err != nil {
					return fmt.Errorf("failed to get blobmap: %w", err)
				}
				defer res.Body.Close()

				w, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
				if err != nil {
					return fmt.Errorf("failed to open blobmap file: %w", err)
				}
				defer w.Close()

				_, err = io.Copy(w, res.Body)
				if err != nil {
					return fmt.Errorf("failed to write blobmap file: %w", err)
				}

				return nil
			},
			func(ctx context.Context, b *blobmap.Reader) error {
				for i := step.from; i < step.from+step.count; i++ {
					data, err := b.Read(i)
					if err != nil {
						return fmt.Errorf("failed to read data %d from blobmap %s: %w", i, step.blobmapKey, err)
					}

					err = readFn(ctx, i, data)
					if err != nil {
						return fmt.Errorf("failed to process data %d from blobmap %s: %w", i, step.blobmapKey, err)
					}
				}

				return nil
			},
		)
		if err != nil {
			return err
		}

	}

	return nil

}
