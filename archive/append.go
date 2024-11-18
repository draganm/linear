package archive

import (
	"context"
	"fmt"
	"os"
	"path"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/draganm/blobmap"
	"github.com/draganm/statemate"
)

func (a *Archive) Append(ctx context.Context, sm *statemate.StateMate[uint64]) error {

	firstIndex := sm.GetFirstIndex()
	lastIndex := sm.GetLastIndex()

	blobFileName := fmt.Sprintf("blob-%020d-%020d", firstIndex, lastIndex)

	blobFilePath := filepath.Join(a.workDir, blobFileName)

	builder, err := blobmap.NewBuilder(
		blobFilePath,
		firstIndex,
		lastIndex-firstIndex+1,
	)
	if err != nil {
		return fmt.Errorf("failed to create blob builder: %w", err)
	}

	for i := firstIndex; i <= lastIndex; i++ {
		err = sm.Read(i, func(data []byte) error {
			return builder.Add(i, data)
		})
		if err != nil {
			return fmt.Errorf("failed to add data to blob: %w", err)
		}
	}

	err = builder.Build()
	if err != nil {
		return fmt.Errorf("failed to build blob: %w", err)
	}

	uploader := manager.NewUploader(
		a.s3Client,
	)

	key := path.Join(a.name, "blobs", blobFileName)

	f, err := os.Open(blobFilePath)
	if err != nil {
		return fmt.Errorf("failed to open blob file: %w", err)
	}
	defer f.Close()

	_, err = uploader.Upload(ctx, &s3.PutObjectInput{
		Bucket: &a.s3Bucket,
		Key:    &key,
		Body:   f,
	})
	if err != nil {
		return fmt.Errorf("failed to upload blob to s3: %w", err)
	}

	return nil
}
