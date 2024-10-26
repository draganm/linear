package archive

import (
	"context"
	"fmt"
	"path/filepath"

	"github.com/draganm/blobmap"
	"github.com/draganm/statemate"
)

func (a *Archive) Append(ctx context.Context, sm *statemate.StateMate[uint64]) error {

	firstIndex := sm.GetFirstIndex()
	lastIndex := sm.GetLastIndex()

	blobFileName := filepath.Join(a.blobDir, fmt.Sprintf("blob-%018d-%018d", firstIndex, lastIndex))

	builder, err := blobmap.NewBuilder(
		blobFileName,
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

	return nil
}
