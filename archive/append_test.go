package archive_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/draganm/linear/archive"
	"github.com/draganm/statemate"
	"github.com/neilotoole/slogt"
	"github.com/stretchr/testify/require"
)

func TestAppend(t *testing.T) {

	statemateDir := t.TempDir()
	blobDir := t.TempDir()

	sm, err := statemate.Open[uint64](filepath.Join(statemateDir, "statemate"), statemate.Options{})
	require.NoError(t, err)

	for i := uint64(0); i < 100; i++ {
		err = sm.Append(i, []byte{1, 2, 3})
		require.NoError(t, err)
	}

	ctx := context.Background()

	ar, err := archive.Open(
		ctx,
		slogt.New(t),
		archive.OpenOptions{
			LocalDir: blobDir,
		},
	)
	require.NoError(t, err)

	err = ar.Append(ctx, sm)
	require.NoError(t, err)

}
