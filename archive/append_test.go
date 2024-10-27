package archive_test

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/draganm/blobmap"
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
		err = sm.Append(i, []byte{1, 2, byte(i)})
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

	blobs := filepath.Join(blobDir, "blobs")

	files, err := os.ReadDir(blobs)
	require.NoError(t, err)

	require.Len(t, files, 1)

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

}
