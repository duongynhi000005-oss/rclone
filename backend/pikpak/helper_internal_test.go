package pikpak

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"sync/atomic"
	"testing"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/lib/pacer"
	"github.com/rclone/rclone/lib/pool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReadGcid checks readGcid hashes and replays its input from a pooled
// buffer for small known sizes, and from a temp file for large or unknown
// sizes, releasing the buffer on cleanup.
func TestReadGcid(t *testing.T) {
	content := bytes.Repeat([]byte("pikpak"), 1024)
	want, err := calcGcid(bytes.NewReader(content), int64(len(content)))
	require.NoError(t, err)

	for _, test := range []struct {
		name      string
		size      int64
		threshold int64
		wantFile  bool
	}{
		{"memory", int64(len(content)), int64(len(content)), false},
		{"file", int64(len(content)), int64(len(content)) - 1, true},
		{"unknown size", -1, int64(len(content)), true},
	} {
		t.Run(test.name, func(t *testing.T) {
			inUse := pool.Global().InUse()
			gcid, out, cleanup, err := readGcid(bytes.NewReader(content), test.size, test.threshold)
			require.NoError(t, err)
			assert.Equal(t, want, gcid)
			_, isFile := out.(*os.File)
			assert.Equal(t, test.wantFile, isFile, "expected temp file %v", test.wantFile)
			got, err := io.ReadAll(out)
			require.NoError(t, err)
			assert.Equal(t, content, got)
			cleanup()
			assert.Equal(t, inUse, pool.Global().InUse(), "buffer should be returned to the pool")
		})
	}
}

func TestOpenHTTPStatusHandling(t *testing.T) {
	ctx := context.Background()

	t.Run("retries 502 and never returns HTML error body", func(t *testing.T) {
		var calls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if calls.Add(1) == 1 {
				w.Header().Set("Content-Type", "text/html")
				w.WriteHeader(http.StatusBadGateway)
				_, _ = w.Write([]byte("<html><head><title>502 Bad Gateway</title></head></html>"))
				return
			}
			assert.Equal(t, "bytes=0-10", r.Header.Get("Range"))
			w.Header().Set("Content-Range", "bytes 0-10/11")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = w.Write([]byte("hello world"))
		}))
		defer srv.Close()

		f := &Fs{
			client: srv.Client(),
			pacer:  fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(0), pacer.MaxSleep(0))),
		}
		f.pacer.SetRetries(2)
		o := &Object{
			fs:   f,
			size: 11,
		}

		rc, err := o.open(ctx, srv.URL, &fs.RangeOption{Start: 0, End: 10})
		require.NoError(t, err)
		defer func() { _ = rc.Close() }()

		got, err := io.ReadAll(rc)
		require.NoError(t, err)
		assert.Equal(t, "hello world", string(got))
		assert.Equal(t, int32(2), calls.Load())
	})

	t.Run("persistent 502 returns error instead of error body", func(t *testing.T) {
		var calls atomic.Int32
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			calls.Add(1)
			w.Header().Set("Content-Type", "text/html")
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("<html><head><title>502 Bad Gateway</title></head></html>"))
		}))
		defer srv.Close()

		f := &Fs{
			client: srv.Client(),
			pacer:  fs.NewPacer(ctx, pacer.NewDefault(pacer.MinSleep(0), pacer.MaxSleep(0))),
		}
		f.pacer.SetRetries(2)
		o := &Object{
			fs:   f,
			size: 11,
		}

		rc, err := o.open(ctx, srv.URL, &fs.RangeOption{Start: 0, End: 10})
		require.Error(t, err)
		assert.Nil(t, rc)
		assert.Contains(t, err.Error(), "502 Bad Gateway")
		assert.Equal(t, int32(2), calls.Load())
	})
}
