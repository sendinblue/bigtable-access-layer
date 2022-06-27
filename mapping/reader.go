package mapping

import (
	"context"
	"io"
	"time"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
)

type Reader struct {
	readerBucket func(ctx context.Context, fileName string) (io.ReadCloser, error)
}

func NewReader(gcreds *GcloudCreds, bucketName string) (*Reader, *storage.Client, error) {
	gb, gbClient, err := newGCSBucketGetter(gcreds, bucketName)
	if err != nil {
		return nil, nil, errors.Wrap(err, "new gcs bucket")
	}
	return &Reader{
		readerBucket: gb.GetStorageReader,
	}, gbClient, nil
}

func (r *Reader) Load(ctx context.Context, eventFamily string, version string) (*Mapping, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()
	filename := getMappingFilename(eventFamily, version)
	reader, err := r.readerBucket(ctx, filename)
	if err != nil {
		return nil, errors.Wrap(err, "get storage reader")
	}
	m, err := LoadMappingIO(reader)
	err = reader.Close()
	if err != nil {
		return nil, errors.Wrap(err, "close reader")
	}
	return m, nil
}
