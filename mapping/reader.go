package mapping

import (
	"context"
	"io"
	"time"

	"cloud.google.com/go/storage"
)

type Reader struct {
	readerBucket func(ctx context.Context, fileName string) (io.ReadCloser, error)
}

func NewReader(gcreds *GcloudCreds, bucketName string) (*Reader, *storage.Client, error) {
	gb, gbClient, err := NewGCSBucketGetter(gcreds, bucketName)
	if err != nil {
		return nil, nil, err
	}
	return &Reader{
		readerBucket: gb.GetStorageReader,
	}, gbClient, nil
}

func newReaderFromGCSClient(gbSL func(ctx context.Context, fileName string) (io.ReadCloser, error)) *Reader {
	return &Reader{
		readerBucket: gbSL,
	}
}

func (r *Reader) Load(ctx context.Context, eventFamily string, version string, environment string) (*Mapping, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()
	filename := getMappingFilename(eventFamily, version, environment)
	reader, err := r.readerBucket(ctx, filename)
	if err != nil {
		return nil, err
	}
	m, err := LoadMappingIO(reader)
	if err != nil {
		return nil, err
	}
	err = reader.Close()
	if err != nil {
		return nil, err
	}
	return m, nil
}
