package mapping

import (
	"context"
	"encoding/json"
	"io"
	"time"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
)

type Writer struct {
	writerBucket func(ctx context.Context, fileName string) io.WriteCloser
}

func NewWriter(gcreds *GcloudCreds, bucketName string) (*Writer, *storage.Client, error) {
	gb, gbClient, err := newGCSBucketGetter(gcreds, bucketName)
	if err != nil {
		return nil, nil, errors.Wrap(err, "new gcs bucket")
	}
	return &Writer{
		writerBucket: gb.GetStorageWriter,
	}, gbClient, nil
}

func (w *Writer) uploadMappingFile(ctx context.Context, eventFamily, version string, mapping *Mapping) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()
	filename := getMappingFilename(eventFamily, version)
	writer := w.writerBucket(ctx, filename)
	encoder := json.NewEncoder(writer)
	err := encoder.Encode(mapping)
	if err != nil {
		return errors.Wrap(err, "encode mapping")
	}
	err = writer.Close()
	if err != nil {
		return errors.Wrap(err, "close uploaded gcp file")
	}
	return nil
}
