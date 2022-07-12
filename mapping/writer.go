package mapping

import (
	"context"
	"encoding/json"
	"io"
	"time"

	std_errors "errors"

	"cloud.google.com/go/storage"
	"github.com/davecgh/go-spew/spew"
	"github.com/pierrre/compare"
	"github.com/pkg/errors"
)

type Writer struct {
	writerBucket func(ctx context.Context, fileName string) io.WriteCloser
	readerLoad   func(ctx context.Context, eventFamily string, version string, environment string) (*Mapping, error)
}

func NewWriter(gcreds *GcloudCreds, bucketName string) (*Writer, *storage.Client, error) {
	gb, gbClient, err := NewGCSBucketGetter(gcreds, bucketName)
	if err != nil {
		return nil, nil, errors.Wrap(err, "new gcs bucket")
	}

	return &Writer{
		writerBucket: gb.GetStorageWriter,
		readerLoad:   newReaderFromGCSClient(gb.GetStorageReader).Load,
	}, gbClient, nil
}

func NewWriterFromGCSClient(gbSW func(ctx context.Context, fileName string) io.WriteCloser, gbSL func(ctx context.Context, fileName string) (io.ReadCloser, error)) (*Writer, error) {
	return &Writer{
		writerBucket: gbSW,
		readerLoad:   newReaderFromGCSClient(gbSL).Load,
	}, nil
}

func (w *Writer) Upload(ctx context.Context, eventFamily, version string, environment string, writeMapping *Mapping, forceUpload bool) error {
	ctx, cancel := context.WithTimeout(ctx, time.Second*50)
	defer cancel()
	filename := getMappingFilename(eventFamily, version, environment)
	//if force upload is false, we check for already existing mapping and return without overwriting
	if !forceUpload {
		readMapping, err := w.readerLoad(ctx, eventFamily, version, environment)
		if err != nil && UnwrapAll(err) != storage.ErrObjectNotExist {
			return errors.Wrap(err, "get storage reader")
		}
		diff := compare.Compare(readMapping, writeMapping)
		if readMapping != nil {
			return errors.Errorf("mapping already exists:\nread:\n%s\nwrite:\n%s\ndiff:\n%+v", spew.Sdump(readMapping), spew.Sdump(writeMapping), diff)
		}
	}

	// only if force upload is true or object does not exists
	writer := w.writerBucket(ctx, filename)
	encoder := json.NewEncoder(writer)
	err := encoder.Encode(writeMapping)
	if err != nil {
		return errors.Wrap(err, "encode mapping")
	}
	err = writer.Close()
	if err != nil {
		return errors.Wrap(err, "close uploaded gcp file")
	}
	return nil
}

// Unwrap calls std_errors.Unwrap.
func Unwrap(err error) error {
	return std_errors.Unwrap(err)
}

// UnwrapAll unwraps all nested errors, and returns the last one.
func UnwrapAll(err error) error {
	for {
		werr := Unwrap(err)
		if werr == nil {
			return err
		}
		err = werr
	}
}
