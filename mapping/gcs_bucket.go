package mapping

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
	"google.golang.org/api/option"
)

type GcloudCreds struct {
	Type         string `json:"type"`
	ProjectID    string `json:"project_id"`
	PrivateKeyID string `json:"private_key_id"`
	PrivateKey   string `json:"private_key"`
	ClientEmail  string `json:"client_email"`
	ClientID     string `json:"client_id"`
	AuthURI      string `json:"auth_uri"`
	TokenURI     string `json:"token_uri"`
	AuthProvider string `json:"auth_provider_x509_cert_url"`
	CertURL      string `json:"client_x509_cert_url"`
}

type gcsBucketGetter struct {
	objectGetter interface {
		Object(name string) *storage.ObjectHandle
	}
}

func NewGCSBucketGetter(gcreds *GcloudCreds, bucketName string) (*gcsBucketGetter, *storage.Client, error) {
	credsB, err := json.Marshal(gcreds)
	if err != nil {
		return nil, nil, errors.Wrap(err, "json marshal")
	}

	client, err := storage.NewClient(context.Background(), option.WithCredentialsJSON(credsB))
	if err != nil {
		return nil, nil, errors.Wrap(err, "gcs storage client")
	}
	return &gcsBucketGetter{objectGetter: client.Bucket(bucketName)}, client, nil
}

func NewGCSBucketGetterFromEnvironment(bucketName string) (*gcsBucketGetter, *storage.Client, error) {
	client, err := storage.NewClient(context.Background())
	if err != nil {
		return nil, nil, errors.Wrap(err, "gcs storage client")
	}
	return &gcsBucketGetter{objectGetter: client.Bucket(bucketName)}, client, nil
}

func NewGCSBucketGetterWithClient(client *storage.Client, bucketName string) (*gcsBucketGetter, error) {
	return &gcsBucketGetter{objectGetter: client.Bucket(bucketName)}, nil
}

// GetStorageWriter returns the storage writer for google cloud storage.
func (r *gcsBucketGetter) GetStorageWriter(ctx context.Context, fileName string) io.WriteCloser {
	return r.objectGetter.Object(fileName).NewWriter(ctx)
}

// GetStorageReader returns the storage reader for google cloud storage.
func (r *gcsBucketGetter) GetStorageReader(ctx context.Context, fileName string) (io.ReadCloser, error) {
	return r.objectGetter.Object(fileName).NewReader(ctx)
}

func getMappingFilename(eventFamily string, version string, environment string) string {
	// event_family/v1.0.0.json
	return fmt.Sprintf("%s/%s/%s.json", eventFamily, environment, version)
}
