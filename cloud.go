package backup

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"cloud.google.com/go/storage"
	"github.com/pkg/errors"
)

const (
	metadataHashKey = "x-goog-meta-backup-hash"
)

// Create a Google Cloud client.
// Files will be saved into directory 'dir' in the specified projectID / bucket (the bucket will be created if needed).
func CreateCloud(ctx context.Context, projectID, bucketName, dir string) (*Cloud, error) {
	client, err := storage.NewClient(ctx)
	if err != nil {
		return nil, errors.Wrapf(err, "failed to create the google cloud client")
	}

	bucket := client.Bucket(bucketName)
	if attrs, err := bucket.Attrs(ctx); err != nil {
		if err == storage.ErrBucketNotExist {
			if err := bucket.Create(ctx, projectID, &storage.BucketAttrs{Name: bucketName, StorageClass: "COLDLINE"}); err != nil {
				return nil, errors.Wrapf(err, "cannot create bucket '%s'", bucketName)
			}
		} else {
			return nil, errors.Wrapf(err, "cannot open bucket '%s'", bucketName)
		}
	} else {
		fmt.Printf("Using bucket %s located in %s with storage class %s\n", attrs.Name, attrs.Location, attrs.StorageClass)
	}

	return &Cloud{
		client: client,
		bucket: bucket,
		dir:    dir,
	}, nil
}

type Cloud struct {
	client *storage.Client
	bucket *storage.BucketHandle
	dir    string
}

// fp is the full path of the file; p is the logical path of the file (relative to the backup root)
func (i *Cloud) Upload(ctx context.Context, fp, p string, size int64, hash [32]byte) (cloud string, chksum []byte, err error) {
	cloud = ""
	chksum = make([]byte, 0)
	in, err := os.Open(fp)
	if err != nil {
		return
	}
	defer in.Close()

	cloud = filepath.Join(i.dir, p)
	object := i.bucket.Object(cloud)

	// check if the file exists on the cloud
	if attrs, aErr := object.Attrs(ctx); aErr != storage.ErrObjectNotExist {
		if aErr != nil {
			err = errors.Wrapf(aErr, "cannot check cloud file '%s'", cloud)
			return
		}
		// found the same file on the cloud
		if attrs.Size == size && attrs.Metadata[metadataHashKey] == encode(hash) {
			chksum = attrs.MD5
			return
		}
		err = fmt.Errorf("cloud file '%s' already exists, but it's different: %d vs %d, %s vs %s", cloud, size, attrs.Size, encode(hash), attrs.Metadata[metadataHashKey])
		return
	}

	w := object.NewWriter(ctx)
	w.ChunkSize = 1024 * 1024 // 1 Mbyte
	_, err = io.Copy(w, in)
	if err != nil {
		return
	}
	w.Close()

	attrs, aErr := object.Update(ctx, storage.ObjectAttrsToUpdate{
		Metadata: map[string]string{
			metadataHashKey: encode(hash),
		},
	})
	if aErr != nil {
		err = errors.Wrapf(aErr, "cannot update attributes")
		return
	}
	chksum = attrs.MD5

	return
}

func encode(hash [32]byte) string {
	return fmt.Sprintf("%X", hash)
}

// TODO: create a method to upload the db, as //	writer.ObjectAttrs =  STANDARD
