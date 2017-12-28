package backup

import (
	"bytes"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"time"

	"github.com/boltdb/bolt"
	"github.com/pkg/errors"
)

func CreateDB(path string, name string) (*DB, error) {
	db, err := bolt.Open(path, 0600, &bolt.Options{Timeout: 1 * time.Second})
	if err != nil {
		return nil, errors.Wrapf(err, "cannot open db '%f'", path)
	}

	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(name))
		if err != nil {
			return errors.Wrap(err, "bucket creation failed")
		}
		return nil
	})

	return &DB{
		db:   db,
		name: name,
	}, nil
}

type DB struct {
	db   *bolt.DB
	name string
}

func computeKey(size int64, sha256 [32]byte) []byte {
	b := make([]byte, 40)
	binary.LittleEndian.PutUint64(b, uint64(size))
	for i, v := range sha256 {
		b[i+8] = v
	}
	return b
}

// Entry in the database.
// When the file is first observed, these fields are populated: ID, Path, Size, Hash.
// Then the file is uploaded to the cloud, and fields Cloud and MD5 are populated.
// Notice that only field Path can be updated afterwards (e.g., when the file is moved on disk).
type Entry struct {
	ID    uint64   // unique progressive id
	Path  string   // Path of the file on disk
	Cloud string   // Path of the file on cloud
	Size  int64    // Size in bytes of the file
	Hash  [32]byte // checksum of a small part of the file
	MD5   []byte   // checksum of the entire file
}

func (i Entry) encode() ([]byte, error) {
	b := bytes.Buffer{}
	e := gob.NewEncoder(&b)
	err := e.Encode(i)
	if err != nil {
		return make([]byte, 0), errors.Wrap(err, "cannot encode the entry")
	}
	return b.Bytes(), nil
}

func decodeEntry(b []byte) (*Entry, error) {
	buf := bytes.Buffer{}
	buf.Write(b)
	d := gob.NewDecoder(&buf)
	e := &Entry{}
	if err := d.Decode(e); err != nil {
		return nil, errors.Wrap(err, "failed to decode")
	}
	return e, nil
}

func encodeAndPut(bkt *bolt.Bucket, key []byte, e Entry) error {
	b, err := e.encode()
	if err != nil {
		return errors.Wrapf(err, "cannot encode db entry")
	}
	err = bkt.Put(key, b)
	if err != nil {
		return errors.Wrap(err, "cannot put the entry in the dababase")
	}
	return nil
}

type UpdateFn func(*Entry) error

func (i *DB) Update(size int64, sha256 [32]byte, fn UpdateFn) error {
	key := computeKey(size, sha256)
	return i.db.Update(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(i.name))
		v := bkt.Get(key)
		if v == nil {
			return fmt.Errorf("cannot find element with key %X", key)
		}
		e, err := decodeEntry(v)
		if err != nil {
			return errors.Wrapf(err, "cannot decode element with key %X", key)
		}
		err = fn(e)
		if err != nil {
			return errors.Wrapf(err, "cannot update entry with key %X", key)
		}
		return encodeAndPut(bkt, key, *e)
	})
}

func (i *DB) Add(path string, size int64, sha256 [32]byte) (added bool, id uint64, rPath string, rErr error) {
	key := computeKey(size, sha256)

	rErr = i.db.Update(func(tx *bolt.Tx) error {
		var err error
		bkt := tx.Bucket([]byte(i.name))

		// Check if the element is present
		if v := bkt.Get(key); v != nil {
			e, err := decodeEntry(v)
			if err != nil {
				return errors.Wrapf(err, "cannot decode element with key %X", key)
			}
			// if Cloud or MD5 are not set, the file was not successfully uploaded to the cloud.
			added = e.Cloud == "" || len(e.MD5) == 0
			id = e.ID
			rPath = e.Path
			return nil
		}

		added = true
		id, err = bkt.NextSequence()
		if err != nil {
			return errors.Wrap(err, "cannot obtain the next id")
		}
		return encodeAndPut(bkt, key, Entry{
			ID:   id,
			Path: path,
			Size: size,
			Hash: sha256,
		})
	})

	return
}

func (i *DB) CheckMissing(found map[uint64]bool) {
	i.db.View(func(tx *bolt.Tx) error {
		bkt := tx.Bucket([]byte(i.name))
		c := bkt.Cursor()
		for k, v := c.First(); k != nil; k, v = c.Next() {
			e, err := decodeEntry(v)
			if err != nil {
				fmt.Printf("ERROR parsing entry %X: %v", k, err)
			} else {
				if !found[e.ID] {
					fmt.Printf("Missing %X %s\n", k, e.Path)
				}
			}
		}
		return nil
	})
	return
}

func (i *DB) Close() {
	i.db.Close()
}
