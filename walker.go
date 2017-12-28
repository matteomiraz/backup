package backup

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/pkg/errors"
)

const (
	// How much to read to compute the sha256 checksum
	dataToRead = 128000
)

func Walk(db *DB, cloud *Cloud, root string, skipDirs []string, skipFiles []string, threads int) (map[uint64]bool, error) {
	skipDirsMap := make(map[string]bool)
	for _, d := range skipDirs {
		skipDirsMap[d] = true
	}
	w := &walker{
		db:        db,
		cloud:     cloud,
		root:      filepath.Clean(root),
		skipDirs:  skipDirsMap,
		skipFiles: skipFiles,
		wc:        make(chan work, 2*threads), // work channel
		dc:        make(chan bool),            // done channel
		touched:   make(map[uint64]bool),      // file observed
	}

	for i := 0; i < threads; i++ {
		go w.worker()
	}

	fmt.Printf("Walking into %s\n", w.root)
	err := filepath.Walk(w.root, w.visit)
	close(w.wc)
	for i := 0; i < threads; i++ {
		<-w.dc
	}
	if err != nil {
		return w.touched, errors.Wrap(err, "cannot walk through files")
	}
	return w.touched, nil
}

type walker struct {
	db        *DB
	cloud     *Cloud
	root      string
	skipDirs  map[string]bool
	skipFiles []string
	mTouched  sync.Mutex // Mutex on touched
	touched   map[uint64]bool
	wc        chan work
	dc        chan bool
}

type work struct {
	path string
	size int64
}

func (i *walker) visit(fp string, fi os.FileInfo, err error) error {
	if err != nil {
		return err
	}

	if fi.IsDir() {
		if i.skipDirs[fi.Name()] {
			return filepath.SkipDir
		}
		return nil
	}

	for _, f := range i.skipFiles {
		if strings.HasSuffix(fi.Name(), f) {
			return nil
		}
	}

	size := fi.Size() // int64
	if size <= 0 {
		fmt.Printf("File '%s' is empty.\n", fp)
		return nil
	}

	i.wc <- work{path: fp, size: size}
	return nil
}

func (i *walker) worker() {
	for f := range i.wc {
		err := i.work(f.path, f.size)
		if err != nil {
			fmt.Printf("Cannot process file '%s': %v\n", f.path, err)
		}
	}
	i.dc <- true
}

func (i *walker) work(fp string, size int64) error {
	p, err := filepath.Rel(i.root, fp)
	if err != nil {
		return errors.Wrapf(err, "cannot compute relative path of '%s'", fp)
	}
	sum, err := computeChecksum(fp, size)
	if err != nil {
		return errors.Wrapf(err, "cannot compute checksum of '%s'", fp)
	}

	added, id, dbpath, err := i.db.Add(p, size, sum)
	if err != nil {
		return errors.Wrap(err, "cannot add entry to database")
	}
	i.mTouched.Lock()
	wasTouched := i.touched[id]
	i.touched[id] = true
	i.mTouched.Unlock()
	if added && !wasTouched {
		cloudPath, md5, err := i.cloud.Upload(context.Background(), fp, p, size, sum)
		if err != nil {
			return errors.Wrapf(err, "cannot upload '%s' to cloud storage", fp)
		}
		i.db.Update(size, sum, func(e *Entry) error {
			e.Cloud = cloudPath
			e.MD5 = md5
			return nil
		})
		fmt.Printf("Uploaded '%s'\n", p)
	} else {
		if wasTouched {
			// duplicate file
			fmt.Printf("File %s is a likely a duplicate of %s\n", fp, filepath.Join(i.root, dbpath))
		} else {
			if p != dbpath {
				fmt.Printf("File %s has been renamed to %s\n", filepath.Join(i.root, dbpath), p)
				i.db.Update(size, sum, func(e *Entry) error {
					e.Path = p
					// Cloud Path stays the same
					return nil
				})
			}
		}
	}

	return nil
}

func computeChecksum(path string, size int64) ([32]byte, error) {
	var eb [32]byte

	f, err := os.Open(path)
	defer f.Close()
	if err != nil {
		return eb, errors.Wrapf(err, "cannot open file '%s'", path)
	}

	var b []byte
	if size < dataToRead {
		b = make([]byte, size)
		if _, err = f.Read(b); err != nil {
			return eb, errors.Wrapf(err, "cannot read from file '%s'", path)
		}
	} else {
		// read from the middle of the file
		if _, err := f.Seek((size-dataToRead)/2, 0); err != nil {
			return eb, errors.Wrapf(err, "cannot seek in file '%s'", path)
		}
		b = make([]byte, dataToRead)
		if _, err = f.Read(b); err != nil {
			return eb, errors.Wrapf(err, "cannot read from file '%s'", path)
		}
	}
	return sha256.Sum256(b), nil
}
