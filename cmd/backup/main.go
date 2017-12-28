package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"

	"github.com/matteomiraz/backup"
)

var (
	dbPath     = flag.String("db", "", "Path to the database.")
	backupName = flag.String("name", "", "Name of the backup.")
	dir        = flag.String("dir", "", "Path to the files to backup.")
	threads    = flag.Int("threads", 20, "How many threads to use.")
	projectID  = flag.String("projectID", "", "Project ID of your Google Cloud.")
	bucket     = flag.String("bucket", "", "Name of the bucket to use.")
	skipDirs   = flag.String("skipDir", "@eaDir,.thumbcache", "Directories to skip (comma separated).")
	skipFiles  = flag.String("skipFile", ".dttags,.ini,thumbs.db,.DS_Store", "Skip files with this extension (or full name).")
)

func main() {
	flag.Parse()

	// Validate flags
	if *dbPath == "" {
		log.Fatal("Specify where to keep the database with --db=/path/to/db")
	}
	if *backupName == "" {
		log.Fatal("Specify the name for this backup with --name=myName")
	}
	if *dir == "" {
		log.Fatal("Specify which dir to backup with --dir=/path/to/dir")
	}
	if *threads <= 0 {
		log.Fatal("We need at least 1 thread to backup your data.")
	}
	if *projectID == "" {
		log.Fatal("Specify the Google cloud project to use with --projectID=myUniqueCloudProject")
	}
	if *bucket == "" {
		log.Fatal("Specify the name of the bucket with --bucket=myBucket")
	}

	fmt.Printf("Database in %s\n", *dbPath)
	db, err := backup.CreateDB(*dbPath, *backupName)
	if err != nil {
		log.Fatalf("Cannot create DB: %v", err)
	}
	defer db.Close()

	ctx := context.Background()
	cloud, err := backup.CreateCloud(ctx, *projectID, *bucket, *backupName)
	if err != nil {
		log.Fatalf("Cannot connect to Google Cloud: %v", err)
	}

	touched, err := backup.Walk(db, cloud, *dir, strings.Split(*skipDirs, ","), strings.Split(*skipFiles, ","), *threads)
	if err != nil {
		log.Fatalf("Cannot walk '%s': %v", *dir, err)
	}
	db.CheckMissing(touched)
}
