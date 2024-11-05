package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"cloud.google.com/go/storage"
	"github.com/fsnotify/fsnotify"
)

type Watcher struct {
	watcher *fsnotify.Watcher
	bucket  *storage.BucketHandle
	ctx     context.Context
}

func NewWatcher(bucket *storage.BucketHandle, ctx context.Context, path string) (*Watcher, error) {
	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create watcher: %v", err)
	}

	st, err := os.Lstat(path)
	if err != nil {
		w.Close()
		return nil, fmt.Errorf("failed to stat path: %v", err)
	}

	if !st.IsDir() {
		w.Close()
		return nil, fmt.Errorf("%q is not a directory", path)
	}

	if err = w.Add(path); err != nil {
		w.Close()
		return nil, fmt.Errorf("failed to add path to watcher: %v", err)
	}

	return &Watcher{watcher: w, bucket: bucket, ctx: ctx}, nil
}

func (w *Watcher) Close() error {
	return w.watcher.Close()
}

func (w *Watcher) Watch(stop <-chan struct{}) {
	for {
		select {
		case event, ok := <-w.watcher.Events:
			if !ok {
				log.Println("watcher event channel closed")
				return
			}
			if err := w.handleEventFile(event.Name); err != nil {
				log.Println(err)
			}
		case err, ok := <-w.watcher.Errors:
			if !ok {
				log.Println("watcher error channel closed")
				return
			}
			log.Printf("watcher error: %v\n", err)
		case <-stop:
			return
		}
	}
}

func (w *Watcher) handleEventFile(fileName string) error {
	f, err := os.Open(fileName)
	if err != nil {
		return fmt.Errorf("failed to open file %q: %v", fileName, err)
	}
	defer f.Close()

	ctx, cancel := context.WithTimeout(w.ctx, time.Second*50)
	defer cancel()

	wc := w.bucket.Object(fileName).NewWriter(ctx)
	if wc == nil {
		return fmt.Errorf("failed to get object writer for %q", fileName)
	}

	if _, err = io.Copy(wc, f); err != nil {
		return fmt.Errorf("failed to copy file %q to bucket: %v", fileName, err)
	}

	if err = wc.Close(); err != nil {
		return fmt.Errorf("failed to close writer for %q: %v", fileName, err)
	}

	log.Printf("successfully uploaded file %q to bucket", fileName)

	return nil
}

func main() {
	var path string

	flag.StringVar(&path, "p", "", "the directory path to watch")
	flag.Parse()

	bucketName := os.Getenv("GCS_BUCKET")
	if bucketName == "" {
		log.Println("Missing GCS_BUCKET environment variable")
		os.Exit(1)
	}

	if path == "" {
		log.Println("Missing path to watch")
		flag.Usage()
		os.Exit(1)
	}

	ctx := context.Background()
	client, err := storage.NewClient(ctx)
	if err != nil {
		log.Fatalf("failed to create storage client: %v", err)
	}
	defer client.Close()

	bkt := client.Bucket(bucketName)
	if bkt == nil {
		log.Fatalf("failed to get bucket %q", bucketName)
	}

	w, err := NewWatcher(bkt, ctx, path)
	if err != nil {
		log.Fatalf("failed to create watcher: %v", err)
	}
	defer w.Close()

	stopCh := make(chan struct{})
	defer close(stopCh)

	go w.Watch(stopCh)

	<-stopCh
}
