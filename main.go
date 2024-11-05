package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"reflect"

	"cloud.google.com/go/storage"
	"github.com/fsnotify/fsnotify"
)

func main() {
	var bucketName string
	var pathToWatch string

	flag.StringVar(&bucketName, "b", "", "the bucket name")
	flag.StringVar(&pathToWatch, "p", "", "the directory path to watch")
	flag.Parse()

	if bucketName == "" {
		log.Println("Missing bucket name")
		flag.Usage()
		os.Exit(1)
	}

	if pathToWatch == "" {
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

	err = printBucketAttrs(bkt, ctx)
	if err != nil {
		log.Fatal(err)
	}

	files, err := os.ReadDir(pathToWatch)
	if err != nil {
		log.Fatalf("failed to read directory %q: %v", pathToWatch, err)
	}

	if len(files) == 0 {
		log.Fatalf("no files found in directory %q", pathToWatch)
	}

	objName := files[0].Name()
	wc := bkt.Object(objName).NewWriter(ctx)
	if wc == nil {
		log.Fatalf("failed to get object writer for %q", objName)
	}

	f, err := os.Open(path.Join(pathToWatch, objName))
	if err != nil {
		log.Fatalf("failed to open file %q: %v", objName, err)
	}
	defer f.Close()

	if _, err = io.Copy(wc, f); err != nil {
		log.Fatalf("failed to copy file %q to bucket %q: %v", objName, bucketName, err)
	}
	if err = wc.Close(); err != nil {
		log.Fatalf("failed to close writer for %q: %v", objName, err)
	}

	log.Printf("successfully uploaded file %q to bucket %q", objName, bucketName)

	// w, err := fsnotify.NewWatcher()
	// if err != nil {
	// 	log.Fatalf("failed to create watcher: %v", err)
	// }
	// defer w.Close()

	// go watch(w)

	// err = addPathToWatch(w, pathToWatch)
	// if err != nil {
	// 	log.Fatal(err)
	// }

	// <-make(chan struct{})
}

func watch(w *fsnotify.Watcher) {
	for {
		select {
		case event, ok := <-w.Events:
			if !ok {
				return
			}
			log.Println("event:", event)
		case err, ok := <-w.Errors:
			if !ok {
				return
			}
			log.Println("error:", err)
		}
	}
}

func addPathToWatch(w *fsnotify.Watcher, p string) error {
	st, err := os.Lstat(p)
	if err != nil {
		return fmt.Errorf("failed to stat path: %v", err)
	}

	if !st.IsDir() {
		return fmt.Errorf("%q is not a directory", p)
	}

	err = w.Add(p)
	if err != nil {
		return fmt.Errorf("failed to add path to watcher: %v", err)
	}

	return nil
}

func printBucketAttrs(bkt *storage.BucketHandle, ctx context.Context) error {
	attrs, err := bkt.Attrs(ctx)
	if err != nil {
		return fmt.Errorf("failed to get bucket attrs: %v", err)
	}

	fmt.Println("Bucket Information:")
	v := reflect.ValueOf(attrs).Elem()
	fmt.Println("v")
	t := v.Type()
	for i := 0; i < v.NumField(); i++ {
		field := v.Field(i)
		fieldName := t.Field(i).Name
		fmt.Printf("\t%s: %v\n", fieldName, field.Interface())
	}

	return nil
}
