package syncer

import (
	"archive/tar"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"

	"cloud.google.com/go/storage"
	"github.com/fsnotify/fsnotify"
	"github.com/gerryhuynh/monitor-below-gcs/pkg/config"
)

type Syncer struct {
	ctx             context.Context
	config          config.Config
	bucket          *storage.BucketHandle
	configUpdatesCh <-chan fsnotify.Event
}

func New(ctx context.Context, config config.Config, bucket *storage.BucketHandle, configUpdatesCh <-chan fsnotify.Event) *Syncer {
	return &Syncer{ctx: ctx, config: config, bucket: bucket, configUpdatesCh: configUpdatesCh}
}

func (s *Syncer) Start() error {
	for {
		select {
		case <-s.configUpdatesCh:
			if err := s.syncBelowLogDir(); err != nil {
				return fmt.Errorf("error syncing below log dir: %v", err)
			}
		case <-s.ctx.Done():
			fmt.Println("closing syncer")
			return nil
		}
	}
}

func (s *Syncer) syncBelowLogDir() error {
	tarGzReader, err := s.tarBelowLogDir()
	if err != nil {
		return fmt.Errorf("error creating tar.gz of below log dir: %v", err)
	}
	defer tarGzReader.Close()

	ctx, cancel := context.WithTimeout(s.ctx, s.config.ContextTimeout)
	defer cancel()

	objectName := fmt.Sprintf("below_%s.tar.gz", s.config.CurrentNode)
	wc := s.bucket.Object(objectName).NewWriter(ctx)
	if wc == nil {
		return fmt.Errorf("failed to get object writer for %q", objectName)
	}

	if _, err = io.Copy(wc, tarGzReader); err != nil {
		return fmt.Errorf("failed to copy tar.gz to bucket: %v", err)
	}

	if err = wc.Close(); err != nil {
		return fmt.Errorf("failed to close writer for %q: %v", objectName, err)
	}

	log.Printf("successfully uploaded tar.gz file %q to bucket", objectName)

	return nil
}

func (s *Syncer) tarBelowLogDir() (io.ReadCloser, error) {
	dir := s.config.BelowLogDir

	dirInfo, err := os.Stat(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("directory %q does not exist", dir)
		}
		return nil, fmt.Errorf("error accessing directory %q: %v", dir, err)
	}
	if !dirInfo.IsDir() {
		return nil, fmt.Errorf("%q is not a directory", dir)
	}

	var files []string
	err = filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error creating file snapshot: %v", err)
	}

	pr, pw := io.Pipe()
	go func() {
		gw := gzip.NewWriter(pw)
		tw := tar.NewWriter(gw)

		defer func() {
			tw.Close()
			gw.Close()
			pw.Close()
		}()

		for _, path := range files {
			info, err := os.Stat(path)
			if err != nil {
				pw.CloseWithError(fmt.Errorf("error accessing file %q: %v", path, err))
				return
			}

			header, err := tar.FileInfoHeader(info, info.Name())
			if err != nil {
				pw.CloseWithError(fmt.Errorf("error creating header for %q: %v", path, err))
				return
			}

			relPath, err := filepath.Rel(dir, path)
			if err != nil {
				pw.CloseWithError(fmt.Errorf("error getting relative path for %q: %v", path, err))
				return
			}
			header.Name = filepath.ToSlash(relPath)

			if err := tw.WriteHeader(header); err != nil {
				pw.CloseWithError(fmt.Errorf("error writing header for %q: %v", path, err))
				return
			}

			if !info.IsDir() {
				file, err := os.Open(path)
				if err != nil {
					pw.CloseWithError(fmt.Errorf("error opening file %q: %v", path, err))
					return
				}
				if _, err := io.Copy(tw, file); err != nil {
					file.Close()
					pw.CloseWithError(fmt.Errorf("error copying file %q: %v", path, err))
					return
				}
				file.Close()
			}
		}
	}()

	return pr, nil
}
