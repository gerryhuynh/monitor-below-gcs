package syncer

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"cloud.google.com/go/storage"
	"github.com/gerryhuynh/monitor-below-gcs/pkg/bundler"
	"github.com/gerryhuynh/monitor-below-gcs/pkg/config"
	"github.com/gerryhuynh/monitor-below-gcs/pkg/watcher"
)

type Syncer struct {
	ctx             context.Context
	config          config.Config
	bucket          *storage.BucketHandle
	configUpdatesCh <-chan watcher.ConfigEvent
}

func New(ctx context.Context, config config.Config, bucket *storage.BucketHandle, configUpdatesCh <-chan watcher.ConfigEvent) *Syncer {
	return &Syncer{
		ctx:             ctx,
		config:          config,
		bucket:          bucket,
		configUpdatesCh: configUpdatesCh,
	}
}

func (s *Syncer) Start() error {
	ticker := time.NewTicker(s.config.UploadFrequency)
	defer ticker.Stop()

	uploadsEnabled := false

	return s.runEventLoop(ticker, &uploadsEnabled)
}

func (s *Syncer) runEventLoop(ticker *time.Ticker, uploadsEnabled *bool) error {
	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case event := <-s.configUpdatesCh:
			*uploadsEnabled = event.HasNode
			s.attemptSync(uploadsEnabled)
		case <-ticker.C:
			s.attemptSync(uploadsEnabled)
		}
	}
}

func (s *Syncer) attemptSync(uploadsEnabled *bool) {
	if *uploadsEnabled {
		if err := s.syncBelowLogDir(); err != nil {
			log.Printf("error syncing below log dir: %v", err)
		}
	}
}

func (s *Syncer) syncBelowLogDir() error {
	bundler := bundler.New(s.config.BelowLogDir)
	belowTar, err := bundler.Bundle()
	if err != nil {
		return fmt.Errorf("error creating tar.gz of below log dir: %v", err)
	}
	defer belowTar.Close()

	return s.uploadToGCS(belowTar)
}

func (s *Syncer) uploadToGCS(belowTar io.Reader) error {
	ctx, cancel := context.WithTimeout(s.ctx, s.config.ContextTimeout)
	defer cancel()

	objectName := fmt.Sprintf("below_%s.tar.gz", s.config.CurrentNode)
	writer := s.bucket.Object(objectName).NewWriter(ctx)
	if writer == nil {
		return fmt.Errorf("failed to get object writer for %q", objectName)
	}

	if _, err := io.Copy(writer, belowTar); err != nil {
		return fmt.Errorf("failed to copy tar.gz to bucket: %v", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to close writer for %q: %v", objectName, err)
	}

	log.Printf("successfully uploaded tar.gz file %q to bucket", objectName)
	return nil
}
