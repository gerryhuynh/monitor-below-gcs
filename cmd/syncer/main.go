package main

import (
	"context"
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/gerryhuynh/monitor-below-gcs/pkg/config"
	"github.com/gerryhuynh/monitor-below-gcs/pkg/syncer"
	"github.com/gerryhuynh/monitor-below-gcs/pkg/watcher"
)

func main() {
	config := config.New()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := storage.NewClient(ctx)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	bkt := client.Bucket(config.BucketName)
	if bkt == nil {
		panic(fmt.Sprintf("failed to get bucket %q", config.BucketName))
	}

	w, err := watcher.New(config.ConfigPath, config.CurrentNode)
	if err != nil {
		panic(err)
	}
	defer w.Close()

	configUpdatesCh, err := w.StartWatch()
	if err != nil {
		panic(err)
	}

	stopCh := make(chan struct{})
	defer close(stopCh)

	syncer := syncer.New(ctx, config, bkt, configUpdatesCh)
	if err := syncer.Start(); err != nil {
		panic(err)
	}

	<-stopCh
}
