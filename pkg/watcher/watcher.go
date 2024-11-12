package watcher

import (
	"fmt"
	"os"
	"slices"

	"github.com/fsnotify/fsnotify"
	"gopkg.in/yaml.v2"
)

type Nodes struct {
	Nodes []string `yaml:"nodes"`
}

type ConfigEvent struct {
	FSEvent fsnotify.Event
	HasNode bool
}

type Watcher struct {
	watcher     *fsnotify.Watcher
	configPath  string
	currentNode string
}

func New(configPath string, currentNode string) (*Watcher, error) {
	fileInfo, err := os.Stat(configPath)
	if err != nil {
		return nil, fmt.Errorf("error accessing config path: %v", err)
	}
	if fileInfo.IsDir() {
		return nil, fmt.Errorf("config path is a directory, expected a file")
	}

	w, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("error creating watcher: %v", err)
	}

	if err := w.Add(configPath); err != nil {
		return nil, fmt.Errorf("error adding config path to watcher: %v", err)
	}

	return &Watcher{
		watcher:     w,
		configPath:  configPath,
		currentNode: currentNode,
	}, nil
}

func (w *Watcher) Close() error {
	return w.watcher.Close()
}

func (w *Watcher) StartWatch() (<-chan ConfigEvent, error) {
	configUpdatesCh := make(chan ConfigEvent)

	hasNode, err := w.checkNodeInConfig()
	if err != nil {
		return nil, fmt.Errorf("error checking if node is in config: %v", err)
	}

	go func() {
		configUpdatesCh <- ConfigEvent{
			FSEvent: fsnotify.Event{},
			HasNode: hasNode,
		}
		w.watchLoop(configUpdatesCh)
	}()

	return configUpdatesCh, nil
}

func (w *Watcher) watchLoop(configUpdatesCh chan<- ConfigEvent) {
	for {
		select {
		case event, ok := <-w.watcher.Events:
			if !ok {
				close(configUpdatesCh)
				return
			}

			if event.Op == fsnotify.Write {
				hasNode, err := w.checkNodeInConfig()
				if err != nil {
					fmt.Printf("error checking if node is in config: %v\n", err)
					continue
				}

				configUpdatesCh <- ConfigEvent{
					FSEvent: event,
					HasNode: hasNode,
				}
			}
		case err, ok := <-w.watcher.Errors:
			if !ok {
				close(configUpdatesCh)
				return
			}
			fmt.Printf("error from watcher: %v\n", err)
		}
	}
}

func (w *Watcher) checkNodeInConfig() (bool, error) {
	data, err := os.ReadFile(w.configPath)
	if err != nil {
		return false, fmt.Errorf("error reading config file: %v", err)
	}

	var nodes Nodes
	if err := yaml.Unmarshal(data, &nodes); err != nil {
		return false, fmt.Errorf("error loading config file: %v", err)
	}

	return slices.Contains(nodes.Nodes, w.currentNode), nil
}
