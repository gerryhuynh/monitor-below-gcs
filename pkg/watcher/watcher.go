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

	return &Watcher{watcher: w, configPath: configPath, currentNode: currentNode}, nil
}

func (w *Watcher) Close() error {
	return w.watcher.Close()
}

func (w *Watcher) StartWatch() (<-chan fsnotify.Event, error) {
	configUpdatesCh := make(chan fsnotify.Event)

	go func() {
		for {
			select {
			case event, ok := <-w.watcher.Events:
				if !ok {
					return
				}

				if event.Op == fsnotify.Write {
					if ok, err := w.checkNodeInConfig(); err != nil {
						fmt.Printf("error checking if node is in config: %v", err)
						continue
					} else if !ok {
						fmt.Printf("node not in config")
						continue
					}

					configUpdatesCh <- event
				}
			case err, ok := <-w.watcher.Errors:
				if !ok {
					return
				}
				fmt.Printf("error from watcher: %v", err)
			}
		}
	}()

	return configUpdatesCh, nil
}

func (w *Watcher) checkNodeInConfig() (bool, error) {
	var nodes Nodes
	data, err := os.ReadFile(w.configPath)
	if err != nil {
		return false, fmt.Errorf("error reading config file: %v", err)
	}

	if err := yaml.Unmarshal(data, &nodes); err != nil {
		return false, fmt.Errorf("error loading config file: %v", err)
	}

	return slices.Contains(nodes.Nodes, w.currentNode), nil
}
