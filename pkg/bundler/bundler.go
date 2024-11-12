package bundler

import (
	"archive/tar"
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type Bundler struct {
	sourceDir string
}

func New(sourceDir string) *Bundler {
	return &Bundler{sourceDir: sourceDir}
}

func (b *Bundler) Bundle() (io.ReadCloser, error) {
	dirInfo, err := os.Stat(b.sourceDir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, fmt.Errorf("below log directory does not exist")
		}
		return nil, fmt.Errorf("error accessing below log directory: %v", err)
	}
	if !dirInfo.IsDir() {
		return nil, fmt.Errorf("below log directory path is not a directory")
	}

	dirFiles, err := b.createDirSnapshot()
	if err != nil {
		return nil, fmt.Errorf("error creating below log directory snapshot: %v", err)
	}

	pr, pw := io.Pipe()

	go b.writeTar(pw, dirFiles)

	return pr, nil
}

func (b *Bundler) writeTar(pw *io.PipeWriter, dirFiles []string) error {
	gw := gzip.NewWriter(pw)
	tw := tar.NewWriter(gw)

	defer func() {
		tw.Close()
		gw.Close()
		pw.Close()
	}()

	for _, path := range dirFiles {
		info, err := os.Stat(path)
		if err != nil {
			return fmt.Errorf("error accessing file %q: %v", path, err)
		}

		header, err := tar.FileInfoHeader(info, info.Name())
		if err != nil {
			return fmt.Errorf("error creating header for %q: %v", path, err)
		}

		relPath, err := filepath.Rel(b.sourceDir, path)
		if err != nil {
			return fmt.Errorf("error getting relative path for %q: %v", path, err)
		}
		header.Name = filepath.ToSlash(relPath)

		if err := tw.WriteHeader(header); err != nil {
			return fmt.Errorf("error writing header for %q: %v", path, err)
		}

		if !info.IsDir() {
			file, err := os.Open(path)
			if err != nil {
				return fmt.Errorf("error opening file %q: %v", path, err)
			}
			if _, err := io.Copy(tw, file); err != nil {
				file.Close()
				return fmt.Errorf("error copying file %q: %v", path, err)
			}
			file.Close()
		}
	}

	return nil
}

func (b *Bundler) createDirSnapshot() ([]string, error) {
	var files []string
	err := filepath.Walk(b.sourceDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		files = append(files, path)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return files, nil
}
