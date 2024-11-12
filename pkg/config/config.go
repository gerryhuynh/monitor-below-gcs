package config

import (
	"fmt"
	"os"
	"time"

	"github.com/spf13/pflag"
)

type Config struct {
	ConfigPath      string
	BucketName      string
	UploadFrequency time.Duration
	ContextTimeout  time.Duration
	CurrentNode     string
	BelowLogDir     string
}

func New() Config {
	c := Config{}

	pflag.StringVar(&c.ConfigPath, "config-path", "", "the path to the config file (required)")
	pflag.StringVar(&c.BucketName, "bucket-name", "", "the name of the GCS bucket (required)")
	pflag.DurationVar(&c.UploadFrequency, "upload-frequency", time.Minute, "the frequency for uploads to GCS")
	pflag.DurationVar(&c.ContextTimeout, "context-timeout", time.Minute, "the timeout duration when uploading files to GCS")
	pflag.StringVar(&c.BelowLogDir, "below-log-dir", "/var/log/below/store", "the directory containing the below log files")

	pflag.Parse()

	numMissingFlags := 0
	pflag.VisitAll(func(f *pflag.Flag) {
		if f.Value.String() == "" {
			numMissingFlags++
		}
	})

	if numMissingFlags > 0 {
		fmt.Fprintf(os.Stderr, "Error: required flags are not set: %d\n", numMissingFlags)
		pflag.Usage()
		os.Exit(1)
	}

	if c.ContextTimeout <= 0 {
		panic("context-timeout must be greater than 0")
	}

	hostname, err := os.Hostname()
	if err != nil {
		panic(fmt.Sprintf("error getting hostname: %v", err))
	}
	c.CurrentNode = hostname

	return c
}
