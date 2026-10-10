package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/sundayfun/sundial"
	yamlcodec "github.com/sundayfun/sundial/codec/yaml"
	s3provider "github.com/sundayfun/sundial/provider/s3"
)

type config struct {
	Server serverConfig `yaml:"server"`
	Debug  bool         `yaml:"debug"`
}

type serverConfig struct {
	Host string `yaml:"host"`
	Port int    `yaml:"port"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	port := flag.Int("port", -1, "update the server port; negative is read-only")
	initialConfig := flag.String("init", "", "publish an initial configuration file before loading")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	storage := s3provider.StorageConfig{
		Bucket:             os.Getenv("SUNDIAL_S3_BUCKET"),
		CurrentRevisionKey: os.Getenv("SUNDIAL_S3_CURRENT_REVISION_KEY"),
		RevisionKeyPrefix:  os.Getenv("SUNDIAL_S3_REVISION_KEY_PREFIX"),
		WatchInterval:      0,
	}
	provider, err := s3provider.NewProvider(ctx, &s3provider.Config{
		Region:        "",
		Endpoint:      "",
		UsePathStyle:  false,
		StorageConfig: storage,
	})
	if err != nil {
		return err
	}

	if *initialConfig != "" {
		data, readErr := os.ReadFile(*initialConfig)
		if readErr != nil {
			return fmt.Errorf("read initial configuration: %w", readErr)
		}
		if _, putErr := provider.Put(ctx, data); putErr != nil {
			return fmt.Errorf("publish initial configuration: %w", putErr)
		}
	}

	store, err := sundial.New(ctx, provider,
		sundial.WithCodec[config](yamlcodec.New()))
	if err != nil {
		return err
	}

	entry := store.Get()
	printEntry("loaded", entry)

	if *port >= 0 {
		entry, err = store.Update(ctx, func(draft *config) error {
			draft.Server.Port = *port
			return nil
		})
		if err != nil {
			return fmt.Errorf("update configuration: %w", err)
		}
		printEntry("updated", entry)
	}

	return nil
}

func printEntry(event string, entry sundial.Entry[config]) {
	log.Printf(
		"%s: host=%s port=%d debug=%t revision_id=%s",
		event,
		entry.Value.Server.Host,
		entry.Value.Server.Port,
		entry.Value.Debug,
		entry.Revision.ID,
	)
}
