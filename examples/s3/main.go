package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/sundayfun/sundial"
	s3provider "github.com/sundayfun/sundial/provider/s3"
)

type config struct {
	Server serverConfig `json:"server"`
	Debug  bool         `json:"debug"`
}

type serverConfig struct {
	Host string `json:"host"`
	Port int    `json:"port"`
}

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	port := flag.Int("port", -1, "update the server port; negative is read-only")
	flag.Parse()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	store, err := s3provider.New[config](
		ctx,
		&s3provider.Config{
			Region:       os.Getenv("AWS_REGION"),
			Bucket:       os.Getenv("SUNDIAL_S3_BUCKET"),
			PathPrefix:   os.Getenv("SUNDIAL_S3_PATH_PREFIX"),
			Key:          os.Getenv("SUNDIAL_S3_KEY"),
			Endpoint:     "",
			UsePathStyle: false,
			// Zero uses the default 30-second interval.
			WatchInterval: 0,
		},
		// Optional: called after a changed configuration is reloaded.
		sundial.WithOnChange(func(entry sundial.Entry[config]) {
			printEntry("reloaded", entry)
		}),
		// Optional: called when automatic reload fails.
		sundial.WithOnError[config](func(reloadErr error) {
			log.Printf("reload configuration: %v", reloadErr)
		}),
	)
	if err != nil {
		return err
	}

	entry, err := store.Get()
	if err != nil {
		return fmt.Errorf("get loaded configuration: %w", err)
	}
	printEntry("loaded", entry)

	if *port >= 0 {
		entry.Value.Server.Port = *port
		updatedEntry, putErr := store.Put(ctx, entry)
		if putErr != nil {
			if sundial.IsConflict(putErr) {
				return errors.New("configuration changed before it could be saved")
			}
			return fmt.Errorf("put configuration: %w", putErr)
		}
		printEntry("updated", updatedEntry)
	}

	return nil
}

func printEntry(event string, entry sundial.Entry[config]) {
	log.Printf(
		"%s: host=%s port=%d debug=%t revision=%s",
		event,
		entry.Value.Server.Host,
		entry.Value.Server.Port,
		entry.Value.Debug,
		entry.Metadata.Revision,
	)
}
