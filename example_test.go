package sundial_test

import (
	"context"
	"fmt"

	"github.com/sundayfun/sundial"
	providertesting "github.com/sundayfun/sundial/provider/testing"
)

func ExampleNew() {
	type Config struct {
		Port int `json:"port"`
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel() // Stop automatic reloads when the client is no longer needed.

	// Use an in-memory test provider with an existing JSON document.
	provider := providertesting.New([]byte(`{"port":8080}`))
	client, err := sundial.New[Config](ctx, provider)
	if err != nil {
		panic(err)
	}
	entry := client.Get()
	fmt.Println(entry.Value.Port)

	// Output: 8080
}

func ExampleClient_Update_port() {
	type Config struct {
		Port int `json:"port"`
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	provider := providertesting.New([]byte(`{"port":8080}`))
	client, err := sundial.New[Config](ctx, provider)
	if err != nil {
		panic(err)
	}
	entry := client.Get()
	saved, err := client.Update(ctx, func(config *Config) error {
		config.Port = 9090
		return nil
	})
	if err != nil {
		panic(err)
	}
	current := client.Get()
	fmt.Println(saved.Value.Port)
	fmt.Println(saved.Revision.ID != entry.Revision.ID)
	fmt.Println(current.Revision.ID == saved.Revision.ID)

	// Output:
	// 9090
	// true
	// true
}

func ExampleClient_Update_conflict() {
	type Config struct {
		Port int `json:"port"`
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	provider := providertesting.New([]byte(`{"port":8080}`))
	client, err := sundial.New[Config](ctx, provider)
	if err != nil {
		panic(err)
	}
	staleClient, err := sundial.New[Config](ctx, provider)
	if err != nil {
		panic(err)
	}
	// Stop automatic reloads so the second client keeps the original snapshot.
	cancel()
	writeCtx := context.Background()
	if _, err = client.Update(writeCtx, func(config *Config) error {
		config.Port = 9090
		return nil
	}); err != nil {
		panic(err)
	}

	// A write based on the second client's stale snapshot cannot overwrite it.
	_, err = staleClient.Update(writeCtx, func(config *Config) error {
		config.Port = 7070
		return nil
	})
	fmt.Println(sundial.IsConflict(err))
	current := client.Get()
	fmt.Println(current.Value.Port)

	// Output:
	// true
	// 9090
}

func ExampleClient_Update() {
	type Config struct {
		Labels map[string]string `json:"labels"`
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	provider := providertesting.New([]byte(`{"labels":{"region":"east"}}`))
	client, err := sundial.New[Config](ctx, provider)
	if err != nil {
		panic(err)
	}

	// Edit an independent draft without first calling Get.
	saved, err := client.Update(ctx, func(config *Config) error {
		config.Labels["region"] = "west"
		return nil
	})
	if err != nil {
		panic(err)
	}
	fmt.Println(saved.Value.Labels["region"])
	fmt.Println(client.Get().Value.Labels["region"])

	// Output:
	// west
	// west
}
