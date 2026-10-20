# Sundial

[![Go Reference](https://pkg.go.dev/badge/github.com/sundayfun/sundial.svg)](https://pkg.go.dev/github.com/sundayfun/sundial)

[简体中文](README.zh-CN.md)

Sundial is a lightweight, extensible, type-safe configuration SDK for Go with
in-memory reads, persistent writes, and live updates.

## Why Sundial

- **Type-safe access** — applications read their own configuration struct instead of string paths and `any` values.
- **Fast reads** — `Get` returns the already parsed in-memory configuration without decoding or deep copying.
- **Persistent writes** — `Update` conditionally saves one complete typed configuration document.
- **Version history** — browse historical revisions and restore configuration.
- **Live updates** — automatic reload keeps memory synchronized with external changes.
- **Extensible storage and formats** — storage sources implement `Provider`; JSON works by default and other formats use codecs.

One `Client` manages one complete configuration document.

## In action

![Sundial in-memory reads, concurrent write protection, and revision restore](docs/images/sundial-overview.png)

If two clients cache revision A, the first successful update creates B; the second
write based on A returns `ErrConflict`. Later, restoring A while C is current
creates a new revision D with A's content, preserving the full history.

## Installation

```sh
go get github.com/sundayfun/sundial
```

## Quick start

This example reads and updates an existing JSON configuration in S3.
For credentials and initial publication, see the [S3 example](examples/s3).

```go
package main

import (
    "context"
    "fmt"
    "log"

    s3provider "github.com/sundayfun/sundial/provider/s3"
)

type Config struct {
    Port int `json:"port"`
}

func main() {
    ctx := context.Background()

    store, err := s3provider.New[Config](ctx, &s3provider.Config{
        Region: "us-east-1",
        StorageConfig: s3provider.StorageConfig{
            Bucket:             "my-config-bucket",
            CurrentRevisionKey: "production/app/metadata.yaml",
            RevisionKeyPrefix:  "production/app/",
        },
    })
    if err != nil {
        log.Fatal(err)
    }

    defer store.Close()

    fmt.Println(store.Get().Port)

    if _, err := store.Update(ctx, func(config *Config) error {
        config.Port = 9090
        return nil
    }); err != nil {
        log.Fatal(err)
    }
}
```

`Get() T` returns the current configuration directly. Use `GetEntry() Entry[T]`
when you need the configuration and its revision paired from one atomic read:

```go
entry := store.GetEntry()
fmt.Println(entry.Value.Port, entry.Revision.ID)
```

Both methods return shared read-only values without an error return, encoding,
decoding or deep copying. Do not modify the returned configuration,
including nested maps, slices and pointers. Go does not enforce this read-only
contract; callers must follow it. Successful `Update` and `RestoreRevision` results and values
passed to `OnChange` have the same shared read-only contract.

Ordinary reads return the configuration directly; versioned current state uses `Entry[T]`:

| API | Return values or callback |
| --- | --- |
| `Get()` | `T` |
| `GetEntry()` | `Entry[T]` |
| `Update(ctx, modify)` | `(Entry[T], error)` |
| `RestoreRevision(ctx, targetID, currentID)` | `(Entry[T], error)` |
| `GetRevision(ctx, revisionID)` | `(T, Revision, error)` |
| `WithOnChange(callback)` | `func(Entry[T])` |

Migration from the previous API: replace `Get().Value` with `Get()`, and use
`GetEntry()` wherever you previously needed the complete entry or revision.
`Update`, `RestoreRevision`, `WithOnChange` and `GetRevision` keep their existing signatures.

For ordinary edits, call `Update(ctx, func(*T) error)` directly; no preceding `Get`
is needed. This is the client's only ordinary write interface. Internally, it
decodes the cached source document into an independent draft, applies the
callback, then encodes and decodes the result to validate it and isolate the
stored snapshot before publishing with revision protection. Each update uses
one encode and two decodes; the snapshot retains the source document.
Any encoding, decoding, callback or publication failure leaves the cached
snapshot unchanged. The returned configuration is shared read-only. Do not modify the
callback's draft concurrently with the call. The callback must not call `Update`,
`Reload` or `RestoreRevision` on the same client because they acquire the same
write lock.

`New(ctx, provider, opts...)` needs no clone function from the caller. The internal
draft reparses the cached document using the codec; a codec that injects dynamic
values may produce a draft different from the already parsed snapshot. Each codec
`Decode` call must create independent mutable objects, without retaining or
reusing their references; `Encode` must not modify its input.

`Update` saves the complete document using compare-and-swap (CAS) against the
cached snapshot's revision. If another client publishes a new revision first,
the stale write returns `ErrConflict`, without automatic merging or retries.
Canceling the context stops automatic reload.
Failed writes or reloads leave the last valid in-memory configuration unchanged.

## Static configuration

Use `provider/static` for a fixed, read-only configuration document:

```go
import staticprovider "github.com/sundayfun/sundial/provider/static"

provider := staticprovider.NewProvider([]byte(`{"port":8080}`))
client, err := sundial.New[Config](ctx, provider)
if err != nil {
    log.Fatal(err)
}
defer client.Close()
fmt.Println(client.Get().Port)
```

The provider copies the supplied document and returns a zero revision. It uses
the same client and codec pipeline as other providers. Pass `WithCodec` when the
document uses another format. Empty or invalid documents fail the initial load.

`Put` and `PutIfRevision` return `sundial.ErrReadOnly`. A client `Update` still
runs its modification and codec steps before the provider rejects publication;
the cached configuration remains unchanged. `Reload` rereads the same document,
and history operations return `ErrUnsupported`.

The provider does not access external storage. Its watcher notifies once, then
waits for context cancellation without polling. Call `client.Close()` when
it is no longer needed.

## Documentation

- [S3 example](examples/s3) — setup, initial publication, and conditional writes.
- [API reference](https://pkg.go.dev/github.com/sundayfun/sundial) — revision history, codecs, and reload callbacks.

## License

[MIT](LICENSE)

## Client lifecycle

Call `Close()` when a client is no longer needed. It cancels and waits for its
automatic reload goroutine, including native provider watchers and fallback
polling. Repeated and concurrent calls are safe. The parent
context is unaffected; canceling that context also stops automatic reloads.

After `Close`, `Get` and `GetEntry` return the latest retained snapshot and log
`ErrClosed` at Warn level on each call. Their signatures do not return errors.
The snapshot is no longer automatically refreshed. `Update`, `Reload`,
`GetRevision`, `ListRevisions`, and `RestoreRevision` return `ErrClosed`.
Previously returned configuration values remain valid. Operations already in
progress may complete; queued writes and reloads check closure after acquiring
the write lock. `Close` does not close the provider.
Providers must honor Watch context cancellation for shutdown to complete.

Reload callbacks run synchronously on the watcher goroutine. Do not call `Close`
from `OnChange`, `OnError`, or a provider's `Watch` method: signal the owner to
close the client after the callback returns instead.

With `WithLogger[Config](logger)`, `Close` logs its start and completion at Info,
including the elapsed duration on completion. If the watcher has not exited
after five seconds, it logs one Warn and continues waiting. This is a diagnostic
warning, not a shutdown timeout. Repeated or concurrent Close calls emit these
records only once. Logging remains disabled unless a logger is supplied.
