# Sundial

[简体中文](README.zh-CN.md)

Sundial is a lightweight, extensible, type-safe configuration SDK for Go with
in-memory reads, persistent writes, and live updates.

## Why Sundial

- **Type-safe access** — applications read their own configuration struct instead of string paths and `any` values.
- **Fast reads** — `Get` reads only from an in-memory snapshot.
- **Persistent writes** — `Put` conditionally saves one complete typed configuration document.
- **Live updates** — automatic reload keeps memory synchronized with external changes.
- **Extensible storage and formats** — storage sources implement `Provider`; JSON works by default and other formats use codecs.

One `Client` manages one complete configuration document.

## Installation

```sh
go get github.com/sundayfun/sundial
```

## Quick start

Define the configuration owned by the application:

```go
type Config struct {
	Server struct {
		Host string `json:"host"`
		Port int    `json:"port"`
	} `json:"server"`
	Debug bool `json:"debug"`
}
```

The following example uses S3:

```go
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

configStore, err := s3provider.New[Config](ctx, &s3provider.Config{
	Region: "us-east-1",
	Bucket: "my-config-bucket",
	Key:    "production/app.json",
})
if err != nil {
	log.Fatal(err)
}
```

### Read

`Get` returns a detached `Entry` from the current in-memory snapshot:

```go
entry, err := configStore.Get()
if err != nil {
	log.Fatal(err)
}

fmt.Println(entry.Value.Server.Port)
```

### Write

Modify the value and pass the same `Entry` back for a conditional write:

```go
entry.Value.Server.Port = 9090
entry, err = configStore.Put(ctx, entry)
if err != nil {
	if sundial.IsConflict(err) {
		// Reload, reapply the change, and retry if appropriate.
		log.Print("configuration changed before it could be saved")
		return
	}
	log.Fatal(err)
}
```

`Put` uses the revision in `entry.Metadata` and returns the codec-decoded saved
`Entry` with its new revision. A stale revision returns `ErrConflict`. It does
not merge or retry automatically.

JSON is used by default. Other formats can be configured with `WithCodec`.
Storage implementations live under `provider/<source>`.

See the runnable [S3 example](examples/s3).

## References

- [koanf](https://github.com/knadh/koanf)
- [Viper](https://github.com/spf13/viper)

## Behavior

- A missing document causes `New` or `Reload` to return `ErrNotFound`.
- An empty or whitespace-only document causes `New`, `Put`, or `Reload` to return a decode error.
- A failed or conflicting `Put` leaves the current in-memory snapshot unchanged.
- A failed reload keeps the last valid snapshot.
- `WithOnChange` receives the newly published `Entry`; `WithOnError` receives
  automatic reload errors.
- Canceling the context passed to `New` stops automatic reload.
- `Get` is safe for concurrent use. `Put` calls are serialized per instance,
  and stale revisions return `ErrConflict`.

## License

[MIT](LICENSE)
