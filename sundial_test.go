package sundial_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sundayfun/sundial"
	yamlcodec "github.com/sundayfun/sundial/codec/yaml"
	providertesting "github.com/sundayfun/sundial/provider/testing"
)

type testConfig struct {
	Server  serverConfig      `json:"server"`
	Enabled bool              `json:"enabled"`
	Ratio   float64           `json:"ratio"`
	Counter int               `json:"counter"`
	Labels  map[string]string `json:"labels"`
}

type serverConfig struct {
	Host string     `json:"host"`
	Port int        `json:"port"`
	Tags []string   `json:"tags"`
	TLS  *tlsConfig `json:"tls"`
}

type tlsConfig struct {
	Enabled bool `json:"enabled"`
}

type prefixedJSONCodec struct {
	prefix []byte
}

type fixedEncodeCodec struct {
	encoded []byte
}

type reloadErrorWatcher struct {
	*providertesting.Provider

	reloadErr      error
	failGet        atomic.Bool
	callbackResult chan error
}

type cancellationWatcher struct {
	*providertesting.Provider

	stopped chan struct{}
}

func (p *reloadErrorWatcher) Get(ctx context.Context) ([]byte, sundial.Metadata, error) {
	if p.failGet.Load() {
		return nil, sundial.Metadata{}, p.reloadErr
	}
	return p.Provider.Get(ctx)
}

func (p *reloadErrorWatcher) Watch(_ context.Context, notify func() error) error {
	p.failGet.Store(true)
	err := notify()
	p.callbackResult <- err
	return err
}

func (p *cancellationWatcher) Watch(ctx context.Context, notify func() error) error {
	if err := notify(); err != nil {
		return err
	}
	<-ctx.Done()
	close(p.stopped)
	return ctx.Err()
}

func (c prefixedJSONCodec) Encode(value any) ([]byte, error) {
	data, err := json.Marshal(value)
	return append(append([]byte(nil), c.prefix...), data...), err
}

func (c prefixedJSONCodec) Decode(data []byte, value any) error {
	if !bytes.HasPrefix(data, c.prefix) {
		return errors.New("missing custom prefix")
	}
	return json.Unmarshal(bytes.TrimPrefix(data, c.prefix), value)
}

func (c fixedEncodeCodec) Encode(any) ([]byte, error) {
	return append([]byte(nil), c.encoded...), nil
}

func (fixedEncodeCodec) Decode(data []byte, value any) error {
	return json.Unmarshal(data, value)
}

func TestNewLoadsTypedConfigurationIntoMemory(t *testing.T) {
	t.Parallel()

	provider := providertesting.New([]byte(`{
		"server":{"host":"127.0.0.1","port":8080},
		"ratio":1.5,
		"enabled":true
	}`))
	configStore, err := sundial.New[testConfig](
		t.Context(),
		provider,
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	entry, err := configStore.Get()
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	config := entry.Value
	if config.Server.Port != 8080 {
		t.Fatalf("Get().Server.Port = %d, want 8080", config.Server.Port)
	}
	if config.Ratio != 1.5 {
		t.Fatalf("Get().Ratio = %v, want 1.5", config.Ratio)
	}
	if !config.Enabled {
		t.Fatal("Get().Enabled = false, want true")
	}
	if got := provider.GetCount(); got != 1 {
		t.Fatalf("GetCount() = %d, want 1", got)
	}

	for range 10 {
		if _, getErr := configStore.Get(); getErr != nil {
			t.Fatalf("Get() error = %v", getErr)
		}
	}
	if got := provider.GetCount(); got != 1 {
		t.Fatalf("memory reads called Provider.Get: count = %d", got)
	}
}

func TestNewRejectsMissingConfiguration(t *testing.T) {
	t.Parallel()

	_, err := sundial.New[testConfig](
		context.Background(),
		providertesting.New(nil),
	)
	if !errors.Is(err, sundial.ErrNotFound) {
		t.Fatalf("New() error = %v, want ErrNotFound", err)
	}
}

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	_, err := sundial.New[testConfig](
		context.Background(),
		providertesting.New([]byte(`{"server":`)),
	)
	if err == nil {
		t.Fatal("New() error = nil, want decode failure")
	}
}

func TestNewRejectsEmptyConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		data    []byte
		options []sundial.Option[testConfig]
	}{
		{
			name:    "empty JSON",
			data:    []byte{},
			options: nil,
		},
		{
			name:    "whitespace JSON",
			data:    []byte(" \n\t"),
			options: nil,
		},
		{
			name:    "empty YAML",
			data:    []byte{},
			options: []sundial.Option[testConfig]{sundial.WithCodec[testConfig](yamlcodec.New())},
		},
		{
			name:    "whitespace YAML",
			data:    []byte(" \n\t"),
			options: []sundial.Option[testConfig]{sundial.WithCodec[testConfig](yamlcodec.New())},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := sundial.New[testConfig](
				context.Background(),
				providertesting.New(test.data),
				test.options...,
			)
			if !errors.Is(err, sundial.ErrEmptyDocument) {
				t.Fatalf("New() error = %v, want ErrEmptyDocument", err)
			}
		})
	}
}

func TestNewAcceptsEmptyObjectConfiguration(t *testing.T) {
	t.Parallel()

	_, err := sundial.New[testConfig](
		t.Context(),
		providertesting.New([]byte(`{}`)),
	)
	if err != nil {
		t.Fatalf("New() error = %v, want nil", err)
	}
}

func TestWithLoggerLogsSuccessfulOperationsAndErrors(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{
		AddSource:   false,
		Level:       slog.LevelDebug,
		ReplaceAttr: nil,
	}))
	provider := providertesting.New([]byte(`{"enabled":false}`))
	configStore, err := sundial.New[testConfig](
		t.Context(),
		provider,
		sundial.WithLogger[testConfig](logger),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	entry, err := configStore.Get()
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	entry.Value.Enabled = true
	if _, putErr := configStore.Put(t.Context(), entry); putErr != nil {
		t.Fatalf("Put() error = %v", putErr)
	}

	provider.SetData([]byte(`{"enabled":false}`))
	if reloadErr := configStore.Reload(t.Context()); reloadErr != nil {
		t.Fatalf("Reload() error = %v", reloadErr)
	}
	provider.SetData([]byte(`{"enabled":`))
	if reloadErr := configStore.Reload(t.Context()); reloadErr == nil {
		t.Fatal("Reload() error = nil, want decode failure")
	}

	provider.SetPutIfRevisionError(errors.New("backend unavailable"))
	entry, err = configStore.Get()
	if err != nil {
		t.Fatalf("Get() before failed Put error = %v", err)
	}
	if _, putErr := configStore.Put(t.Context(), entry); putErr == nil {
		t.Fatal("Put() error = nil, want failure")
	}

	logs := output.String()
	for _, message := range []string{
		`level=DEBUG msg="loaded configuration"`,
		`level=DEBUG msg="put configuration"`,
		`level=DEBUG msg="reloaded configuration"`,
		`level=ERROR msg="reload configuration"`,
		`level=ERROR msg="put configuration"`,
		`error="sundial: put configuration: backend unavailable"`,
	} {
		if !strings.Contains(logs, message) {
			t.Errorf("logs do not contain %q:\n%s", message, logs)
		}
	}
}

func TestWithLoggerLogsInitialLoadError(t *testing.T) {
	t.Parallel()

	var output bytes.Buffer
	logger := slog.New(slog.NewTextHandler(&output, &slog.HandlerOptions{
		AddSource:   false,
		Level:       slog.LevelDebug,
		ReplaceAttr: nil,
	}))
	_, err := sundial.New[testConfig](
		t.Context(),
		providertesting.New([]byte(`{"enabled":`)),
		sundial.WithLogger[testConfig](logger),
	)
	if err == nil {
		t.Fatal("New() error = nil, want decode failure")
	}

	logs := output.String()
	if !strings.Contains(logs, `level=ERROR msg="load configuration"`) {
		t.Fatalf("logs do not contain initial load error:\n%s", logs)
	}
}

func TestCustomCodec(t *testing.T) {
	t.Parallel()

	const prefix = "custom:"
	provider := providertesting.New([]byte(`custom:{"enabled":true}`))
	configStore, err := sundial.New[testConfig](
		t.Context(),
		provider,
		sundial.WithCodec[testConfig](prefixedJSONCodec{prefix: []byte(prefix)}),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	entry, err := configStore.Get()
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	config := entry.Value
	config.Enabled = false
	entry.Value = config
	if _, putErr := configStore.Put(context.Background(), entry); putErr != nil {
		t.Fatalf("Put() error = %v", putErr)
	}
	if !bytes.HasPrefix(provider.Data(), []byte(prefix)) {
		t.Fatalf("saved data = %q, want custom prefix", provider.Data())
	}
}

func TestYAMLCodec(t *testing.T) {
	t.Parallel()

	provider := providertesting.New([]byte("server:\n  port: 8080\n"))
	configStore, err := sundial.New[testConfig](
		t.Context(),
		provider,
		sundial.WithCodec[testConfig](yamlcodec.New()),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	entry, err := configStore.Get()
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	config := entry.Value
	if config.Server.Port != 8080 {
		t.Fatalf("Get().Server.Port = %d, want 8080", config.Server.Port)
	}

	entry, err = configStore.Get()
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	config = entry.Value
	config.Server.Port = 9090
	entry.Value = config
	if _, putErr := configStore.Put(context.Background(), entry); putErr != nil {
		t.Fatalf("Put() error = %v", putErr)
	}
	if !bytes.Contains(provider.Data(), []byte("port: 9090")) {
		t.Fatalf("saved data = %q, want YAML port 9090", provider.Data())
	}
}

func TestPutPersistsCompleteDocument(t *testing.T) {
	t.Parallel()

	provider := providertesting.New([]byte(`{
		"server":{"host":"127.0.0.1","port":8080},
		"enabled":true
	}`))
	configStore, err := sundial.New[testConfig](
		t.Context(),
		provider,
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	entry, err := configStore.Get()
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	config := entry.Value
	config.Server.Port = 9090
	entry.Value = config
	savedEntry, putErr := configStore.Put(context.Background(), entry)
	if putErr != nil {
		t.Fatalf("Put() error = %v", putErr)
	}
	if savedEntry.Metadata.Revision == entry.Metadata.Revision {
		t.Fatalf("Put() revision = %q, want a new revision", savedEntry.Metadata.Revision)
	}
	currentEntry, getErr := configStore.Get()
	if getErr != nil {
		t.Fatalf("Get() after Put error = %v", getErr)
	}
	if !reflect.DeepEqual(savedEntry, currentEntry) {
		t.Fatalf("Put() entry = %#v, want current Entry %#v", savedEntry, currentEntry)
	}

	if got := provider.PutIfRevisionCount(); got != 1 {
		t.Fatalf("PutIfRevisionCount() = %d, want 1", got)
	}
	var saved testConfig
	if decodeErr := json.Unmarshal(provider.Data(), &saved); decodeErr != nil {
		t.Fatalf("decode saved configuration: %v", decodeErr)
	}
	if saved.Server.Host != "127.0.0.1" || saved.Server.Port != 9090 || !saved.Enabled {
		t.Fatalf("saved configuration = %#v, want complete updated document", saved)
	}

	reloaded, err := sundial.New[testConfig](
		t.Context(),
		provider,
	)
	if err != nil {
		t.Fatalf("reload New() error = %v", err)
	}
	reloadedEntry, err := reloaded.Get()
	if err != nil {
		t.Fatalf("reload Get() error = %v", err)
	}
	if reloadedEntry.Value.Server.Port != 9090 {
		t.Fatalf("reloaded port = %d, want 9090", reloadedEntry.Value.Server.Port)
	}
}

func TestPutRejectsEmptyEncodedConfiguration(t *testing.T) {
	t.Parallel()

	for _, encoded := range [][]byte{{}, []byte(" \n\t")} {
		provider := providertesting.New([]byte(`{"server":{"port":8080}}`))
		configStore, err := sundial.New[testConfig](
			t.Context(),
			provider,
			sundial.WithCodec[testConfig](fixedEncodeCodec{encoded: encoded}),
		)
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}

		entry, err := configStore.Get()
		if err != nil {
			t.Fatalf("Get() error = %v", err)
		}
		entry.Value.Server.Port = 9090
		_, putErr := configStore.Put(context.Background(), entry)
		if !errors.Is(putErr, sundial.ErrEmptyDocument) {
			t.Fatalf("Put() error = %v, want ErrEmptyDocument", putErr)
		}
		if got := provider.PutIfRevisionCount(); got != 0 {
			t.Fatalf("PutIfRevisionCount() = %d, want 0", got)
		}
		current, getErr := configStore.Get()
		if getErr != nil {
			t.Fatalf("Get() after failed Put error = %v", getErr)
		}
		if current.Value.Server.Port != 8080 {
			t.Fatalf("port after failed Put = %d, want 8080", current.Value.Server.Port)
		}
	}
}

func TestPutFailureKeepsPreviousMemory(t *testing.T) {
	t.Parallel()

	provider := providertesting.New([]byte(`{"server":{"port":8080}}`))
	configStore, err := sundial.New[testConfig](
		t.Context(),
		provider,
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	provider.SetPutIfRevisionError(errors.New("put failed"))
	entry, err := configStore.Get()
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	config := entry.Value
	config.Server.Port = 9090
	entry.Value = config
	if _, putErr := configStore.Put(context.Background(), entry); putErr == nil {
		t.Fatal("Put() error = nil, want failure")
	}

	currentEntry, err := configStore.Get()
	if err != nil {
		t.Fatalf("Get() after failed Put error = %v", err)
	}
	if currentEntry.Value.Server.Port != 8080 {
		t.Fatalf("port after failed Put = %d, want 8080", currentEntry.Value.Server.Port)
	}
}

func TestReloadFailureKeepsPreviousMemory(t *testing.T) {
	t.Parallel()

	provider := providertesting.New([]byte(`{"server":{"port":8080}}`))
	configStore, err := sundial.New[testConfig](
		t.Context(),
		provider,
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	provider.SetData([]byte(`{"server":`))
	if reloadErr := configStore.Reload(context.Background()); reloadErr == nil {
		t.Fatal("Reload() error = nil, want decode failure")
	}
	entry, err := configStore.Get()
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if entry.Value.Server.Port != 8080 {
		t.Fatalf("port after failed reload = %d, want 8080", entry.Value.Server.Port)
	}
}

func TestReloadRejectsEmptyConfigurationAndKeepsPreviousMemory(t *testing.T) {
	t.Parallel()

	provider := providertesting.New([]byte(`{"server":{"port":8080}}`))
	configStore, err := sundial.New[testConfig](t.Context(), provider)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	provider.SetData([]byte(" \n\t"))
	reloadErr := configStore.Reload(context.Background())
	if !errors.Is(reloadErr, sundial.ErrEmptyDocument) {
		t.Fatalf("Reload() error = %v, want ErrEmptyDocument", reloadErr)
	}
	entry, err := configStore.Get()
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if entry.Value.Server.Port != 8080 {
		t.Fatalf("port after failed reload = %d, want 8080", entry.Value.Server.Port)
	}
}

func TestReloadRejectsMissingConfiguration(t *testing.T) {
	t.Parallel()

	provider := providertesting.New([]byte(`{"server":{"port":8080}}`))
	configStore, err := sundial.New[testConfig](t.Context(), provider)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	provider.SetData(nil)
	if reloadErr := configStore.Reload(context.Background()); !errors.Is(reloadErr, sundial.ErrNotFound) {
		t.Fatalf("Reload() error = %v, want ErrNotFound", reloadErr)
	}
	entry, err := configStore.Get()
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if entry.Value.Server.Port != 8080 {
		t.Fatalf("port after missing reload = %d, want 8080", entry.Value.Server.Port)
	}
}

func TestGetReturnsDetachedConfiguration(t *testing.T) {
	t.Parallel()

	provider := providertesting.New([]byte(`{
		"server":{"tags":["api"],"tls":{"enabled":true}},
		"labels":{"region":"east"}
	}`))
	configStore, err := sundial.New[testConfig](
		t.Context(),
		provider,
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	entry, err := configStore.Get()
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	config := entry.Value
	config.Server.Tags[0] = "mutated"
	config.Server.TLS.Enabled = false
	config.Labels["region"] = "west"

	currentEntry, err := configStore.Get()
	if err != nil {
		t.Fatalf("second Get() error = %v", err)
	}
	current := currentEntry.Value
	if current.Server.Tags[0] != "api" {
		t.Fatalf("detached tag = %q, want api", current.Server.Tags[0])
	}
	if !current.Server.TLS.Enabled {
		t.Fatal("detached TLS.Enabled = false, want true")
	}
	if current.Labels["region"] != "east" {
		t.Fatalf("detached region = %q, want east", current.Labels["region"])
	}
}

func TestAutomaticNativeWatcherReloadsExternalChanges(t *testing.T) {
	t.Parallel()

	provider := providertesting.NewWatcher([]byte(`{"enabled":false}`))
	changed := make(chan sundial.Entry[testConfig], 1)
	_, err := sundial.New[testConfig](
		t.Context(),
		provider,
		sundial.WithOnChange[testConfig](func(entry sundial.Entry[testConfig]) {
			changed <- entry
		}),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for provider.GetCount() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if provider.GetCount() < 2 {
		t.Fatal("native watcher did not finish initial notification")
	}
	getCount := provider.GetCount()

	provider.Change([]byte(`{"enabled":true}`))
	select {
	case entry := <-changed:
		if !entry.Value.Enabled {
			t.Fatal("OnChange() Enabled = false, want true")
		}
		if entry.Metadata.Revision != "2" {
			t.Fatalf("OnChange() revision = %q, want 2", entry.Metadata.Revision)
		}
	case <-time.After(time.Second):
		t.Fatal("native watcher did not report external change")
	}
	if got := provider.GetCount(); got != getCount+1 {
		t.Fatalf("Provider.Get() count = %d, want %d without callback Get", got, getCount+1)
	}
}

func TestContextCancellationStopsAutomaticReload(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	provider := &cancellationWatcher{
		Provider: providertesting.New([]byte(`{"enabled":false}`)),
		stopped:  make(chan struct{}),
	}
	configStore, err := sundial.New[testConfig](
		ctx,
		provider,
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	cancel()
	select {
	case <-provider.stopped:
	case <-time.After(time.Second):
		t.Fatal("automatic reload did not stop after context cancellation")
	}
	provider.SetData([]byte(`{"enabled":true}`))

	entry, err := configStore.Get()
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if entry.Value.Enabled {
		t.Fatal("configuration changed after context cancellation")
	}
}

func TestNativeWatcherReceivesReloadError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("load failed")
	provider := &reloadErrorWatcher{
		Provider:       providertesting.New([]byte(`{"enabled":false}`)),
		reloadErr:      wantErr,
		callbackResult: make(chan error, 1),
	}
	reported := make(chan error, 1)
	_, err := sundial.New[testConfig](
		t.Context(),
		provider,
		sundial.WithOnError[testConfig](func(err error) {
			reported <- err
		}),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	select {
	case callbackErr := <-provider.callbackResult:
		if !errors.Is(callbackErr, wantErr) {
			t.Fatalf("Watcher callback error = %v, want %v", callbackErr, wantErr)
		}
	case <-time.After(time.Second):
		t.Fatal("Watcher callback was not invoked")
	}
	select {
	case reportedErr := <-reported:
		if !errors.Is(reportedErr, wantErr) {
			t.Fatalf("OnError() error = %v, want %v", reportedErr, wantErr)
		}
	case <-time.After(time.Second):
		t.Fatal("OnError() was not invoked")
	}
	select {
	case duplicateErr := <-reported:
		t.Fatalf("OnError() was invoked twice; second error = %v", duplicateErr)
	default:
	}
}

func TestPutRejectsStaleRevisionWithinInstance(t *testing.T) {
	t.Parallel()

	provider := providertesting.New([]byte(`{"counter":0,"enabled":false}`))
	configStore, err := sundial.New[testConfig](t.Context(), provider)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	firstEntry, err := configStore.Get()
	if err != nil {
		t.Fatalf("first Get() error = %v", err)
	}
	first := firstEntry.Value
	staleEntry, err := configStore.Get()
	if err != nil {
		t.Fatalf("second Get() error = %v", err)
	}
	stale := staleEntry.Value

	first.Counter = 1
	firstEntry.Value = first
	if _, putErr := configStore.Put(context.Background(), firstEntry); putErr != nil {
		t.Fatalf("first Put() error = %v", putErr)
	}
	stale.Enabled = true
	staleEntry.Value = stale
	_, putErr := configStore.Put(context.Background(), staleEntry)
	if !errors.Is(putErr, sundial.ErrConflict) {
		t.Fatalf("stale Put() error = %v, want ErrConflict", putErr)
	}

	currentEntry, err := configStore.Get()
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	current := currentEntry.Value
	if current.Counter != 1 || current.Enabled {
		t.Fatalf("configuration after conflict = %#v, want first write only", current)
	}
}

func TestIsConflict(t *testing.T) {
	t.Parallel()

	if !sundial.IsConflict(errors.Join(errors.New("provider rejected write"), sundial.ErrConflict)) {
		t.Fatal("IsConflict() = false for wrapped ErrConflict")
	}
	if sundial.IsConflict(errors.New("provider unavailable")) {
		t.Fatal("IsConflict() = true for unrelated error")
	}
}

func TestIsNotFound(t *testing.T) {
	t.Parallel()

	if !sundial.IsNotFound(errors.Join(errors.New("provider load failed"), sundial.ErrNotFound)) {
		t.Fatal("IsNotFound() = false for wrapped ErrNotFound")
	}
	if sundial.IsNotFound(errors.New("provider unavailable")) {
		t.Fatal("IsNotFound() = true for unrelated error")
	}
}

func TestPutRejectsStaleRevisionAcrossInstancesAndAllowsRetry(t *testing.T) {
	t.Parallel()

	provider := providertesting.New([]byte(`{"counter":0,"enabled":false}`))
	firstStore, err := sundial.New[testConfig](t.Context(), provider)
	if err != nil {
		t.Fatalf("first New() error = %v", err)
	}
	secondStore, err := sundial.New[testConfig](t.Context(), provider)
	if err != nil {
		t.Fatalf("second New() error = %v", err)
	}

	firstEntry, err := firstStore.Get()
	if err != nil {
		t.Fatalf("first Get() error = %v", err)
	}
	first := firstEntry.Value
	secondEntry, err := secondStore.Get()
	if err != nil {
		t.Fatalf("second Get() error = %v", err)
	}
	second := secondEntry.Value

	first.Counter = 1
	firstEntry.Value = first
	if _, putErr := firstStore.Put(context.Background(), firstEntry); putErr != nil {
		t.Fatalf("first Put() error = %v", putErr)
	}
	second.Enabled = true
	secondEntry.Value = second
	_, putErr := secondStore.Put(context.Background(), secondEntry)
	if !errors.Is(putErr, sundial.ErrConflict) {
		t.Fatalf("stale Put() error = %v, want ErrConflict", putErr)
	}

	if reloadErr := secondStore.Reload(context.Background()); reloadErr != nil {
		t.Fatalf("Reload() error = %v", reloadErr)
	}
	secondEntry, err = secondStore.Get()
	if err != nil {
		t.Fatalf("retry Get() error = %v", err)
	}
	second = secondEntry.Value
	second.Enabled = true
	secondEntry.Value = second
	if _, putErr := secondStore.Put(context.Background(), secondEntry); putErr != nil {
		t.Fatalf("retry Put() error = %v", putErr)
	}

	var saved testConfig
	if decodeErr := json.Unmarshal(provider.Data(), &saved); decodeErr != nil {
		t.Fatalf("decode saved configuration: %v", decodeErr)
	}
	if saved.Counter != 1 || !saved.Enabled {
		t.Fatalf("configuration after retry = %#v, want both changes", saved)
	}
}

func TestPutRejectsEntryWithoutRevision(t *testing.T) {
	t.Parallel()

	provider := providertesting.New([]byte(`{"counter":0}`))
	configStore, err := sundial.New[testConfig](t.Context(), provider)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	_, putErr := configStore.Put(context.Background(), sundial.Entry[testConfig]{
		Value: testConfig{Counter: 1},
	})
	if !errors.Is(putErr, sundial.ErrConflict) {
		t.Fatalf("Put() error = %v, want ErrConflict", putErr)
	}
	if string(provider.Data()) != `{"counter":0}` {
		t.Fatalf("provider data = %s, want unchanged configuration", provider.Data())
	}
}

func TestReloadTracksChangedProviderRevisionWhenContentIsUnchanged(t *testing.T) {
	t.Parallel()

	data := []byte(`{"enabled":true}`)
	provider := providertesting.New(data)
	configStore, err := sundial.New[testConfig](t.Context(), provider)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	provider.SetData(data)
	if reloadErr := configStore.Reload(context.Background()); reloadErr != nil {
		t.Fatalf("Reload() error = %v", reloadErr)
	}
	entry, err := configStore.Get()
	if err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	config := entry.Value
	config.Enabled = false
	entry.Value = config
	if _, putErr := configStore.Put(context.Background(), entry); putErr != nil {
		t.Fatalf("Put() error = %v", putErr)
	}
	if got := provider.PutIfRevisionCount(); got != 1 {
		t.Fatalf("PutIfRevisionCount() = %d, want 1 without a stale-revision retry", got)
	}
}

func TestConcurrentReadsAndWrites(t *testing.T) {
	t.Parallel()

	provider := providertesting.New([]byte(`{"counter":0}`))
	configStore, err := sundial.New[testConfig](
		t.Context(),
		provider,
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var group sync.WaitGroup
	for i := range 20 {
		group.Add(2)
		go func(value int) {
			defer group.Done()
			for {
				entry, getErr := configStore.Get()
				if getErr != nil {
					t.Errorf("Get() error = %v", getErr)
					return
				}
				config := entry.Value
				config.Counter = value
				entry.Value = config
				_, putErr := configStore.Put(context.Background(), entry)
				if errors.Is(putErr, sundial.ErrConflict) {
					continue
				}
				if putErr != nil {
					t.Errorf("Put() error = %v", putErr)
				}
				return
			}
		}(i)
		go func() {
			defer group.Done()
			if _, getErr := configStore.Get(); getErr != nil {
				t.Errorf("Get() error = %v", getErr)
			}
		}()
	}
	group.Wait()
}
