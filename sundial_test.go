package sundial_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func (p *reloadErrorWatcher) Get(ctx context.Context) ([]byte, sundial.Revision, error) {
	if p.failGet.Load() {
		return nil, sundial.Revision{}, p.reloadErr
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

func (c *fixedEncodeCodec) Encode(any) ([]byte, error) {
	return append([]byte(nil), c.encoded...), nil
}

func (*fixedEncodeCodec) Decode(data []byte, value any) error {
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
		provider)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	entry := configStore.Get()
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
		configStore.Get()
	}
	if got := provider.GetCount(); got != 1 {
		t.Fatalf("memory reads called Provider.Get: count = %d", got)
	}
}

func TestNewRejectsMissingConfiguration(t *testing.T) {
	t.Parallel()

	_, err := sundial.New[testConfig](
		context.Background(),
		providertesting.New(nil))
	if !errors.Is(err, sundial.ErrNotFound) {
		t.Fatalf("New() error = %v, want ErrNotFound", err)
	}
}

func TestNewRejectsInvalidConfiguration(t *testing.T) {
	t.Parallel()

	_, err := sundial.New[testConfig](
		context.Background(),
		providertesting.New([]byte(`{"server":`)))
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
				providertesting.New(
					test.data,
				), test.options...)
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
		providertesting.New([]byte(`{}`)))
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
		provider, sundial.WithLogger[testConfig](logger))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if _, updateErr := configStore.Update(t.Context(), func(config *testConfig) error {
		config.Enabled = true
		return nil
	}); updateErr != nil {
		t.Fatalf("Update() error = %v", updateErr)
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
	if _, updateErr := configStore.Update(t.Context(), func(*testConfig) error { return nil }); updateErr == nil {
		t.Fatal("Update() error = nil, want failure")
	}

	logs := output.String()
	for _, message := range []string{
		`level=DEBUG msg="loaded configuration"`,
		`level=DEBUG msg="update configuration"`,
		`level=DEBUG msg="reloaded configuration"`,
		`level=ERROR msg="reload configuration"`,
		`level=ERROR msg="update configuration"`,
		`error="sundial: update configuration: backend unavailable"`,
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
		providertesting.New([]byte(`{"enabled":`)), sundial.WithLogger[testConfig](logger))
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
		provider, sundial.WithCodec[testConfig](prefixedJSONCodec{prefix: []byte(prefix)}))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	if _, updateErr := configStore.Update(context.Background(), func(config *testConfig) error {
		config.Enabled = false
		return nil
	}); updateErr != nil {
		t.Fatalf("Update() error = %v", updateErr)
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
		provider, sundial.WithCodec[testConfig](yamlcodec.New()))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	entry := configStore.Get()
	config := entry.Value
	if config.Server.Port != 8080 {
		t.Fatalf("Get().Server.Port = %d, want 8080", config.Server.Port)
	}

	if _, updateErr := configStore.Update(context.Background(), func(config *testConfig) error {
		config.Server.Port = 9090
		return nil
	}); updateErr != nil {
		t.Fatalf("Update() error = %v", updateErr)
	}
	if !bytes.Contains(provider.Data(), []byte("port: 9090")) {
		t.Fatalf("saved data = %q, want YAML port 9090", provider.Data())
	}
}

func TestUpdatePersistsCompleteDocument(t *testing.T) {
	t.Parallel()

	provider := providertesting.New([]byte(`{
		"server":{"host":"127.0.0.1","port":8080},
		"enabled":true
	}`))
	configStore, err := sundial.New[testConfig](
		t.Context(),
		provider)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	entry := configStore.Get()
	savedEntry, updateErr := configStore.Update(context.Background(), func(config *testConfig) error {
		config.Server.Port = 9090
		return nil
	})
	if updateErr != nil {
		t.Fatalf("Update() error = %v", updateErr)
	}
	if savedEntry.Revision.ID == entry.Revision.ID {
		t.Fatalf("Update() revision ID = %q, want a new revision ID", savedEntry.Revision.ID)
	}
	currentEntry := configStore.Get()
	if !reflect.DeepEqual(savedEntry, currentEntry) {
		t.Fatalf("Update() entry = %#v, want current Entry %#v", savedEntry, currentEntry)
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
		provider)
	if err != nil {
		t.Fatalf("reload New() error = %v", err)
	}
	reloadedEntry := reloaded.Get()
	if reloadedEntry.Value.Server.Port != 9090 {
		t.Fatalf("reloaded port = %d, want 9090", reloadedEntry.Value.Server.Port)
	}
}

func TestUpdateRejectsEmptyEncodedConfiguration(t *testing.T) {
	t.Parallel()

	for _, encoded := range [][]byte{{}, []byte(" \n\t")} {
		provider := providertesting.New([]byte(`{"server":{"port":8080}}`))
		configStore, err := sundial.New[testConfig](
			t.Context(),
			provider, sundial.WithCodec[testConfig](&fixedEncodeCodec{encoded: encoded}))
		if err != nil {
			t.Fatalf("New() error = %v", err)
		}

		_, updateErr := configStore.Update(context.Background(), func(config *testConfig) error {
			config.Server.Port = 9090
			return nil
		})
		if !errors.Is(updateErr, sundial.ErrEmptyDocument) {
			t.Fatalf("Update() error = %v, want ErrEmptyDocument", updateErr)
		}
		if got := provider.PutIfRevisionCount(); got != 0 {
			t.Fatalf("PutIfRevisionCount() = %d, want 0", got)
		}
		current := configStore.Get()
		if current.Value.Server.Port != 8080 {
			t.Fatalf("port after failed Update = %d, want 8080", current.Value.Server.Port)
		}
	}
}

func TestReloadFailureKeepsPreviousMemory(t *testing.T) {
	t.Parallel()

	provider := providertesting.New([]byte(`{"server":{"port":8080}}`))
	configStore, err := sundial.New[testConfig](
		t.Context(),
		provider)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	provider.SetData([]byte(`{"server":`))
	if reloadErr := configStore.Reload(context.Background()); reloadErr == nil {
		t.Fatal("Reload() error = nil, want decode failure")
	}
	entry := configStore.Get()
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
	entry := configStore.Get()
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
	entry := configStore.Get()
	if entry.Value.Server.Port != 8080 {
		t.Fatalf("port after missing reload = %d, want 8080", entry.Value.Server.Port)
	}
}

func TestAutomaticNativeWatcherReloadsExternalChanges(t *testing.T) {
	t.Parallel()

	provider := providertesting.NewWatcher([]byte(`{"enabled":false}`))
	changed := make(chan sundial.Entry[testConfig], 1)
	_, err := sundial.New[testConfig](
		t.Context(),
		provider, sundial.WithOnChange[testConfig](func(entry sundial.Entry[testConfig]) {
			changed <- entry
		}))
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
		if entry.Revision.ID != "2" {
			t.Fatalf("OnChange() revision ID = %q, want 2", entry.Revision.ID)
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
		provider)
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

	entry := configStore.Get()
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
		provider, sundial.WithOnError[testConfig](func(err error) {
			reported <- err
		}))
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

func TestUpdateRejectsStaleRevisionAcrossInstancesAndAllowsRetry(t *testing.T) {
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

	_, err = firstStore.Update(t.Context(), func(config *testConfig) error {
		config.Counter = 1
		return nil
	})
	require.NoError(t, err)
	change := func(config *testConfig) error {
		config.Enabled = true
		return nil
	}
	_, err = secondStore.Update(t.Context(), change)
	require.ErrorIs(t, err, sundial.ErrConflict)
	require.NoError(t, secondStore.Reload(t.Context()))
	_, err = secondStore.Update(t.Context(), change)
	require.NoError(t, err)

	var saved testConfig
	if decodeErr := json.Unmarshal(provider.Data(), &saved); decodeErr != nil {
		t.Fatalf("decode saved configuration: %v", decodeErr)
	}
	if saved.Counter != 1 || !saved.Enabled {
		t.Fatalf("configuration after retry = %#v, want both changes", saved)
	}
}

func TestReloadTracksChangedRevisionIDWhenContentIsUnchanged(t *testing.T) {
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
	if _, updateErr := configStore.Update(context.Background(), func(config *testConfig) error {
		config.Enabled = false
		return nil
	}); updateErr != nil {
		t.Fatalf("Update() error = %v", updateErr)
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
		provider)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	var group sync.WaitGroup
	for i := range 20 {
		group.Add(2)
		go func(value int) {
			defer group.Done()
			_, updateErr := configStore.Update(t.Context(), func(config *testConfig) error {
				config.Counter = value
				return nil
			})
			assert.NoError(t, updateErr)
		}(i)
		go func() {
			defer group.Done()
			configStore.Get()
		}()
	}
	group.Wait()
}

func TestRevisionOperationsReturnUnsupportedForBasicProvider(t *testing.T) {
	t.Parallel()
	configStore, err := sundial.New[testConfig](
		t.Context(),
		providertesting.New([]byte(`{"enabled":true}`)))
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	_, _, err = configStore.GetRevision(t.Context(), "revision")
	if !errors.Is(err, sundial.ErrUnsupported) {
		t.Fatalf("GetRevision() error = %v, want ErrUnsupported", err)
	}
	_, err = configStore.ListRevisions(t.Context(), sundial.ListRevisionsOptions{})
	if !errors.Is(err, sundial.ErrUnsupported) {
		t.Fatalf("ListRevisions() error = %v, want ErrUnsupported", err)
	}
	_, err = configStore.RestoreRevision(t.Context(), "revision", "current")
	if !errors.Is(err, sundial.ErrUnsupported) {
		t.Fatalf("RestoreRevision() error = %v, want ErrUnsupported", err)
	}
}

// countingCodec makes read-path codec work observable, including accidental encoding.
type countingCodec struct {
	encodes      atomic.Int64
	decodes      atomic.Int64
	failDecode   atomic.Bool
	failEncode   atomic.Bool
	beforeDecode func([]byte)
	afterDecode  func(any)
}

func (c *countingCodec) Encode(value any) ([]byte, error) {
	c.encodes.Add(1)
	if c.failEncode.Load() {
		return nil, errors.New("encode unavailable")
	}
	return json.Marshal(value)
}

func (c *countingCodec) Decode(data []byte, value any) error {
	c.decodes.Add(1)
	if c.failDecode.Load() {
		return errors.New("decode unavailable")
	}
	if c.beforeDecode != nil {
		c.beforeDecode(data)
	}
	if err := json.Unmarshal(data, value); err != nil {
		return err
	}
	if c.afterDecode != nil {
		c.afterDecode(value)
	}
	return nil
}

func TestGetDoesNotDecodeAcceptedConfiguration(t *testing.T) {
	t.Parallel()
	provider := providertesting.New([]byte(`{"counter":1}`))
	documentCodec := &countingCodec{}
	store, err := sundial.New[testConfig](t.Context(), provider, sundial.WithCodec[testConfig](documentCodec))
	require.NoError(t, err)
	for range 10 {
		require.Equal(t, 1, store.Get().Value.Counter)
	}
	assert.Equal(t, 1, provider.GetCount())
	assert.EqualValues(t, 1, documentCodec.decodes.Load())
	assert.Zero(t, documentCodec.encodes.Load())
}

func TestGetSharesReadOnlyReferences(t *testing.T) {
	t.Parallel()
	provider := providertesting.New([]byte(`{"server":{"tags":["api"],"tls":{"enabled":true}},"labels":{"region":"east"}}`))
	documentCodec := &countingCodec{}
	store, err := sundial.New[testConfig](t.Context(), provider, sundial.WithCodec[testConfig](documentCodec))
	require.NoError(t, err)
	first := store.Get()
	second := store.Get()
	assert.Same(t, first.Value.Server.TLS, second.Value.Server.TLS)
	assert.Same(t, &first.Value.Server.Tags[0], &second.Value.Server.Tags[0])
	assert.EqualValues(t, 1, documentCodec.decodes.Load())
	assert.Zero(t, documentCodec.encodes.Load())
}

func TestUpdateUsesEncodedThenDecodedValue(t *testing.T) {
	t.Parallel()
	for _, encoded := range []string{`{"counter":"invalid"}`, `{"counter":2}`} {
		t.Run(encoded, func(t *testing.T) {
			t.Parallel()
			provider := providertesting.New([]byte(`{"counter":0}`))
			store, err := sundial.New[testConfig](
				t.Context(),
				provider,
				sundial.WithCodec[testConfig](&fixedEncodeCodec{encoded: []byte(encoded)}),
			)
			require.NoError(t, err)
			before := store.Get()
			saved, err := store.Update(t.Context(), func(config *testConfig) error {
				config.Counter = 100
				return nil
			})
			if encoded == `{"counter":"invalid"}` {
				require.ErrorContains(t, err, "decode configuration")
				assert.Zero(t, provider.PutIfRevisionCount())
				assert.Equal(t, []byte(`{"counter":0}`), provider.Data())
				assert.Equal(t, before, store.Get())
				return
			}
			require.NoError(t, err)
			assert.Equal(t, 2, saved.Value.Counter)
			assert.Equal(t, saved, store.Get())
			assert.Equal(t, []byte(encoded), provider.Data())
		})
	}
}

func TestConcurrentGetPreservesValueRevisionPair(t *testing.T) {
	t.Parallel()
	provider := providertesting.New([]byte(`{"counter":0,"labels":{"region":"east"}}`))
	store, err := sundial.New[testConfig](t.Context(), provider)
	require.NoError(t, err)
	var group sync.WaitGroup
	for range 8 {
		group.Go(func() {
			for range 100 {
				entry := store.Get()
				assert.Equal(t, strconv.Itoa(entry.Value.Counter+1), entry.Revision.ID)
				assert.Equal(t, "east", entry.Value.Labels["region"])
			}
		})
	}
	for i := range 100 {
		_, updateErr := store.Update(t.Context(), func(config *testConfig) error {
			config.Counter = i + 1
			return nil
		})
		require.NoError(t, updateErr)
	}
	group.Wait()
}

type retryDecodeWatcher struct {
	*providertesting.Provider

	attempts atomic.Int64
}

func (p *retryDecodeWatcher) Watch(ctx context.Context, notify func() error) error {
	if p.attempts.Add(1) == 1 {
		p.SetData([]byte(`{"counter":"invalid"}`))
	}
	if err := notify(); err != nil {
		return err
	}
	<-ctx.Done()
	return ctx.Err()
}

func TestWatchRetriesDecodeFailureAndReportsReadOnlySnapshot(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		provider := &retryDecodeWatcher{Provider: providertesting.New([]byte(`{"counter":0}`))}
		errorsSeen := make(chan error, 2)
		changes := make(chan sundial.Entry[testConfig], 2)
		store, err := sundial.New[testConfig](ctx, provider, sundial.WithOnError[testConfig](func(err error) { errorsSeen <- err }),
			sundial.WithOnChange(func(entry sundial.Entry[testConfig]) {
				changes <- entry
			}))
		require.NoError(t, err)
		before := store.Get()
		synctest.Wait()
		require.Len(t, errorsSeen, 1)
		require.ErrorContains(t, <-errorsSeen, "decode configuration")
		assert.Empty(t, changes)
		assert.Equal(t, before, store.Get())
		provider.SetData([]byte(`{"counter":2,"labels":{"region":"east"},"server":{"tags":["api"],"tls":{"enabled":true}}}`))
		time.Sleep(30 * time.Second)
		synctest.Wait()
		require.EqualValues(t, 2, provider.attempts.Load())
		require.Len(t, changes, 1)
		changed := <-changes
		current := store.Get()
		assert.Equal(t, changed, current)
		assert.Same(t, changed.Value.Server.TLS, current.Value.Server.TLS)
		assert.Equal(t, "3", current.Revision.ID)
		assert.Equal(t, 2, current.Value.Counter)
		assert.Equal(t, "east", current.Value.Labels["region"])
		assert.Equal(t, []string{"api"}, current.Value.Server.Tags)
		assert.True(t, current.Value.Server.TLS.Enabled)
		assert.Empty(t, errorsSeen)
	})
}

func TestOnChangeStillOnlyReportsAutomaticContentChanges(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		provider := providertesting.NewWatcher([]byte(`{"counter":0}`))
		var changes atomic.Int64
		store, err := sundial.New[testConfig](ctx, provider, sundial.WithOnChange(func(sundial.Entry[testConfig]) { changes.Add(1) }))
		require.NoError(t, err)
		synctest.Wait()
		assert.Zero(t, changes.Load())
		provider.Change([]byte(`{"counter":1}`))
		synctest.Wait()
		assert.EqualValues(t, 1, changes.Load())
		stale := store.Get()
		provider.Change([]byte(`{"counter":1}`))
		synctest.Wait()
		assert.EqualValues(t, 1, changes.Load())
		assert.NotEqual(t, stale.Revision, store.Get().Revision)
		_, err = store.Update(ctx, func(*testConfig) error { return nil })
		require.NoError(t, err)
		provider.SetData([]byte(`{"counter":2}`))
		require.NoError(t, store.Reload(ctx))
		synctest.Wait()
		assert.EqualValues(t, 1, changes.Load())
		assert.Equal(t, 2, store.Get().Value.Counter)
	})
}

func TestReloadAndUpdateSerializePublication(t *testing.T) {
	t.Parallel()
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		provider := providertesting.New([]byte(`{"counter":0}`))
		decoding := make(chan struct{})
		release := make(chan struct{})
		var blocked atomic.Bool
		store, err := sundial.New[testConfig](ctx, provider, sundial.WithCodec[testConfig](&countingCodec{beforeDecode: func(data []byte) {
			if string(data) == `{"counter":1}` && blocked.CompareAndSwap(false, true) {
				close(decoding)
				<-release
			}
		}}))
		require.NoError(t, err)
		provider.SetData([]byte(`{"counter":1}`))
		reloaded := make(chan error, 1)
		go func() { reloaded <- store.Reload(ctx) }()
		<-decoding
		updated := make(chan error, 1)
		go func() {
			_, updateErr := store.Update(ctx, func(config *testConfig) error { config.Counter++; return nil })
			updated <- updateErr
		}()
		assert.Zero(t, provider.PutIfRevisionCount())
		assert.Equal(t, "1", store.Get().Revision.ID)
		close(release)
		require.NoError(t, <-reloaded)
		require.NoError(t, <-updated)
		assert.Equal(t, "3", store.Get().Revision.ID)
		assert.Equal(t, 2, store.Get().Value.Counter)
	})
}

func TestUpdateSupportsValueFields(t *testing.T) {
	t.Parallel()
	type name string
	type server struct {
		Port    int  `json:"port"`
		Enabled bool `json:"enabled"`
	}
	type config struct {
		Name    name      `json:"name"`
		Servers [2]server `json:"servers"`
	}
	provider := providertesting.New([]byte(`{"name":"app","servers":[{"port":8080,"enabled":true},{}]}`))
	store, err := sundial.New[config](t.Context(), provider)
	require.NoError(t, err)
	before := store.Get()
	saved, err := store.Update(t.Context(), func(draft *config) error {
		draft.Name = "changed"
		draft.Servers[0].Port = 9090
		assert.Equal(t, before, store.Get())
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, saved, store.Get())
	assert.True(t, store.Get().Value.Servers[0].Enabled)
	assert.Equal(t, 9090, store.Get().Value.Servers[0].Port)
}

func TestNewSupportsScalar(t *testing.T) {
	t.Parallel()
	provider := providertesting.New([]byte(`"hello"`))
	store, err := sundial.New[string](t.Context(), provider)
	require.NoError(t, err)
	assert.Equal(t, "hello", store.Get().Value)
}

func TestNewAcceptsCodecSupportedTypes(t *testing.T) {
	t.Parallel()
	type node struct {
		Next *node `json:"next"`
	}
	type config struct {
		Timestamp time.Time `json:"timestamp"`
		Value     any       `json:"value"`
		Node      *node     `json:"node"`
	}
	provider := providertesting.New([]byte(`{"timestamp":"2026-10-09T00:00:00Z","value":{"enabled":true},"node":{"next":{}}}`))
	store, err := sundial.New[config](t.Context(), provider)
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 10, 9, 0, 0, 0, 0, time.UTC), store.Get().Value.Timestamp)
	wantErr := errors.New("reject change")
	_, err = store.Update(t.Context(), func(draft *config) error {
		value, ok := draft.Value.(map[string]any)
		require.True(t, ok)
		value["enabled"] = false
		draft.Node.Next = nil
		return wantErr
	})
	require.ErrorIs(t, err, wantErr)
	assert.Equal(t, map[string]any{"enabled": true}, store.Get().Value.Value)
	assert.NotNil(t, store.Get().Value.Node.Next)
}

func TestUpdateDetachesNestedReferences(t *testing.T) {
	t.Parallel()
	type item struct {
		Values  []string `json:"values"`
		Enabled *bool    `json:"enabled"`
	}
	type config struct {
		Groups      map[string][2]*item `json:"groups"`
		NilValues   []string            `json:"nil_values"`
		EmptyValues []string            `json:"empty_values"`
		NilMap      map[string]string   `json:"nil_map"`
		EmptyMap    map[string]string   `json:"empty_map"`
	}
	provider := providertesting.New([]byte(`{"groups":{"a":[{"values":["one"],"enabled":true},null]},"empty_values":[],"empty_map":{}}`))
	store, err := sundial.New[config](t.Context(), provider)
	require.NoError(t, err)
	before := store.Get()
	wantErr := errors.New("reject change")
	_, err = store.Update(t.Context(), func(value *config) error {
		value.Groups["a"][0].Values[0] = "changed"
		*value.Groups["a"][0].Enabled = false
		delete(value.Groups, "a")
		return wantErr
	})
	require.ErrorIs(t, err, wantErr)
	assert.Equal(t, before, store.Get())
	assert.Nil(t, before.Value.NilValues)
	assert.NotNil(t, before.Value.EmptyValues)
	assert.Nil(t, before.Value.NilMap)
	assert.NotNil(t, before.Value.EmptyMap)
	assert.Nil(t, before.Value.Groups["a"][1])
	assert.Equal(t, "one", store.Get().Value.Groups["a"][0].Values[0])
	assert.True(t, *store.Get().Value.Groups["a"][0].Enabled)
}

func TestUpdateSupportsNamedPointerType(t *testing.T) {
	t.Parallel()
	type values []string
	type namedPointer *values
	type config struct {
		Values namedPointer `json:"values"`
	}
	provider := providertesting.New([]byte(`{"values":["original"]}`))
	store, err := sundial.New[config](t.Context(), provider)
	require.NoError(t, err)
	wantErr := errors.New("reject change")
	_, err = store.Update(t.Context(), func(value *config) error {
		(*value.Values)[0] = "changed"
		return wantErr
	})
	require.ErrorIs(t, err, wantErr)
	assert.Equal(t, "original", (*store.Get().Value.Values)[0])
}

// BenchmarkGetVsJSONDecode compares shared typed reads with parsing the same
// cached document on each read. Provider I/O and initialization are excluded.
func BenchmarkGetVsJSONDecode(b *testing.B) {
	b.Run("Small", func(b *testing.B) {
		benchmarkTypedReads[testConfig](b, []byte(`{
 "server":{"host":"localhost","port":8080,"tags":["api","worker"],"tls":{"enabled":true}},
 "enabled":true,"counter":1,"labels":{"region":"east"}
}`))
	})
	b.Run("MapAndSlice", func(b *testing.B) {
		type rule struct {
			Limit int      `json:"limit"`
			Tags  []string `json:"tags"`
		}
		type config struct {
			Enabled    bool              `json:"enabled"`
			Operations map[string][]rule `json:"operations"`
		}
		value := config{Enabled: true, Operations: make(map[string][]rule, 64)}
		for i := range 64 {
			rules := make([]rule, 8)
			for j := range rules {
				rules[j] = rule{Limit: j + 100, Tags: []string{"api", "user", "region", "default"}}
			}
			value.Operations["operation-"+strconv.Itoa(i)] = rules
		}
		data, err := json.Marshal(value)
		if err != nil {
			b.Fatal(err)
		}
		benchmarkTypedReads[config](b, data)
	})
}

func benchmarkTypedReads[T any](b *testing.B, data []byte) {
	b.Helper()
	b.Run("Get", func(b *testing.B) {
		store, err := sundial.New[T](b.Context(), providertesting.New(data))
		if err != nil {
			b.Fatal(err)
		}
		b.ReportAllocs()
		for b.Loop() {
			store.Get()
		}
	})
	b.Run("JSONDecode", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			var value T
			if err := json.Unmarshal(data, &value); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func TestUpdateCallbackFailureKeepsAcceptedSnapshot(t *testing.T) {
	t.Parallel()
	provider := providertesting.New([]byte(`{"server":{"tags":["api"],"tls":{"enabled":true}},"labels":{"region":"east"}}`))
	store, err := sundial.New[testConfig](t.Context(), provider)
	require.NoError(t, err)
	before := store.Get()
	beforeData := provider.Data()
	wantErr := errors.New("validation rejected")
	_, err = store.Update(t.Context(), func(draft *testConfig) error {
		draft.Server.Tags[0] = "changed"
		draft.Server.TLS.Enabled = false
		draft.Labels["region"] = "west"
		return wantErr
	})
	require.ErrorIs(t, err, wantErr)
	assert.Equal(t, before, store.Get())
	assert.Equal(t, "api", store.Get().Value.Server.Tags[0])
	assert.True(t, store.Get().Value.Server.TLS.Enabled)
	assert.Equal(t, "east", store.Get().Value.Labels["region"])
	assert.Equal(t, beforeData, provider.Data())
	assert.Zero(t, provider.PutIfRevisionCount())
}

func TestUpdateWriteFailureKeepsAcceptedSnapshot(t *testing.T) {
	t.Parallel()
	provider := providertesting.New([]byte(`{"counter":1,"labels":{"region":"east"}}`))
	store, err := sundial.New[testConfig](t.Context(), provider)
	require.NoError(t, err)
	before := store.Get()
	beforeData := provider.Data()
	wantErr := errors.New("backend unavailable")
	provider.SetPutIfRevisionError(wantErr)
	_, err = store.Update(t.Context(), func(draft *testConfig) error {
		draft.Counter++
		draft.Labels["region"] = "west"
		return nil
	})
	require.ErrorIs(t, err, wantErr)
	assert.Equal(t, before, store.Get())
	assert.Equal(t, "east", store.Get().Value.Labels["region"])
	assert.Equal(t, beforeData, provider.Data())
}

func TestUpdatePublicationDecodeFailureKeepsSnapshot(t *testing.T) {
	t.Parallel()
	provider := providertesting.New([]byte(`{"counter":1}`))
	documentCodec := &countingCodec{}
	store, err := sundial.New[testConfig](
		t.Context(),
		provider, sundial.WithCodec[testConfig](documentCodec),
	)
	require.NoError(t, err)
	before := store.Get()
	called := false
	_, err = store.Update(t.Context(), func(*testConfig) error {
		called = true
		documentCodec.failDecode.Store(true)
		return nil
	})
	require.ErrorContains(t, err, "decode unavailable")
	assert.True(t, called)
	assert.Equal(t, before, store.Get())
	assert.Zero(t, provider.PutIfRevisionCount())
}

func TestConcurrentUpdatesDoNotLoseChanges(t *testing.T) {
	t.Parallel()
	provider := providertesting.New([]byte(`{"counter":0}`))
	store, err := sundial.New[testConfig](t.Context(), provider)
	require.NoError(t, err)
	var group sync.WaitGroup
	for range 8 {
		group.Go(func() {
			for range 20 {
				_, updateErr := store.Update(t.Context(), func(draft *testConfig) error {
					draft.Counter++
					return nil
				})
				assert.NoError(t, updateErr)
			}
		})
	}
	group.Wait()
	assert.Equal(t, 160, store.Get().Value.Counter)
	assert.Equal(t, 160, provider.PutIfRevisionCount())
	var persisted testConfig
	require.NoError(t, json.Unmarshal(provider.Data(), &persisted))
	assert.Equal(t, 160, persisted.Counter)
}

func TestUpdateDetachesRetainedCallbackDraft(t *testing.T) {
	t.Parallel()
	provider := providertesting.New([]byte(`{"server":{"tags":["api"],"tls":{"enabled":true}},"labels":{"region":"east"}}`))
	store, err := sundial.New[testConfig](t.Context(), provider)
	require.NoError(t, err)
	var retained *testConfig
	saved, err := store.Update(t.Context(), func(draft *testConfig) error {
		retained = draft
		draft.Server.Port = 9090
		return nil
	})
	require.NoError(t, err)
	require.NotNil(t, retained)
	retained.Server.Tags[0] = "mutated"
	retained.Server.TLS.Enabled = false
	retained.Labels["region"] = "mutated"
	retained.Server.Port = 10000
	assert.Equal(t, saved, store.Get())
	assert.Equal(t, 9090, saved.Value.Server.Port)
	assert.Equal(t, "api", saved.Value.Server.Tags[0])
	assert.True(t, saved.Value.Server.TLS.Enabled)
	assert.Equal(t, "east", saved.Value.Labels["region"])
	var persisted testConfig
	require.NoError(t, json.Unmarshal(provider.Data(), &persisted))
	assert.Equal(t, saved.Value, persisted)
}

func TestUpdateAcrossInstancesRejectsStaleSnapshot(t *testing.T) {
	t.Parallel()
	provider := providertesting.New([]byte(`{"counter":0}`))
	first, err := sundial.New[testConfig](t.Context(), provider)
	require.NoError(t, err)
	second, err := sundial.New[testConfig](t.Context(), provider)
	require.NoError(t, err)
	before := second.Get()
	increment := func(draft *testConfig) error {
		draft.Counter++
		return nil
	}
	saved, err := first.Update(t.Context(), increment)
	require.NoError(t, err)
	_, err = second.Update(t.Context(), increment)
	require.ErrorIs(t, err, sundial.ErrConflict)
	assert.Equal(t, before, second.Get())
	var persisted testConfig
	require.NoError(t, json.Unmarshal(provider.Data(), &persisted))
	assert.Equal(t, saved.Value, persisted)
	require.NoError(t, second.Reload(t.Context()))
	saved, err = second.Update(t.Context(), increment)
	require.NoError(t, err)
	assert.Equal(t, 2, saved.Value.Counter)
}

func TestUpdateCodecFailuresKeepSnapshot(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"encode", "decode"} {
		t.Run(operation, func(t *testing.T) {
			t.Parallel()
			provider := providertesting.New([]byte(`{"counter":1,"labels":{"region":"east"}}`))
			documentCodec := &countingCodec{}
			store, err := sundial.New[testConfig](t.Context(), provider, sundial.WithCodec[testConfig](documentCodec))
			require.NoError(t, err)
			before := store.Get()
			beforeData := provider.Data()
			documentCodec.failEncode.Store(operation == "encode")
			documentCodec.failDecode.Store(operation == "decode")
			called := false
			_, err = store.Update(t.Context(), func(*testConfig) error { called = true; return nil })
			require.ErrorContains(t, err, operation+" unavailable")
			assert.Equal(t, operation == "encode", called)
			assert.Equal(t, before, store.Get())
			assert.Equal(t, beforeData, provider.Data())
			assert.Zero(t, provider.PutIfRevisionCount())
		})
	}
}

func TestUpdateDecodesCachedDocument(t *testing.T) {
	t.Parallel()
	var decodes int
	documentCodec := &countingCodec{afterDecode: func(value any) {
		decodes++
		if decodes == 1 {
			config, ok := value.(*testConfig)
			require.True(t, ok)
			config.Counter = 42
		}
	}}
	provider := providertesting.New([]byte(`{"counter":1}`))
	store, err := sundial.New[testConfig](t.Context(), provider, sundial.WithCodec[testConfig](documentCodec))
	require.NoError(t, err)
	before := store.Get()
	require.Equal(t, 42, before.Value.Counter)
	saved, err := store.Update(t.Context(), func(value *testConfig) error {
		assert.Equal(t, 1, value.Counter)
		value.Counter++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, 2, saved.Value.Counter)
	assert.Equal(t, 3, decodes)
	assert.EqualValues(t, 1, documentCodec.encodes.Load())
}

func TestUpdateUsesLatestCachedDocumentAndRevision(t *testing.T) {
	t.Parallel()
	provider := providertesting.New([]byte(`{"counter":1}`))
	documentCodec := &countingCodec{}
	store, err := sundial.New[testConfig](t.Context(), provider, sundial.WithCodec[testConfig](documentCodec))
	require.NoError(t, err)
	for _, wantBefore := range []int{1, 2} {
		previous := store.Get()
		saved, updateErr := store.Update(t.Context(), func(value *testConfig) error {
			assert.Equal(t, wantBefore, value.Counter)
			value.Counter++
			return nil
		})
		require.NoError(t, updateErr)
		assert.Equal(t, previous.Revision.ID, saved.Revision.ParentID)
		assert.Equal(t, wantBefore+1, saved.Value.Counter)
	}
	provider.SetData([]byte(`{"counter":10}`))
	require.NoError(t, store.Reload(t.Context()))
	previous := store.Get()
	saved, err := store.Update(t.Context(), func(value *testConfig) error {
		assert.Equal(t, 10, value.Counter)
		value.Counter++
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, previous.Revision.ID, saved.Revision.ParentID)
	assert.Equal(t, 11, saved.Value.Counter)
	assert.EqualValues(t, 3, documentCodec.encodes.Load())
	assert.EqualValues(t, 8, documentCodec.decodes.Load())
	assert.Equal(t, 2, provider.GetCount())
}
