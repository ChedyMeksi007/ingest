package plugin

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	hclog "github.com/hashicorp/go-hclog"
	"github.com/hashicorp/go-multierror"
	hplugin "github.com/hashicorp/go-plugin"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func NewPluginManager(i time.Duration, l log.Logger) *PluginManager {
	if l == nil {
		l = log.NewNopLogger()
	}

	return &PluginManager{
		Interval: i,
		l:        l,
		reg:      prometheus.NewRegistry(),
	}
}

// PluginManager can start new plugins watch and kill all plugins.
type PluginManager struct {
	Interval     time.Duration
	reg          *prometheus.Registry
	sources      []withClient[Source]
	destinations []withClient[Destination]
	l            log.Logger
	m            sync.Mutex
}

func ptr[T any](t T) *T {
	return &t
}

func (pm *PluginManager) Gather() ([]*dto.MetricFamily, error) {
	g := multierror.Group{}
	pm.m.Lock()
	defer pm.m.Unlock()
	m, err := pm.reg.Gather()
	if err != nil {
		return nil, err
	}

	all := make([][]*dto.MetricFamily, len(pm.sources)+len(pm.destinations))
	for i := range pm.sources {
		i := i
		g.Go(func() error {
			g, ok := pm.sources[i].t.(prometheus.Gatherer)
			if !ok {
				return errors.New("failed to cast")
			}
			mfs, err := g.Gather()
			if err != nil {
				level.Error(pm.l).Log("msg", "failed to gather metrics for plugin", "err", err.Error(), "path", pm.sources[i].path, "mode", "source")
				return nil
			}
			for _, mf := range mfs {
				for _, m := range mf.Metric {
					for k, v := range pm.sources[i].labels {
						m.Label = append(m.Label, &dto.LabelPair{Name: ptr(k), Value: ptr(v)})
					}
				}
			}
			all[i] = mfs
			return nil
		})
	}
	for i := range pm.destinations {
		i := i
		g.Go(func() error {
			g, ok := pm.destinations[i].t.(prometheus.Gatherer)
			if !ok {
				return errors.New("failed to cast")
			}

			mfs, err := g.Gather()
			if err != nil {
				level.Error(pm.l).Log("msg", "failed to gather metrics for plugin", "err", err.Error(), "path", pm.destinations[i].path, "mode", "destination")
				return nil

			}
			for _, mf := range mfs {
				for _, m := range mf.Metric {
					for k, v := range pm.destinations[i].labels {
						m.Label = append(m.Label, &dto.LabelPair{Name: ptr(k), Value: ptr(v)})
					}
				}
			}
			all[i+len(pm.sources)] = mfs
			return nil
		})
	}

	if err := g.Wait().ErrorOrNil(); err != nil {
		return nil, err
	}

	merged := make([]*dto.MetricFamily, 0)
	for _, m := range all {
		merged = append(merged, m...)
	}
	return append(merged, m...), g.Wait().ErrorOrNil()
}

// NewDestination returns a new Destination interface from a plugin path and configuration.
func (pm *PluginManager) NewDestination(path string, config map[string]any, labels prometheus.Labels) (Destination, error) {
	pm.m.Lock()
	defer pm.m.Unlock()

	c := client(path, pm.reg)
	cp, err := c.Client()
	if err != nil {
		c.Kill()
		return nil, fmt.Errorf("failed to create rpc client interface: %w", err)
	}
	d, err := newDestination(cp)
	if err != nil {
		c.Kill()
		return nil, err
	}

	if err := d.Configure(config); err != nil {
		c.Kill()
		return nil, fmt.Errorf("failed to configure destination: %w", err)
	}

	pm.destinations = append(pm.destinations, withClient[Destination]{t: d, c: c, path: path, config: config, labels: labels})

	return d, nil
}

// NewSource returns a new Source interface from a plugin path and configuration.
func (pm *PluginManager) NewSource(path string, config map[string]any, labels prometheus.Labels) (Source, error) {
	pm.m.Lock()
	defer pm.m.Unlock()

	c := client(path, prometheus.WrapRegistererWith(labels, pm.reg))
	cp, err := c.Client()
	if err != nil {
		c.Kill()
		return nil, fmt.Errorf("failed to create rpc client interface: %w", err)
	}
	s, err := newSource(cp)
	if err != nil {
		c.Kill()
		return nil, err
	}
	if err := s.Configure(config); err != nil {
		c.Kill()
		return nil, fmt.Errorf("failed to configure source: %w", err)
	}
	pm.sources = append(pm.sources, withClient[Source]{t: s, c: c, path: path, config: config, labels: labels})
	return s, nil
}

// Stop will block until all rpc clients are closed.
// After Stop was called all currently managed plugins cannot be used anymore.
func (pm *PluginManager) Stop() {
	pm.m.Lock()
	defer pm.m.Unlock()

	g := &multierror.Group{}
	f := func(c *hplugin.Client) func() error {
		return func() error {
			c.Kill()
			return nil
		}
	}
	for _, c := range pm.sources {
		g.Go(f(c.c))
	}
	for _, c := range pm.destinations {
		g.Go(f(c.c))
	}
	if err := g.Wait().ErrorOrNil(); err != nil {
		// We can panic here because none of the go routines in the group return errors.
		panic(err)
	}
	pm.destinations = nil
	pm.sources = nil
}

// Watch will return an error when a plugin can not be pinged anymore or return when ctx is done.
func (pm *PluginManager) Watch(ctx context.Context) error {
	t := time.NewTicker(pm.Interval)
	for {
		select {
		case <-ctx.Done():
			return nil
		case start := <-t.C:
			g := multierror.Group{}
			for i := range pm.sources {
				i := i
				g.Go(func() error {
					cp, err := pm.sources[i].c.Client()
					if err != nil {
						return fmt.Errorf("source client not initialized: %w", err)
					}
					if err := cp.Ping(); err != nil {
						return fmt.Errorf("failed to ping source: %w", err)
					}
					return nil
				})
			}
			for i := range pm.destinations {
				i := i
				g.Go(func() error {
					cp, err := pm.destinations[i].c.Client()
					if err != nil {
						return fmt.Errorf("destination client not initialized: %w", err)
					}
					if err := cp.Ping(); err != nil {
						return fmt.Errorf("failed to ping destination: %w", err)
					}
					return nil
				})
			}
			level.Debug(pm.l).Log("msg", "successfully pinged all plugins", "duration", time.Since(start), "source plugins", len(pm.sources), "destination plugins", len(pm.destinations))

			err := g.Wait().ErrorOrNil()
			if err != nil {
				return err
			}

		}
	}
}

func client(path string, reg prometheus.Registerer) *hplugin.Client {
	handshakeConfig := hplugin.HandshakeConfig{
		ProtocolVersion:  PluginMagicProtocalVersion,
		MagicCookieKey:   PluginMagicCookieKey,
		MagicCookieValue: PluginCookieValue,
	}

	logger := hclog.New(&hclog.LoggerOptions{
		Name:       "plugin",
		JSONFormat: true,
		Output:     os.Stdout,
		Level:      hclog.Debug,
	})

	pluginMap := map[string]hplugin.Plugin{
		"destination": &pluginDestination{r: prometheus.WrapRegistererWith(prometheus.Labels{"todo": "dest", "path": path}, reg)},
		"source":      &pluginSource{r: prometheus.WrapRegistererWith(prometheus.Labels{"todo": "source", "path": path}, reg)},
	}

	return hplugin.NewClient(&hplugin.ClientConfig{
		HandshakeConfig: handshakeConfig,
		Plugins:         pluginMap,
		Cmd:             exec.Command(path),
		Logger:          logger.With("path", path),
		AutoMTLS:        true,
		Managed:         true,
	})
}

type withClient[T any] struct {
	t      T
	path   string
	c      *hplugin.Client
	config map[string]any
	labels prometheus.Labels
}

func newDestination(cp hplugin.ClientProtocol) (Destination, error) {
	raw, err := cp.Dispense("destination")
	if err != nil {
		return nil, fmt.Errorf("failed to dispense destination: %w", err)
	}
	return raw.(Destination), nil
}

func newSource(cp hplugin.ClientProtocol) (Source, error) {
	raw, err := cp.Dispense("source")
	if err != nil {
		return nil, fmt.Errorf("failed to dispense source: %w", err)
	}
	return raw.(Source), nil
}
