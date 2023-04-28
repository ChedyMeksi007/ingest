package plugin

import (
	"net/url"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/storage"
)

type instrumentedPluginSourceRPC struct {
	*pluginSourceRPC
	cv *prometheus.CounterVec
}

type instrumentedPluginDestinationRPC struct {
	*pluginDestinationRPC
	cv *prometheus.CounterVec
}

func newInstrumentedPLuginSourceRPC(serv *pluginSourceRPC, reg prometheus.Registerer) *instrumentedPLuginSourceRPC {
	cv := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "plugin_rpc_calls_total",
		Help: "The total number of rpc calls",
	}, []string{"rpc_method", "result"})
	reg.MustRegister(cv)

	return &instrumentedPluginSourceRPC{pluginSourceRPCServer: serv, cv: cv}
}

func newInstrumentedPLuginDestinationRPC(serv *pluginDestinationRPC, reg prometheus.Registerer) *instrumentedPluginDestinationRPC {
	cv := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "plugin_rpc_calls_total",
		Help: "The total number of rpc calls",
	}, []string{"rpc_method", "result"})
	reg.MustRegister(cv)
	return &instrumentedPluginDestinationRPCServer{pluginDestinationRPCServer: serv, cv: cv}
}

//instrumentedPLuginSourceRPC
func (so *instrumentedPLuginSourceRPC) Gather() (resp []*dto.MetricFamily, err error) {
	resp, err = so.plugininSourceRPC.Gather()
	if err != nil {
		so.cv.With(prometheus.Labels{"rpc_method": "Gather", "result": "failed"}).Inc()
		return resp, err
	}
	so.cv.With(prometheus.Labels{"rpc_method": "Gather", "result": "success"}).Inc()
	return resp, nil
}

func (so *instrumentedPluginSourceRPC) CleanUp(ctx context.Context, s ingest.Codec) error {
	err := so.plugininSourceRPC.CleanUp(ctx,s)
	if err != nil {
		so.cv.With(prometheus.Labels{"rpc_method": "CLeanUp", "result": "failed"}).Inc()
		return err
	}
	so.cv.With(prometheus.Labels{"rpc_method": "CleanUp", "result": "success"}).Inc()
	return nil
}

func (so *instrumentedPluginSourceRPC) Configure(conf map[string]any) error {
	err := so.pluginSourceRPC.Configure(conf)
	if err != nil {
		so.cv.With(prometheus.Labels{"rpc_method": "Configure", "result": "failed"}).Inc()
		return err
	}
	so.cv.With(prometheus.Labels{"rpc_method": "Configure", "result": "success"}).Inc()
	return nil
}

func (so *instrumentedPluginSourceRPC) Download(ctx context.Context, s ingest.Codec) (*ingest.Object, error) {
	obj, err := so.plugininSourceRPC.Download(ctx,s)
	if err != nil {
		so.cv.With(prometheus.Labels{"rpc_method": "Download", "result": "failed"}).Inc()
		return nil, err
	}
	so.cv.With(prometheus.Labels{"rpc_method": "Download", "result": "success"}).Inc()
	return obj, nil
}

func (so *instrumentedPluginSourceRPC) Next(ctx context.Context) (*ingest.Codec, error) {
	resp, err := so.plugininSourceRPC.Next(ctx)
	if err != nil {
		so.cv.With(prometheus.Labels{"rpc_method": "Next", "result": "failed"}).Inc()
		return nil, err
	}
	so.cv.With(prometheus.Labels{"rpc_method": "Next", "result": "success"}).Inc()
	return &resp, nil
}

func (so *instrumentedPluginSourceRPC) Reset(ctx context.Context) error {
	err := so.plugininSourceRPC.Reset(ctx)
	if err != nil {
		so.cv.With(prometheus.Labels{"rpc_method": "Reset", "result": "failed"}).Inc()
		return err
	}
	so.cv.With(prometheus.Labels{"rpc_method": "Reset", "result": "success"})
	return nil
}

//instrumentedPLuginDestinationRPC
func (d *instrumentedPluginDestinationRPC) Gather() (resp []*dto.MetricFamily, err error) {
	resp, err = d.pluginDestinationRPC.Gather()
	if err != nil {
		d.cv.With(prometheus.Labels{"rpc_method": "Gather", "result": "failed"}).Inc()
		return resp, err
	}
	d.cv.With(prometheus.Labels{"rpc_method": "Gather", "result": "success"}).Inc()
	return resp, nil
}

func (d *instrumentedPluginDestinationRPC) Configure(conf map[string]any) error {
	err := d.pluginDestinationRPC.Configure(conf)
	if err != nil {
		d.cv.With(prometheus.Labels{"rpc_method": "Configure", "result": "failed"}).Inc()
		return err
	}
	d.cv.With(prometheus.Labels{"rpc-method": "Configure", "result": "success"}).Inc()
	return nil
}

func (d *instrumentedPluginDestinationRPC) Stat(ctx context.Context, s ingest.Codec) (*storage.ObjectInfo, error) {
	resp, err := d.pluginDestinationRPC.Stat(ctx,s)
	if err != nil {
		d.cv.With(prometheus.Labels{"rpc_method": "Stat", "result": "failed"}).Inc()
		return nil, err
	}
	d.cv.With(prometheus.Labels{"rpc_method": "Stat", "result": "success"}).Inc()
	return &resp, nil
}

func (d *instrumentedPluginDestinationRPC) Store(ctx context.Context, s ingest.Codec, obj ingest.Object) (*url.URL, error) {
	resp, err := d.pluginDestinationRPC.Store(ctx,s)
	if err != nil {
		d.cv.With(prometheus.Labels{"rpc_method": "Store", "result": "success"}).Inc()
		return nil, err
	}
	d.cv.With(prometheus.Labels{"rpc_method": "Store", "result": "success"}).Inc()
	return &resp, nil
}
