package drive

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net/url"
	"os"
	"strings"

	hclog "github.com/hashicorp/go-hclog"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"google.golang.org/api/drive/v3"

	"github.com/connylabs/ingest"
	"github.com/connylabs/ingest/storage"
)

type driveStorage struct {
	s     *drive.Service
	l     hclog.Logger
	p     string
	gdcot *prometheus.CounterVec
}

// New returns a new Storage that can store objects to Google Drive.
func New(folder string, service *drive.Service, l hclog.Logger, r prometheus.Registerer) (storage.Storage, error) {
	parts := strings.Split(folder, "/")
	if len(parts) < 1 {
		return nil, errors.New("no folder was specified")
	}
	ds := &driveStorage{s: service, l: l}
	f, err := ds.find(context.Background(), "", parts)
	if err != nil {
		return nil, fmt.Errorf("failed to find folder: %w", err)
	}
	ds.p = f.Id
	ds.gdcot = promauto.With(r).NewCounterVec(prometheus.CounterOpts{
		Name: "ingest_google_drive_client_operations_total",
		Help: "The number of operations performed by the Google Drive client.",
	}, []string{"operation", "result"})

	for _, o := range []string{"find", "list", "create"} {
		for _, r := range []string{"error", "success"} {
			ds.gdcot.WithLabelValues(o, r).Add(0)
		}
	}

	return ds, nil
}

func (ds *driveStorage) Stat(ctx context.Context, element ingest.Codec) (*storage.ObjectInfo, error) {
	f, err := ds.find(ctx, ds.p, []string{element.Name})
	if err != nil {
		return nil, err
	}

	return &storage.ObjectInfo{URI: f.Id}, nil
}

// find is a helper that will recursively look for a file matching the given hierarchy.
func (ds *driveStorage) find(ctx context.Context, parent string, parts []string) (*drive.File, error) {
	query := fmt.Sprintf("name = '%s' and trashed=false", parts[0])
	if parent != "" {
		query += fmt.Sprintf(" and '%s' in parents", parent)
	}
	fileList, err := ds.s.Files.List().IncludeItemsFromAllDrives(true).SupportsAllDrives(true).Fields("files(id,parents)").Context(ctx).Q(query).Do()
	if err != nil {
		ds.gdcot.WithLabelValues("list", "error").Inc()
		return nil, err
	}
	ds.gdcot.WithLabelValues("list", "success").Inc()
	for i := range fileList.Files {
		if (parent == "") != (len(fileList.Files[i].Parents) == 0) {
			continue
		}
		if len(parts) == 1 {
			return fileList.Files[i], nil
		}
		f, err := ds.find(ctx, fileList.Files[i].Id, parts[1:])
		if os.IsNotExist(err) {
			ds.gdcot.WithLabelValues("find", "success").Inc()
			continue
		}
		if err != nil {
			ds.gdcot.WithLabelValues("find", "error").Inc()
			return nil, err
		}
		ds.gdcot.WithLabelValues("find", "success").Inc()
		return f, nil
	}
	return nil, fs.ErrNotExist
}

func (ds *driveStorage) Store(ctx context.Context, element ingest.Codec, obj ingest.Object) (*url.URL, error) {
	file := &drive.File{
		Name:    element.Name,
		Parents: []string{ds.p},
	}

	f, err := ds.s.Files.Create(file).Media(obj.Reader).SupportsAllDrives(true).Context(ctx).Do()
	if err != nil {
		ds.gdcot.WithLabelValues("create", "error").Inc()
		return nil, err
	}
	ds.gdcot.WithLabelValues("create", "success").Inc()

	return url.Parse(fmt.Sprintf("https://drive.google.com/file/d/%s", f.Id))
}
