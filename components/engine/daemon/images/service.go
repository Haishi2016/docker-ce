package images // import "github.com/docker/docker/daemon/images"

import (
	"context"
	"os"
	"strings"

	"github.com/docker/docker/container"
	daemonevents "github.com/docker/docker/daemon/events"
	"github.com/docker/docker/distribution/metadata"
	"github.com/docker/docker/distribution/xfer"
	"github.com/docker/docker/image"
	"github.com/docker/docker/layer"
	dockerreference "github.com/docker/docker/reference"
	"github.com/docker/docker/registry"
	"github.com/docker/libtrust"
	"github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

type containerStore interface {
	// used by image delete
	First(container.StoreFilter) *container.Container
	// used by image prune, and image list
	List() []*container.Container
	// TODO: remove, only used for CommitBuildStep
	Get(string) *container.Container
}

// ImageServiceConfig is the configuration used to create a new ImageService
type ImageServiceConfig struct {
	ContainerStore            containerStore
	DistributionMetadataStore metadata.Store
	EventsService             *daemonevents.Events
	ImageStore                image.Store
	LayerStores               map[string]layer.Store
	MaxConcurrentDownloads    int
	MaxConcurrentUploads      int
	ReferenceStore            dockerreference.Store
	RegistryService           registry.Service
	TrustKey                  libtrust.PrivateKey
}

// NewImageService returns a new ImageService from a configuration
func NewImageService(config ImageServiceConfig) *ImageService {
	logrus.Debugf("Max Concurrent Downloads: %d", config.MaxConcurrentDownloads)
	logrus.Debugf("Max Concurrent Uploads: %d", config.MaxConcurrentUploads)
	return &ImageService{
		containers:                config.ContainerStore,
		distributionMetadataStore: config.DistributionMetadataStore,
		downloadManager:           xfer.NewLayerDownloadManager(config.LayerStores, config.MaxConcurrentDownloads),
		eventsService:             config.EventsService,
		imageStore:                config.ImageStore,
		layerStores:               config.LayerStores,
		referenceStore:            config.ReferenceStore,
		registryService:           config.RegistryService,
		trustKey:                  config.TrustKey,
		uploadManager:             xfer.NewLayerUploadManager(config.MaxConcurrentUploads),
	}
}

// ImageService provides a backend for image management
type ImageService struct {
	containers                containerStore
	distributionMetadataStore metadata.Store
	downloadManager           *xfer.LayerDownloadManager
	eventsService             *daemonevents.Events
	imageStore                image.Store
	layerStores               map[string]layer.Store // By operating system
	pruneRunning              int32
	referenceStore            dockerreference.Store
	registryService           registry.Service
	trustKey                  libtrust.PrivateKey
	uploadManager             *xfer.LayerUploadManager
}

// CountImages returns the number of images stored by ImageService
// called from info.go
func (i *ImageService) CountImages() int {
	return i.imageStore.Len()
}

// Children returns the children image.IDs for a parent image.
// called from list.go to filter containers
// TODO: refactor to expose an ancestry for image.ID?
func (i *ImageService) Children(id image.ID) []image.ID {
	return i.imageStore.Children(id)
}

func (i *ImageService) GetPatches(id image.ID) ([]string, error) {
	return i.imageStore.GetPatches(id)
}

func (i *ImageService) applyPatches(image *image.Image, patches []string, containerOS string) error {
	if len(patches) > 0 {
		patchMap :=make(map[string][]layer.DiffID)
		chainLookUpMap := make(map[string]layer.ChainID)
		var missedPatches []string
		for _,p := range patches {
			if p != "" {
				_, err := i.GetImage(p)
				if err != nil {
					missedPatches = append(missedPatches, p)
				}
			}
		}
		if len(missedPatches) > 0 {
			return errors.Errorf("Missing patches: %s", strings.Join(missedPatches,","))
		}
		for _, p := range patches {
			if p != "" {
				pImg, err := i.GetImage(p)
				if err != nil {
					return err
				}
				lastMissmatch := 0
				for i,_ := range image.RootFS.DiffIDs {
					lastMissmatch = i
					if lastMissmatch >= len(pImg.RootFS.DiffIDs) {
						break
					}
					if string(image.RootFS.DiffIDs[i]) != string(pImg.RootFS.DiffIDs[i]) {
						break
					}
				}
				if lastMissmatch > 0 && lastMissmatch < len(pImg.RootFS.DiffIDs) {
					key := string(image.RootFS.DiffIDs[lastMissmatch-1])
					_, ok := patchMap[key]
					if ok {
						patchMap[key] = append(patchMap[key], pImg.RootFS.DiffIDs[lastMissmatch:]...)
					} else {
						patchMap[key] = pImg.RootFS.DiffIDs[lastMissmatch:]
					}
				} else {
					return errors.Errorf("Can't apply patch %s", p)
				}
				for i, p := range pImg.RootFS.DiffIDs[lastMissmatch:] {
					chainLookUpMap[string(p)] = layer.CreateChainID(pImg.RootFS.DiffIDs[:lastMissmatch+i+1])
				}
			}
		}
		index := 0
		for i, l := range image.RootFS.DiffIDs {
			chainLookUpMap[string(l)] = layer.CreateChainID(image.RootFS.DiffIDs[:i+1])
		}
		for index < len(image.RootFS.DiffIDs) {
			mItem, ok := patchMap[string(image.RootFS.DiffIDs[index])]
			if ok {
				tmp := make([]layer.DiffID, index+1)
				copy(tmp, image.RootFS.DiffIDs[:index+1])
				tmp2 := make([]layer.DiffID, len(image.RootFS.DiffIDs)-index-1)
				copy(tmp2, image.RootFS.DiffIDs[index+1:])
				tmp = append(tmp, mItem...)
				image.RootFS.DiffIDs = append(tmp, tmp2...)
				index += len(mItem) + 1
			} else {
				index++
			}
		}
		index = 0
		var newIds []layer.DiffID
		for index < len(image.RootFS.DiffIDs) {
			newIds = append(newIds, image.RootFS.DiffIDs[index])
			updatedChainID := layer.CreateChainID(newIds)
			_, err := i.layerStores[containerOS].Get(updatedChainID)
			if err != nil {
				goodChainID, ok := chainLookUpMap[string(image.RootFS.DiffIDs[index])]
				if !ok {
					return err
				}
				tl, err := i.layerStores[containerOS].Get(goodChainID)
				if err == nil {
					ts, err := tl.TarStream()
					if err != nil {
						return err
					}
					defer ts.Close()
					nl, err := i.layerStores[containerOS].Register(ts, layer.CreateChainID(newIds[:len(newIds)-1]))
					if err != nil {
						return err
					} else {
						newIds = append(newIds[:len(newIds)-1], nl.DiffID())
					}
				} else {
					return err
				}
			}
			index++
		}
		copy(image.RootFS.DiffIDs, newIds)

	}
	return nil
}

// CreateLayer creates a filesystem layer for a container.
// called from create.go
// TODO: accept an opt struct instead of container?
func (i *ImageService) CreateLayer(container *container.Container, initFunc layer.MountInit) (layer.RWLayer, error) {
	var layerID layer.ChainID
	if container.ImageID != "" {
		img, err := i.imageStore.Get(container.ImageID)
		if err != nil {
			return nil, err
		}

		err = i.applyPatches(img, container.Patches, container.OS)

		if err != nil {
			return nil, err
		}

		layerID = img.RootFS.ChainID()

	}

	rwLayerOpts := &layer.CreateRWLayerOpts{
		MountLabel: container.MountLabel,
		InitFunc:   initFunc,
		StorageOpt: container.HostConfig.StorageOpt,
	}

	// Indexing by OS is safe here as validation of OS has already been performed in create() (the only
	// caller), and guaranteed non-nil
	return i.layerStores[container.OS].CreateRWLayer(container.ID, layerID, rwLayerOpts)
}

// GetLayerByID returns a layer by ID and operating system
// called from daemon.go Daemon.restore(), and Daemon.containerExport()
func (i *ImageService) GetLayerByID(cid string, os string) (layer.RWLayer, error) {
	return i.layerStores[os].GetRWLayer(cid)
}

// LayerStoreStatus returns the status for each layer store
// called from info.go
func (i *ImageService) LayerStoreStatus() map[string][][2]string {
	result := make(map[string][][2]string)
	for os, store := range i.layerStores {
		result[os] = store.DriverStatus()
	}
	return result
}

// GetLayerMountID returns the mount ID for a layer
// called from daemon.go Daemon.Shutdown(), and Daemon.Cleanup() (cleanup is actually continerCleanup)
// TODO: needs to be refactored to Unmount (see callers), or removed and replaced
// with GetLayerByID
func (i *ImageService) GetLayerMountID(cid string, os string) (string, error) {
	return i.layerStores[os].GetMountID(cid)
}

// Cleanup resources before the process is shutdown.
// called from daemon.go Daemon.Shutdown()
func (i *ImageService) Cleanup() {
	for os, ls := range i.layerStores {
		if ls != nil {
			if err := ls.Cleanup(); err != nil {
				logrus.Errorf("Error during layer Store.Cleanup(): %v %s", err, os)
			}
		}
	}
}

// GraphDriverForOS returns the name of the graph drvier
// moved from Daemon.GraphDriverName, used by:
// - newContainer
// - to report an error in Daemon.Mount(container)
func (i *ImageService) GraphDriverForOS(os string) string {
	return i.layerStores[os].DriverName()
}

// ReleaseLayer releases a layer allowing it to be removed
// called from delete.go Daemon.cleanupContainer(), and Daemon.containerExport()
func (i *ImageService) ReleaseLayer(rwlayer layer.RWLayer, containerOS string) error {
	metadata, err := i.layerStores[containerOS].ReleaseRWLayer(rwlayer)
	layer.LogReleaseMetadata(metadata)
	if err != nil && err != layer.ErrMountDoesNotExist && !os.IsNotExist(errors.Cause(err)) {
		return errors.Wrapf(err, "driver %q failed to remove root filesystem",
			i.layerStores[containerOS].DriverName())
	}
	return nil
}

// LayerDiskUsage returns the number of bytes used by layer stores
// called from disk_usage.go
func (i *ImageService) LayerDiskUsage(ctx context.Context) (int64, error) {
	var allLayersSize int64
	layerRefs := i.getLayerRefs()
	for _, ls := range i.layerStores {
		allLayers := ls.Map()
		for _, l := range allLayers {
			select {
			case <-ctx.Done():
				return allLayersSize, ctx.Err()
			default:
				size, err := l.DiffSize()
				if err == nil {
					if _, ok := layerRefs[l.ChainID()]; ok {
						allLayersSize += size
					} else {
						logrus.Warnf("found leaked image layer %v", l.ChainID())
					}
				} else {
					logrus.Warnf("failed to get diff size for layer %v", l.ChainID())
				}
			}
		}
	}
	return allLayersSize, nil
}

func (i *ImageService) getLayerRefs() map[layer.ChainID]int {
	tmpImages := i.imageStore.Map()
	layerRefs := map[layer.ChainID]int{}
	for id, img := range tmpImages {
		dgst := digest.Digest(id)
		if len(i.referenceStore.References(dgst)) == 0 && len(i.imageStore.Children(id)) != 0 {
			continue
		}

		rootFS := *img.RootFS
		rootFS.DiffIDs = nil
		for _, id := range img.RootFS.DiffIDs {
			rootFS.Append(id)
			chid := rootFS.ChainID()
			layerRefs[chid]++
		}
	}

	return layerRefs
}

// UpdateConfig values
//
// called from reload.go
func (i *ImageService) UpdateConfig(maxDownloads, maxUploads *int) {
	if i.downloadManager != nil && maxDownloads != nil {
		i.downloadManager.SetConcurrency(*maxDownloads)
	}
	if i.uploadManager != nil && maxUploads != nil {
		i.uploadManager.SetConcurrency(*maxUploads)
	}
}
