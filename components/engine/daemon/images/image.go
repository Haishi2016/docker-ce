package images // import "github.com/docker/docker/daemon/images"

import (
	"fmt"

	"github.com/docker/distribution/reference"
	"github.com/docker/docker/errdefs"
	"github.com/docker/docker/image"
	"github.com/sirupsen/logrus"
)

// ErrImageDoesNotExist is error returned when no image can be found for a reference.
type ErrImageDoesNotExist struct {
	ref reference.Reference
}

func (e ErrImageDoesNotExist) Error() string {
	ref := e.ref
	if named, ok := ref.(reference.Named); ok {
		ref = reference.TagNameOnly(named)
	}
	return fmt.Sprintf("No such image: %s", reference.FamiliarString(ref))
}

// NotFound implements the NotFound interface
func (e ErrImageDoesNotExist) NotFound() {}

// GetImage returns an image corresponding to the image referred to by refOrID.
func (i *ImageService) GetImage(refOrID string) (*image.Image, error) {

	logrus.Debugf("Image being requested: ~~~~~~~~~~~~~~~~~~~ %v", refOrID)

	ref, err := reference.ParseAnyReference(refOrID)
	if err != nil {
		logrus.Debug("Failed to parse reference id~~~~~~~~~~~~~~")
		return nil, errdefs.InvalidParameter(err)
	}
	namedRef, ok := ref.(reference.Named)
	if !ok {
		digested, ok := ref.(reference.Digested)
		if !ok {
			logrus.Debug("Image doesn't exist~~~~~~~~~~~~~~~~~~~~~")
			return nil, ErrImageDoesNotExist{ref}
		}
		id := image.IDFromDigest(digested.Digest())
		if img, err := i.imageStore.Get(id); err == nil {
			return img, nil
		}
		logrus.Debug("Image doesn't exist after digest check~~~~~~~~~~~~~~~~")
		return nil, ErrImageDoesNotExist{ref}
	}

	if digest, err := i.referenceStore.Get(namedRef); err == nil {
		// Search the image stores to get the operating system, defaulting to host OS.
		id := image.IDFromDigest(digest)
		if img, err := i.imageStore.Get(id); err == nil {
			return img, nil
		}
	}
	logrus.Debug("Searching store~~~~~~~~~~~~~~~~~~~")
	// Search based on ID
	if id, err := i.imageStore.Search(refOrID); err == nil {
		img, err := i.imageStore.Get(id)
		if err != nil {
			logrus.Debugf("Failed to get image: %v", err)
			return nil, ErrImageDoesNotExist{ref}
		}
		return img, nil
	}
	
	logrus.Debug("Image search failed at this point")
	
	return nil, ErrImageDoesNotExist{ref}
}
