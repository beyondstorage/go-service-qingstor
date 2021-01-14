// Code generated by go generate via internal/cmd/service; DO NOT EDIT.
package qingstor

import (
	"context"
	"io"

	"github.com/aos-dev/go-storage/v2/pkg/credential"
	"github.com/aos-dev/go-storage/v2/pkg/endpoint"
	"github.com/aos-dev/go-storage/v2/pkg/httpclient"
	"github.com/aos-dev/go-storage/v2/services"
	. "github.com/aos-dev/go-storage/v2/types"
)

var _ credential.Provider
var _ endpoint.Provider
var _ Storager
var _ services.ServiceError
var _ httpclient.Options

// Type is the type for qingstor
const Type = "qingstor"

// Service available pairs.
const (
	// DisableURICleaning
	pairDisableURICleaning = "qingstor_disable_uri_cleaning"
)

// WithDisableURICleaning will apply disable_uri_cleaning value to Options
// DisableURICleaning
func WithDisableURICleaning(v bool) Pair {
	return Pair{
		Key:   pairDisableURICleaning,
		Value: v,
	}
}

// GetStorageClass will get storage-class value from metadata.
func GetStorageClass(m *Object) (string, bool) {
	v, ok := m.Get("qingstor-storage-class")
	if !ok {
		return "", false
	}
	return v.(string), true
}

// setstorage-class will set storage-class value into metadata.
func setStorageClass(m *Object, v string) *Object {
	return m.Set("qingstor-storage-class", v)
}

// pairServiceNew is the parsed struct
type pairServiceNew struct {
	pairs []Pair

	// Required pairs
	HasCredential bool
	Credential    *credential.Provider
	// Optional pairs
	HasEndpoint          bool
	Endpoint             endpoint.Provider
	HasHTTPClientOptions bool
	HTTPClientOptions    *httpclient.Options
	HasPairPolicy        bool
	PairPolicy           PairPolicy
	// Generated pairs
}

// parsePairServiceNew will parse Pair slice into *pairServiceNew
func parsePairServiceNew(opts []Pair) (*pairServiceNew, error) {
	result := &pairServiceNew{
		pairs: opts,
	}

	for _, v := range opts {
		switch v.Key {
		// Required pairs
		case "credential":
			result.HasCredential = true
			result.Credential = v.Value.(*credential.Provider)
		// Optional pairs
		case "endpoint":
			result.HasEndpoint = true
			result.Endpoint = v.Value.(endpoint.Provider)
		case "http_client_options":
			result.HasHTTPClientOptions = true
			result.HTTPClientOptions = v.Value.(*httpclient.Options)
		case "pair_policy":
			result.HasPairPolicy = true
			result.PairPolicy = v.Value.(PairPolicy)
			// Generated pairs
		}
	}
	if !result.HasCredential {
		return nil, services.NewPairRequiredError("credential")
	}

	return result, nil
}

// pairServiceCreate is the parsed struct
type pairServiceCreate struct {
	pairs []Pair

	// Required pairs
	HasLocation bool
	Location    string
	// Optional pairs
	// Generated pairs
}

// parsePairServiceCreate will parse Pair slice into *pairServiceCreate
func (s *Service) parsePairServiceCreate(opts []Pair) (*pairServiceCreate, error) {
	result := &pairServiceCreate{
		pairs: opts,
	}

	for _, v := range opts {
		switch v.Key {
		// Required pairs
		case "location":
			result.HasLocation = true
			result.Location = v.Value.(string)
		// Optional pairs
		// Generated pairs
		default:

			continue

		}
	}
	if !result.HasLocation {
		return nil, services.NewPairRequiredError("location")
	}

	return result, nil
}

// pairServiceDelete is the parsed struct
type pairServiceDelete struct {
	pairs []Pair

	// Required pairs
	// Optional pairs
	HasLocation bool
	Location    string
	// Generated pairs
}

// parsePairServiceDelete will parse Pair slice into *pairServiceDelete
func (s *Service) parsePairServiceDelete(opts []Pair) (*pairServiceDelete, error) {
	result := &pairServiceDelete{
		pairs: opts,
	}

	for _, v := range opts {
		switch v.Key {
		// Required pairs
		// Optional pairs
		case "location":
			result.HasLocation = true
			result.Location = v.Value.(string)
		// Generated pairs
		default:

			continue

		}
	}

	return result, nil
}

// pairServiceGet is the parsed struct
type pairServiceGet struct {
	pairs []Pair

	// Required pairs
	// Optional pairs
	HasLocation bool
	Location    string
	// Generated pairs
}

// parsePairServiceGet will parse Pair slice into *pairServiceGet
func (s *Service) parsePairServiceGet(opts []Pair) (*pairServiceGet, error) {
	result := &pairServiceGet{
		pairs: opts,
	}

	for _, v := range opts {
		switch v.Key {
		// Required pairs
		// Optional pairs
		case "location":
			result.HasLocation = true
			result.Location = v.Value.(string)
		// Generated pairs
		default:

			continue

		}
	}

	return result, nil
}

// pairServiceList is the parsed struct
type pairServiceList struct {
	pairs []Pair

	// Required pairs
	// Optional pairs
	HasLocation bool
	Location    string
	// Generated pairs
}

// parsePairServiceList will parse Pair slice into *pairServiceList
func (s *Service) parsePairServiceList(opts []Pair) (*pairServiceList, error) {
	result := &pairServiceList{
		pairs: opts,
	}

	for _, v := range opts {
		switch v.Key {
		// Required pairs
		// Optional pairs
		case "location":
			result.HasLocation = true
			result.Location = v.Value.(string)
		// Generated pairs
		default:

			continue

		}
	}

	return result, nil
}

// Create will create a new storager instance.
//
// This function will create a context by default.
func (s *Service) Create(name string, pairs ...Pair) (store Storager, err error) {
	ctx := context.Background()
	return s.CreateWithContext(ctx, name, pairs...)
}

// CreateWithContext will create a new storager instance.
func (s *Service) CreateWithContext(ctx context.Context, name string, pairs ...Pair) (store Storager, err error) {
	defer func() {
		err = s.formatError("create", err, name)
	}()
	var opt *pairServiceCreate
	opt, err = s.parsePairServiceCreate(pairs)
	if err != nil {
		return
	}

	return s.create(ctx, name, opt)
}

// Delete will delete a storager instance.
//
// This function will create a context by default.
func (s *Service) Delete(name string, pairs ...Pair) (err error) {
	ctx := context.Background()
	return s.DeleteWithContext(ctx, name, pairs...)
}

// DeleteWithContext will delete a storager instance.
func (s *Service) DeleteWithContext(ctx context.Context, name string, pairs ...Pair) (err error) {
	defer func() {
		err = s.formatError("delete", err, name)
	}()
	var opt *pairServiceDelete
	opt, err = s.parsePairServiceDelete(pairs)
	if err != nil {
		return
	}

	return s.delete(ctx, name, opt)
}

// Get will get a valid storager instance for service.
//
// This function will create a context by default.
func (s *Service) Get(name string, pairs ...Pair) (store Storager, err error) {
	ctx := context.Background()
	return s.GetWithContext(ctx, name, pairs...)
}

// GetWithContext will get a valid storager instance for service.
func (s *Service) GetWithContext(ctx context.Context, name string, pairs ...Pair) (store Storager, err error) {
	defer func() {
		err = s.formatError("get", err, name)
	}()
	var opt *pairServiceGet
	opt, err = s.parsePairServiceGet(pairs)
	if err != nil {
		return
	}

	return s.get(ctx, name, opt)
}

// List will list all storager instances under this service.
//
// This function will create a context by default.
func (s *Service) List(pairs ...Pair) (sti *StoragerIterator, err error) {
	ctx := context.Background()
	return s.ListWithContext(ctx, pairs...)
}

// ListWithContext will list all storager instances under this service.
func (s *Service) ListWithContext(ctx context.Context, pairs ...Pair) (sti *StoragerIterator, err error) {
	defer func() {

		err = s.formatError("list", err, "")
	}()
	var opt *pairServiceList
	opt, err = s.parsePairServiceList(pairs)
	if err != nil {
		return
	}

	return s.list(ctx, opt)
}

// pairStorageNew is the parsed struct
type pairStorageNew struct {
	pairs []Pair

	// Required pairs
	HasName bool
	Name    string
	// Optional pairs
	HasDisableURICleaning bool
	DisableURICleaning    bool
	HasHTTPClientOptions  bool
	HTTPClientOptions     *httpclient.Options
	HasLocation           bool
	Location              string
	HasPairPolicy         bool
	PairPolicy            PairPolicy
	HasWorkDir            bool
	WorkDir               string
	// Generated pairs
}

// parsePairStorageNew will parse Pair slice into *pairStorageNew
func parsePairStorageNew(opts []Pair) (*pairStorageNew, error) {
	result := &pairStorageNew{
		pairs: opts,
	}

	for _, v := range opts {
		switch v.Key {
		// Required pairs
		case "name":
			result.HasName = true
			result.Name = v.Value.(string)
		// Optional pairs
		case pairDisableURICleaning:
			result.HasDisableURICleaning = true
			result.DisableURICleaning = v.Value.(bool)
		case "http_client_options":
			result.HasHTTPClientOptions = true
			result.HTTPClientOptions = v.Value.(*httpclient.Options)
		case "location":
			result.HasLocation = true
			result.Location = v.Value.(string)
		case "pair_policy":
			result.HasPairPolicy = true
			result.PairPolicy = v.Value.(PairPolicy)
		case "work_dir":
			result.HasWorkDir = true
			result.WorkDir = v.Value.(string)
			// Generated pairs
		}
	}
	if !result.HasName {
		return nil, services.NewPairRequiredError("name")
	}

	return result, nil
}

// pairStorageCompleteMultipart is the parsed struct
type pairStorageCompleteMultipart struct {
	pairs []Pair

	// Required pairs
	// Optional pairs
	// Generated pairs
}

// parsePairStorageCompleteMultipart will parse Pair slice into *pairStorageCompleteMultipart
func (s *Storage) parsePairStorageCompleteMultipart(opts []Pair) (*pairStorageCompleteMultipart, error) {
	result := &pairStorageCompleteMultipart{
		pairs: opts,
	}

	for _, v := range opts {
		switch v.Key {
		// Required pairs
		// Optional pairs
		// Generated pairs
		default:

			if s.pairPolicy.All || s.pairPolicy.CompleteMultipart {
				return nil, services.NewPairUnsupportedError(v)
			}

		}
	}

	return result, nil
}

// pairStorageCopy is the parsed struct
type pairStorageCopy struct {
	pairs []Pair

	// Required pairs
	// Optional pairs
	// Generated pairs
}

// parsePairStorageCopy will parse Pair slice into *pairStorageCopy
func (s *Storage) parsePairStorageCopy(opts []Pair) (*pairStorageCopy, error) {
	result := &pairStorageCopy{
		pairs: opts,
	}

	for _, v := range opts {
		switch v.Key {
		// Required pairs
		// Optional pairs
		// Generated pairs
		default:

			if s.pairPolicy.All || s.pairPolicy.Copy {
				return nil, services.NewPairUnsupportedError(v)
			}

		}
	}

	return result, nil
}

// pairStorageCreateMultipart is the parsed struct
type pairStorageCreateMultipart struct {
	pairs []Pair

	// Required pairs
	// Optional pairs
	// Generated pairs
}

// parsePairStorageCreateMultipart will parse Pair slice into *pairStorageCreateMultipart
func (s *Storage) parsePairStorageCreateMultipart(opts []Pair) (*pairStorageCreateMultipart, error) {
	result := &pairStorageCreateMultipart{
		pairs: opts,
	}

	for _, v := range opts {
		switch v.Key {
		// Required pairs
		// Optional pairs
		// Generated pairs
		default:

			if s.pairPolicy.All || s.pairPolicy.CreateMultipart {
				return nil, services.NewPairUnsupportedError(v)
			}

		}
	}

	return result, nil
}

// pairStorageDelete is the parsed struct
type pairStorageDelete struct {
	pairs []Pair

	// Required pairs
	// Optional pairs
	HasPartID bool
	PartID    string
	// Generated pairs
}

// parsePairStorageDelete will parse Pair slice into *pairStorageDelete
func (s *Storage) parsePairStorageDelete(opts []Pair) (*pairStorageDelete, error) {
	result := &pairStorageDelete{
		pairs: opts,
	}

	for _, v := range opts {
		switch v.Key {
		// Required pairs
		// Optional pairs
		case "part_id":
			result.HasPartID = true
			result.PartID = v.Value.(string)
		// Generated pairs
		default:

			if s.pairPolicy.All || s.pairPolicy.Delete {
				return nil, services.NewPairUnsupportedError(v)
			}

		}
	}

	return result, nil
}

// pairStorageFetch is the parsed struct
type pairStorageFetch struct {
	pairs []Pair

	// Required pairs
	// Optional pairs
	// Generated pairs
}

// parsePairStorageFetch will parse Pair slice into *pairStorageFetch
func (s *Storage) parsePairStorageFetch(opts []Pair) (*pairStorageFetch, error) {
	result := &pairStorageFetch{
		pairs: opts,
	}

	for _, v := range opts {
		switch v.Key {
		// Required pairs
		// Optional pairs
		// Generated pairs
		default:

			if s.pairPolicy.All || s.pairPolicy.Fetch {
				return nil, services.NewPairUnsupportedError(v)
			}

		}
	}

	return result, nil
}

// pairStorageList is the parsed struct
type pairStorageList struct {
	pairs []Pair

	// Required pairs
	// Optional pairs
	HasListMode bool
	ListMode    ListMode
	// Generated pairs
}

// parsePairStorageList will parse Pair slice into *pairStorageList
func (s *Storage) parsePairStorageList(opts []Pair) (*pairStorageList, error) {
	result := &pairStorageList{
		pairs: opts,
	}

	for _, v := range opts {
		switch v.Key {
		// Required pairs
		// Optional pairs
		case "list_mode":
			result.HasListMode = true
			result.ListMode = v.Value.(ListMode)
		// Generated pairs
		default:

			if s.pairPolicy.All || s.pairPolicy.List {
				return nil, services.NewPairUnsupportedError(v)
			}

		}
	}

	return result, nil
}

// pairStorageListMultipart is the parsed struct
type pairStorageListMultipart struct {
	pairs []Pair

	// Required pairs
	// Optional pairs
	// Generated pairs
}

// parsePairStorageListMultipart will parse Pair slice into *pairStorageListMultipart
func (s *Storage) parsePairStorageListMultipart(opts []Pair) (*pairStorageListMultipart, error) {
	result := &pairStorageListMultipart{
		pairs: opts,
	}

	for _, v := range opts {
		switch v.Key {
		// Required pairs
		// Optional pairs
		// Generated pairs
		default:

			if s.pairPolicy.All || s.pairPolicy.ListMultipart {
				return nil, services.NewPairUnsupportedError(v)
			}

		}
	}

	return result, nil
}

// pairStorageMetadata is the parsed struct
type pairStorageMetadata struct {
	pairs []Pair

	// Required pairs
	// Optional pairs
	// Generated pairs
}

// parsePairStorageMetadata will parse Pair slice into *pairStorageMetadata
func (s *Storage) parsePairStorageMetadata(opts []Pair) (*pairStorageMetadata, error) {
	result := &pairStorageMetadata{
		pairs: opts,
	}

	for _, v := range opts {
		switch v.Key {
		// Required pairs
		// Optional pairs
		// Generated pairs
		default:

			if s.pairPolicy.All || s.pairPolicy.Metadata {
				return nil, services.NewPairUnsupportedError(v)
			}

		}
	}

	return result, nil
}

// pairStorageMove is the parsed struct
type pairStorageMove struct {
	pairs []Pair

	// Required pairs
	// Optional pairs
	// Generated pairs
}

// parsePairStorageMove will parse Pair slice into *pairStorageMove
func (s *Storage) parsePairStorageMove(opts []Pair) (*pairStorageMove, error) {
	result := &pairStorageMove{
		pairs: opts,
	}

	for _, v := range opts {
		switch v.Key {
		// Required pairs
		// Optional pairs
		// Generated pairs
		default:

			if s.pairPolicy.All || s.pairPolicy.Move {
				return nil, services.NewPairUnsupportedError(v)
			}

		}
	}

	return result, nil
}

// pairStorageReach is the parsed struct
type pairStorageReach struct {
	pairs []Pair

	// Required pairs
	HasExpire bool
	Expire    int
	// Optional pairs
	// Generated pairs
}

// parsePairStorageReach will parse Pair slice into *pairStorageReach
func (s *Storage) parsePairStorageReach(opts []Pair) (*pairStorageReach, error) {
	result := &pairStorageReach{
		pairs: opts,
	}

	for _, v := range opts {
		switch v.Key {
		// Required pairs
		case "expire":
			result.HasExpire = true
			result.Expire = v.Value.(int)
		// Optional pairs
		// Generated pairs
		default:

			if s.pairPolicy.All || s.pairPolicy.Reach {
				return nil, services.NewPairUnsupportedError(v)
			}

		}
	}
	if !result.HasExpire {
		return nil, services.NewPairRequiredError("expire")
	}

	return result, nil
}

// pairStorageRead is the parsed struct
type pairStorageRead struct {
	pairs []Pair

	// Required pairs
	// Optional pairs
	HasOffset           bool
	Offset              int64
	HasReadCallbackFunc bool
	ReadCallbackFunc    func([]byte)
	HasSize             bool
	Size                int64
	// Generated pairs
}

// parsePairStorageRead will parse Pair slice into *pairStorageRead
func (s *Storage) parsePairStorageRead(opts []Pair) (*pairStorageRead, error) {
	result := &pairStorageRead{
		pairs: opts,
	}

	for _, v := range opts {
		switch v.Key {
		// Required pairs
		// Optional pairs
		case "offset":
			result.HasOffset = true
			result.Offset = v.Value.(int64)
		case "read_callback_func":
			result.HasReadCallbackFunc = true
			result.ReadCallbackFunc = v.Value.(func([]byte))
		case "size":
			result.HasSize = true
			result.Size = v.Value.(int64)
		// Generated pairs
		default:

			if s.pairPolicy.All || s.pairPolicy.Read {
				return nil, services.NewPairUnsupportedError(v)
			}

		}
	}

	return result, nil
}

// pairStorageStat is the parsed struct
type pairStorageStat struct {
	pairs []Pair

	// Required pairs
	// Optional pairs
	// Generated pairs
}

// parsePairStorageStat will parse Pair slice into *pairStorageStat
func (s *Storage) parsePairStorageStat(opts []Pair) (*pairStorageStat, error) {
	result := &pairStorageStat{
		pairs: opts,
	}

	for _, v := range opts {
		switch v.Key {
		// Required pairs
		// Optional pairs
		// Generated pairs
		default:

			if s.pairPolicy.All || s.pairPolicy.Stat {
				return nil, services.NewPairUnsupportedError(v)
			}

		}
	}

	return result, nil
}

// pairStorageStatistical is the parsed struct
type pairStorageStatistical struct {
	pairs []Pair

	// Required pairs
	// Optional pairs
	// Generated pairs
}

// parsePairStorageStatistical will parse Pair slice into *pairStorageStatistical
func (s *Storage) parsePairStorageStatistical(opts []Pair) (*pairStorageStatistical, error) {
	result := &pairStorageStatistical{
		pairs: opts,
	}

	for _, v := range opts {
		switch v.Key {
		// Required pairs
		// Optional pairs
		// Generated pairs
		default:

			if s.pairPolicy.All || s.pairPolicy.Statistical {
				return nil, services.NewPairUnsupportedError(v)
			}

		}
	}

	return result, nil
}

// pairStorageWrite is the parsed struct
type pairStorageWrite struct {
	pairs []Pair

	// Required pairs
	// Optional pairs
	HasContentMd5       bool
	ContentMd5          string
	HasContentType      bool
	ContentType         string
	HasOffset           bool
	Offset              int64
	HasReadCallbackFunc bool
	ReadCallbackFunc    func([]byte)
	HasStorageClass     bool
	StorageClass        string
	// Generated pairs
}

// parsePairStorageWrite will parse Pair slice into *pairStorageWrite
func (s *Storage) parsePairStorageWrite(opts []Pair) (*pairStorageWrite, error) {
	result := &pairStorageWrite{
		pairs: opts,
	}

	for _, v := range opts {
		switch v.Key {
		// Required pairs
		// Optional pairs
		case "content_md5":
			result.HasContentMd5 = true
			result.ContentMd5 = v.Value.(string)
		case "content_type":
			result.HasContentType = true
			result.ContentType = v.Value.(string)
		case "offset":
			result.HasOffset = true
			result.Offset = v.Value.(int64)
		case "read_callback_func":
			result.HasReadCallbackFunc = true
			result.ReadCallbackFunc = v.Value.(func([]byte))
		case "storage_class":
			result.HasStorageClass = true
			result.StorageClass = v.Value.(string)
		// Generated pairs
		default:

			if s.pairPolicy.All || s.pairPolicy.Write {
				return nil, services.NewPairUnsupportedError(v)
			}

		}
	}

	return result, nil
}

// pairStorageWriteMultipart is the parsed struct
type pairStorageWriteMultipart struct {
	pairs []Pair

	// Required pairs
	// Optional pairs
	// Generated pairs
}

// parsePairStorageWriteMultipart will parse Pair slice into *pairStorageWriteMultipart
func (s *Storage) parsePairStorageWriteMultipart(opts []Pair) (*pairStorageWriteMultipart, error) {
	result := &pairStorageWriteMultipart{
		pairs: opts,
	}

	for _, v := range opts {
		switch v.Key {
		// Required pairs
		// Optional pairs
		// Generated pairs
		default:

			if s.pairPolicy.All || s.pairPolicy.WriteMultipart {
				return nil, services.NewPairUnsupportedError(v)
			}

		}
	}

	return result, nil
}

// CompleteMultipart will complete a multipart upload and construct an Object.
//
// This function will create a context by default.
func (s *Storage) CompleteMultipart(o *Object, parts []*Part, pairs ...Pair) (err error) {
	ctx := context.Background()
	return s.CompleteMultipartWithContext(ctx, o, parts, pairs...)
}

// CompleteMultipartWithContext will complete a multipart upload and construct an Object.
func (s *Storage) CompleteMultipartWithContext(ctx context.Context, o *Object, parts []*Part, pairs ...Pair) (err error) {
	defer func() {
		err = s.formatError("complete_multipart", err)
	}()
	var opt *pairStorageCompleteMultipart
	opt, err = s.parsePairStorageCompleteMultipart(pairs)
	if err != nil {
		return
	}

	return s.completeMultipart(ctx, o, parts, opt)
}

// Copy will copy an Object or multiple object in the service.
//
// This function will create a context by default.
func (s *Storage) Copy(src string, dst string, pairs ...Pair) (err error) {
	ctx := context.Background()
	return s.CopyWithContext(ctx, src, dst, pairs...)
}

// CopyWithContext will copy an Object or multiple object in the service.
func (s *Storage) CopyWithContext(ctx context.Context, src string, dst string, pairs ...Pair) (err error) {
	defer func() {
		err = s.formatError("copy", err, src, dst)
	}()
	var opt *pairStorageCopy
	opt, err = s.parsePairStorageCopy(pairs)
	if err != nil {
		return
	}

	return s.copy(ctx, src, dst, opt)
}

// CreateMultipart will create a new multipart.
//
// This function will create a context by default.
func (s *Storage) CreateMultipart(path string, pairs ...Pair) (o *Object, err error) {
	ctx := context.Background()
	return s.CreateMultipartWithContext(ctx, path, pairs...)
}

// CreateMultipartWithContext will create a new multipart.
func (s *Storage) CreateMultipartWithContext(ctx context.Context, path string, pairs ...Pair) (o *Object, err error) {
	defer func() {
		err = s.formatError("create_multipart", err, path)
	}()
	var opt *pairStorageCreateMultipart
	opt, err = s.parsePairStorageCreateMultipart(pairs)
	if err != nil {
		return
	}

	return s.createMultipart(ctx, path, opt)
}

// Delete will delete an Object from service.
//
// This function will create a context by default.
func (s *Storage) Delete(path string, pairs ...Pair) (err error) {
	ctx := context.Background()
	return s.DeleteWithContext(ctx, path, pairs...)
}

// DeleteWithContext will delete an Object from service.
func (s *Storage) DeleteWithContext(ctx context.Context, path string, pairs ...Pair) (err error) {
	defer func() {
		err = s.formatError("delete", err, path)
	}()
	var opt *pairStorageDelete
	opt, err = s.parsePairStorageDelete(pairs)
	if err != nil {
		return
	}

	return s.delete(ctx, path, opt)
}

// Fetch will fetch from a given url to path.
//
// This function will create a context by default.
func (s *Storage) Fetch(path string, url string, pairs ...Pair) (err error) {
	ctx := context.Background()
	return s.FetchWithContext(ctx, path, url, pairs...)
}

// FetchWithContext will fetch from a given url to path.
func (s *Storage) FetchWithContext(ctx context.Context, path string, url string, pairs ...Pair) (err error) {
	defer func() {
		err = s.formatError("fetch", err, path, url)
	}()
	var opt *pairStorageFetch
	opt, err = s.parsePairStorageFetch(pairs)
	if err != nil {
		return
	}

	return s.fetch(ctx, path, url, opt)
}

// List will return list a specific path.
//
// This function will create a context by default.
func (s *Storage) List(path string, pairs ...Pair) (oi *ObjectIterator, err error) {
	ctx := context.Background()
	return s.ListWithContext(ctx, path, pairs...)
}

// ListWithContext will return list a specific path.
func (s *Storage) ListWithContext(ctx context.Context, path string, pairs ...Pair) (oi *ObjectIterator, err error) {
	defer func() {
		err = s.formatError("list", err, path)
	}()
	var opt *pairStorageList
	opt, err = s.parsePairStorageList(pairs)
	if err != nil {
		return
	}

	return s.list(ctx, path, opt)
}

// ListMultipart will list parts belong to this multipart.
//
// This function will create a context by default.
func (s *Storage) ListMultipart(o *Object, pairs ...Pair) (pi *PartIterator, err error) {
	ctx := context.Background()
	return s.ListMultipartWithContext(ctx, o, pairs...)
}

// ListMultipartWithContext will list parts belong to this multipart.
func (s *Storage) ListMultipartWithContext(ctx context.Context, o *Object, pairs ...Pair) (pi *PartIterator, err error) {
	defer func() {
		err = s.formatError("list_multipart", err)
	}()
	var opt *pairStorageListMultipart
	opt, err = s.parsePairStorageListMultipart(pairs)
	if err != nil {
		return
	}

	return s.listMultipart(ctx, o, opt)
}

// Metadata will return current storager metadata.
//
// This function will create a context by default.
func (s *Storage) Metadata(pairs ...Pair) (meta *StorageMeta, err error) {
	ctx := context.Background()
	return s.MetadataWithContext(ctx, pairs...)
}

// MetadataWithContext will return current storager metadata.
func (s *Storage) MetadataWithContext(ctx context.Context, pairs ...Pair) (meta *StorageMeta, err error) {
	defer func() {
		err = s.formatError("metadata", err)
	}()
	var opt *pairStorageMetadata
	opt, err = s.parsePairStorageMetadata(pairs)
	if err != nil {
		return
	}

	return s.metadata(ctx, opt)
}

// Move will move an object in the service.
//
// This function will create a context by default.
func (s *Storage) Move(src string, dst string, pairs ...Pair) (err error) {
	ctx := context.Background()
	return s.MoveWithContext(ctx, src, dst, pairs...)
}

// MoveWithContext will move an object in the service.
func (s *Storage) MoveWithContext(ctx context.Context, src string, dst string, pairs ...Pair) (err error) {
	defer func() {
		err = s.formatError("move", err, src, dst)
	}()
	var opt *pairStorageMove
	opt, err = s.parsePairStorageMove(pairs)
	if err != nil {
		return
	}

	return s.move(ctx, src, dst, opt)
}

// Reach will provide a way, which can reach the object.
//
// This function will create a context by default.
func (s *Storage) Reach(path string, pairs ...Pair) (url string, err error) {
	ctx := context.Background()
	return s.ReachWithContext(ctx, path, pairs...)
}

// ReachWithContext will provide a way, which can reach the object.
func (s *Storage) ReachWithContext(ctx context.Context, path string, pairs ...Pair) (url string, err error) {
	defer func() {
		err = s.formatError("reach", err, path)
	}()
	var opt *pairStorageReach
	opt, err = s.parsePairStorageReach(pairs)
	if err != nil {
		return
	}

	return s.reach(ctx, path, opt)
}

// Read will read the file's data.
//
// This function will create a context by default.
func (s *Storage) Read(path string, w io.Writer, pairs ...Pair) (n int64, err error) {
	ctx := context.Background()
	return s.ReadWithContext(ctx, path, w, pairs...)
}

// ReadWithContext will read the file's data.
func (s *Storage) ReadWithContext(ctx context.Context, path string, w io.Writer, pairs ...Pair) (n int64, err error) {
	defer func() {
		err = s.formatError("read", err, path)
	}()
	var opt *pairStorageRead
	opt, err = s.parsePairStorageRead(pairs)
	if err != nil {
		return
	}

	return s.read(ctx, path, w, opt)
}

// Stat will stat a path to get info of an object.
//
// This function will create a context by default.
func (s *Storage) Stat(path string, pairs ...Pair) (o *Object, err error) {
	ctx := context.Background()
	return s.StatWithContext(ctx, path, pairs...)
}

// StatWithContext will stat a path to get info of an object.
func (s *Storage) StatWithContext(ctx context.Context, path string, pairs ...Pair) (o *Object, err error) {
	defer func() {
		err = s.formatError("stat", err, path)
	}()
	var opt *pairStorageStat
	opt, err = s.parsePairStorageStat(pairs)
	if err != nil {
		return
	}

	return s.stat(ctx, path, opt)
}

// Statistical will count service's statistics, such as Size, Count.
//
// This function will create a context by default.
func (s *Storage) Statistical(pairs ...Pair) (statistic *StorageStatistic, err error) {
	ctx := context.Background()
	return s.StatisticalWithContext(ctx, pairs...)
}

// StatisticalWithContext will count service's statistics, such as Size, Count.
func (s *Storage) StatisticalWithContext(ctx context.Context, pairs ...Pair) (statistic *StorageStatistic, err error) {
	defer func() {
		err = s.formatError("statistical", err)
	}()
	var opt *pairStorageStatistical
	opt, err = s.parsePairStorageStatistical(pairs)
	if err != nil {
		return
	}

	return s.statistical(ctx, opt)
}

// Write will write data into a file.
//
// This function will create a context by default.
func (s *Storage) Write(path string, r io.Reader, size int64, pairs ...Pair) (n int64, err error) {
	ctx := context.Background()
	return s.WriteWithContext(ctx, path, r, size, pairs...)
}

// WriteWithContext will write data into a file.
func (s *Storage) WriteWithContext(ctx context.Context, path string, r io.Reader, size int64, pairs ...Pair) (n int64, err error) {
	defer func() {
		err = s.formatError("write", err, path)
	}()
	var opt *pairStorageWrite
	opt, err = s.parsePairStorageWrite(pairs)
	if err != nil {
		return
	}

	return s.write(ctx, path, r, size, opt)
}

// WriteMultipart will write content to a multipart.
//
// This function will create a context by default.
func (s *Storage) WriteMultipart(o *Object, r io.Reader, size int64, index int, pairs ...Pair) (n int64, err error) {
	ctx := context.Background()
	return s.WriteMultipartWithContext(ctx, o, r, size, index, pairs...)
}

// WriteMultipartWithContext will write content to a multipart.
func (s *Storage) WriteMultipartWithContext(ctx context.Context, o *Object, r io.Reader, size int64, index int, pairs ...Pair) (n int64, err error) {
	defer func() {
		err = s.formatError("write_multipart", err)
	}()
	var opt *pairStorageWriteMultipart
	opt, err = s.parsePairStorageWriteMultipart(pairs)
	if err != nil {
		return
	}

	return s.writeMultipart(ctx, o, r, size, index, opt)
}
