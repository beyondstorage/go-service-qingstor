package qingstor

import (
	"context"
	"fmt"
	"io"

	"github.com/pengsrc/go-shared/convert"
	"github.com/qingstor/qingstor-sdk-go/v4/service"

	"github.com/aos-dev/go-storage/v3/pkg/headers"
	"github.com/aos-dev/go-storage/v3/pkg/iowrap"
	. "github.com/aos-dev/go-storage/v3/types"
)

func (s *Storage) completeMultipart(ctx context.Context, o *Object, parts []*Part, opt *pairStorageCompleteMultipart) (err error) {
	if o.Mode&ModePart == 0 {
		return fmt.Errorf("object is not a part object")
	}

	objectParts := make([]*service.ObjectPartType, 0, len(parts))
	for _, v := range parts {
		objectParts = append(objectParts, &service.ObjectPartType{
			PartNumber: service.Int(v.Index),
			Size:       service.Int64(v.Size),
		})
	}

	_, err = s.bucket.CompleteMultipartUploadWithContext(ctx, o.ID, &service.CompleteMultipartUploadInput{
		UploadID:    service.String(o.MustGetMultipartID()),
		ObjectParts: objectParts,
	})
	if err != nil {
		return
	}
	return
}

func (s *Storage) copy(ctx context.Context, src string, dst string, opt *pairStorageCopy) (err error) {
	rs := s.getAbsPath(src)
	rd := s.getAbsPath(dst)

	_, err = s.bucket.PutObjectWithContext(ctx, rd, &service.PutObjectInput{
		XQSCopySource: &rs,
	})
	if err != nil {
		return
	}
	return nil
}

func (s *Storage) createMultipart(ctx context.Context, path string, opt *pairStorageCreateMultipart) (o *Object, err error) {
	input := &service.InitiateMultipartUploadInput{}

	rp := s.getAbsPath(path)

	output, err := s.bucket.InitiateMultipartUploadWithContext(ctx, rp, input)
	if err != nil {
		return
	}

	o = s.newObject(true)
	o.ID = rp
	o.Path = path
	o.Mode |= ModePart
	o.SetMultipartID(*output.UploadID)

	return o, nil
}

func (s *Storage) delete(ctx context.Context, path string, opt *pairStorageDelete) (err error) {
	rp := s.getAbsPath(path)

	if opt.HasMultipartID {
		_, err = s.bucket.AbortMultipartUploadWithContext(ctx, rp, &service.AbortMultipartUploadInput{
			UploadID: service.String(opt.MultipartID),
		})
		if err != nil {
			return
		}
		return
	}

	_, err = s.bucket.DeleteObjectWithContext(ctx, rp)
	if err != nil {
		return
	}
	return nil
}

func (s *Storage) fetch(ctx context.Context, path string, url string, opt *pairStorageFetch) (err error) {
	_, err = s.bucket.PutObjectWithContext(ctx, path, &service.PutObjectInput{
		XQSFetchSource: service.String(url),
	})
	return err
}

func (s *Storage) list(ctx context.Context, path string, opt *pairStorageList) (oi *ObjectIterator, err error) {
	input := &objectPageStatus{
		limit:  200,
		prefix: s.getAbsPath(path),
	}

	var nextFn NextObjectFunc

	switch {
	case opt.ListMode.IsPart():
		nextFn = s.nextPartObjectPageByPrefix
	case opt.ListMode.IsDir():
		input.delimiter = "/"
		nextFn = s.nextObjectPageByDir
	case opt.ListMode.IsPrefix():
		nextFn = s.nextObjectPageByPrefix
	default:
		return nil, fmt.Errorf("invalid list mode")
	}

	return NewObjectIterator(ctx, nextFn, input), nil
}

func (s *Storage) listMultipart(ctx context.Context, o *Object, opt *pairStorageListMultipart) (pi *PartIterator, err error) {
	if o.Mode&ModePart == 0 {
		return nil, fmt.Errorf("object is not a part object")
	}

	input := &partPageStatus{
		limit:    200,
		prefix:   o.ID,
		uploadID: o.MustGetMultipartID(),
	}

	return NewPartIterator(ctx, s.nextPartPage, input), nil
}

func (s *Storage) metadata(ctx context.Context, opt *pairStorageMetadata) (meta *StorageMeta, err error) {
	meta = NewStorageMeta()
	meta.Name = *s.properties.BucketName
	meta.WorkDir = s.workDir
	meta.SetLocation(*s.properties.Zone)
	return meta, nil
}

func (s *Storage) move(ctx context.Context, src string, dst string, opt *pairStorageMove) (err error) {
	rs := s.getAbsPath(src)
	rd := s.getAbsPath(dst)

	_, err = s.bucket.PutObjectWithContext(ctx, rd, &service.PutObjectInput{
		XQSMoveSource: &rs,
	})
	if err != nil {
		return
	}
	return nil
}

func (s *Storage) nextObjectPageByDir(ctx context.Context, page *ObjectPage) error {
	input := page.Status.(*objectPageStatus)

	output, err := s.bucket.ListObjectsWithContext(ctx, &service.ListObjectsInput{
		Delimiter: &input.delimiter,
		Limit:     &input.limit,
		Marker:    &input.marker,
		Prefix:    &input.prefix,
	})
	if err != nil {
		return err
	}

	for _, v := range output.CommonPrefixes {
		o := s.newObject(true)
		o.ID = *v
		o.Path = s.getRelPath(*v)
		o.Mode |= ModeDir

		page.Data = append(page.Data, o)
	}

	for _, v := range output.Keys {
		// add filter to exclude dir-key itself, which would exist if created in console, see issue #365
		if convert.StringValue(v.Key) == input.prefix {
			continue
		}
		o, err := s.formatFileObject(v)
		if err != nil {
			return err
		}

		page.Data = append(page.Data, o)
	}

	if service.StringValue(output.NextMarker) == "" {
		return IterateDone
	}
	if output.HasMore != nil && !*output.HasMore {
		return IterateDone
	}
	if len(output.Keys) == 0 {
		return IterateDone
	}

	input.marker = *output.NextMarker
	return nil
}

func (s *Storage) nextObjectPageByPrefix(ctx context.Context, page *ObjectPage) error {
	input := page.Status.(*objectPageStatus)

	output, err := s.bucket.ListObjectsWithContext(ctx, &service.ListObjectsInput{
		Limit:  &input.limit,
		Marker: &input.marker,
		Prefix: &input.prefix,
	})
	if err != nil {
		return err
	}

	for _, v := range output.Keys {
		o, err := s.formatFileObject(v)
		if err != nil {
			return err
		}

		page.Data = append(page.Data, o)
	}

	if service.StringValue(output.NextMarker) == "" {
		return IterateDone
	}
	if output.HasMore != nil && !*output.HasMore {
		return IterateDone
	}
	if len(output.Keys) == 0 {
		return IterateDone
	}

	input.marker = *output.NextMarker
	return nil
}

func (s *Storage) nextPartObjectPageByPrefix(ctx context.Context, page *ObjectPage) error {
	input := page.Status.(*objectPageStatus)

	output, err := s.bucket.ListMultipartUploadsWithContext(ctx, &service.ListMultipartUploadsInput{
		KeyMarker:      &input.marker,
		Limit:          &input.limit,
		Prefix:         &input.prefix,
		UploadIDMarker: &input.partIdMarker,
	})
	if err != nil {
		return err
	}

	for _, v := range output.Uploads {
		o := s.newObject(true)
		o.ID = *v.Key
		o.Path = s.getRelPath(*v.Key)
		o.Mode |= ModePart
		o.SetMultipartID(*v.UploadID)

		page.Data = append(page.Data, o)
	}

	input.marker = service.StringValue(output.NextKeyMarker)
	input.partIdMarker = service.StringValue(output.NextUploadIDMarker)

	if input.marker == "" && input.partIdMarker == "" {
		return IterateDone
	}
	if output.HasMore != nil && !*output.HasMore {
		return IterateDone
	}
	return nil
}

func (s *Storage) nextPartPage(ctx context.Context, page *PartPage) error {
	input := page.Status.(*partPageStatus)

	output, err := s.bucket.ListMultipartWithContext(ctx, input.prefix, &service.ListMultipartInput{
		Limit:            &input.limit,
		PartNumberMarker: &input.partNumberMarker,
		UploadID:         &input.uploadID,
	})
	if err != nil {
		return err
	}

	for _, v := range output.ObjectParts {
		p := &Part{
			Index: *v.PartNumber,
			Size:  *v.Size,
			ETag:  service.StringValue(v.Etag),
		}

		page.Data = append(page.Data, p)
	}

	// FIXME: QingStor ListMulitpart API looks like buggy.
	input.partNumberMarker += len(output.ObjectParts)
	if input.partNumberMarker >= service.IntValue(output.Count) {
		return IterateDone
	}

	return nil
}

func (s *Storage) reach(ctx context.Context, path string, opt *pairStorageReach) (url string, err error) {
	// FIXME: sdk should export GetObjectRequest as interface too?
	bucket := s.bucket.(*service.Bucket)

	rp := s.getAbsPath(path)

	r, _, err := bucket.GetObjectRequest(rp, nil)
	if err != nil {
		return
	}
	if err = r.BuildWithContext(ctx); err != nil {
		return
	}

	expire := opt.Expire
	if err = r.SignQuery(expire); err != nil {
		return
	}
	return r.HTTPRequest.URL.String(), nil
}

func (s *Storage) read(ctx context.Context, path string, w io.Writer, opt *pairStorageRead) (n int64, err error) {
	input := &service.GetObjectInput{}

	if opt.HasOffset || opt.HasSize {
		rs := headers.FormatRange(opt.Offset, opt.Size)
		input.Range = &rs
	}

	rp := s.getAbsPath(path)

	output, err := s.bucket.GetObjectWithContext(ctx, rp, input)
	if err != nil {
		return n, err
	}
	defer output.Body.Close()

	rc := output.Body
	if opt.HasIoCallback {
		rc = iowrap.CallbackReadCloser(rc, opt.IoCallback)
	}

	return io.Copy(w, rc)
}

func (s *Storage) stat(ctx context.Context, path string, opt *pairStorageStat) (o *Object, err error) {
	input := &service.HeadObjectInput{}

	rp := s.getAbsPath(path)

	output, err := s.bucket.HeadObjectWithContext(ctx, rp, input)
	if err != nil {
		return
	}

	o = s.newObject(true)
	o.ID = rp
	o.Path = path
	o.Mode |= ModeRead

	o.SetContentLength(service.Int64Value(output.ContentLength))
	o.SetLastModified(service.TimeValue(output.LastModified))

	if output.ContentType != nil {
		o.SetContentType(service.StringValue(output.ContentType))
	}
	if output.ETag != nil {
		o.SetEtag(service.StringValue(output.ETag))
	}

	sm := make(map[string]string)
	if v := service.StringValue(output.XQSStorageClass); v != "" {
		sm[MetadataStorageClass] = v
	}
	o.SetServiceMetadata(sm)

	return o, nil
}

func (s *Storage) write(ctx context.Context, path string, r io.Reader, size int64, opt *pairStorageWrite) (n int64, err error) {
	if opt.HasIoCallback {
		r = iowrap.CallbackReader(r, opt.IoCallback)
	}

	input := &service.PutObjectInput{
		ContentLength: &size,
		Body:          io.LimitReader(r, size),
	}
	if opt.HasContentMd5 {
		input.ContentMD5 = &opt.ContentMd5
	}
	if opt.HasStorageClass {
		input.XQSStorageClass = service.String(opt.StorageClass)
	}

	rp := s.getAbsPath(path)

	_, err = s.bucket.PutObjectWithContext(ctx, rp, input)
	if err != nil {
		return
	}
	return size, nil
}

func (s *Storage) writeMultipart(ctx context.Context, o *Object, r io.Reader, size int64, index int, opt *pairStorageWriteMultipart) (n int64, err error) {
	if o.Mode&ModePart == 0 {
		return 0, fmt.Errorf("object is not a part object")
	}

	_, err = s.bucket.UploadMultipartWithContext(ctx, o.ID, &service.UploadMultipartInput{
		PartNumber:    service.Int(index),
		UploadID:      service.String(o.MustGetMultipartID()),
		ContentLength: &size,
		Body:          io.LimitReader(r, size),
	})
	if err != nil {
		return
	}
	return size, nil
}
