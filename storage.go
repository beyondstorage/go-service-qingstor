package qingstor

import (
	"context"
	"io"

	"github.com/pengsrc/go-shared/convert"
	"github.com/qingstor/qingstor-sdk-go/v4/service"

	"github.com/aos-dev/go-storage/v2/pkg/headers"
	"github.com/aos-dev/go-storage/v2/pkg/iowrap"
	. "github.com/aos-dev/go-storage/v2/types"
)

func (s *Storage) abortSegment(ctx context.Context, seg Segment, opt *pairStorageAbortSegment) (err error) {
	_, err = s.bucket.AbortMultipartUploadWithContext(ctx, seg.Path, &service.AbortMultipartUploadInput{
		UploadID: service.String(seg.ID),
	})
	if err != nil {
		return
	}
	return
}

func (s *Storage) completeIndexSegment(ctx context.Context, seg Segment, parts []*Part, opt *pairStorageCompleteIndexSegment) (err error) {
	objectParts := make([]*service.ObjectPartType, 0, len(parts))
	for _, v := range parts {
		objectParts = append(objectParts, &service.ObjectPartType{
			PartNumber: service.Int(v.Index),
			Size:       service.Int64(v.Size),
		})
	}

	_, err = s.bucket.CompleteMultipartUploadWithContext(ctx, seg.Path, &service.CompleteMultipartUploadInput{
		UploadID:    service.String(seg.ID),
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

func (s *Storage) delete(ctx context.Context, path string, opt *pairStorageDelete) (err error) {
	rp := s.getAbsPath(path)

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

func (s *Storage) initSegment(ctx context.Context, path string, opt *pairStorageInitSegment) (seg Segment, err error) {
	input := &service.InitiateMultipartUploadInput{}

	rp := s.getAbsPath(path)

	output, err := s.bucket.InitiateMultipartUploadWithContext(ctx, rp, input)
	if err != nil {
		return
	}

	return Segment{
		Path: rp,
		ID:   *output.UploadID,
	}, nil
}

func (s *Storage) list(ctx context.Context, path string, opt *pairStorageList) (oi *ObjectIterator, err error) {
	input := &objectPageStatus{
		limit:  200,
		marker: "",
		prefix: s.getAbsPath(path),
	}

	var nextFn NextObjectFunc
	if opt.HasListType && opt.ListType == ListTypeDir {
		input.delimiter = "/"
		nextFn = s.nextDirPage
	} else {
		nextFn = s.nextPrefixPage
	}

	return NewObjectIterator(ctx, nextFn, input), nil
}

func (s *Storage) listIndexSegment(ctx context.Context, seg Segment, opt *pairStorageListIndexSegment) (pi *PartIterator, err error) {
	input := &partPageStatus{
		limit:    200,
		prefix:   seg.Path,
		uploadID: seg.ID,
	}

	return NewPartIterator(ctx, s.nextPartPage, input), nil
}

func (s *Storage) listNextPrefixSegments(ctx context.Context, page *SegmentPage) error {
	input := page.Status.(*segmentPageStatus)

	output, err := s.bucket.ListMultipartUploadsWithContext(ctx, &service.ListMultipartUploadsInput{
		KeyMarker:      &input.keyMarker,
		Limit:          &input.limit,
		Prefix:         &input.prefix,
		UploadIDMarker: &input.uploadIdMarker,
	})
	if err != nil {
		return err
	}

	for _, v := range output.Uploads {
		seg := &Segment{
			Path: *v.Key,
			ID:   *v.UploadID,
		}

		page.Data = append(page.Data, seg)
	}

	input.keyMarker = service.StringValue(output.NextKeyMarker)
	input.uploadIdMarker = service.StringValue(output.NextUploadIDMarker)

	if input.keyMarker == "" && input.uploadIdMarker == "" {
		return IterateDone
	}
	if output.HasMore != nil && !*output.HasMore {
		return IterateDone
	}
	return nil
}

func (s *Storage) listSegments(ctx context.Context, prefix string, opt *pairStorageListSegments) (si *SegmentIterator, err error) {
	input := &segmentPageStatus{
		limit:  200,
		prefix: s.getAbsPath(prefix),
	}

	return NewSegmentIterator(ctx, s.listNextPrefixSegments, input), nil
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

func (s *Storage) nextDirPage(ctx context.Context, page *ObjectPage) error {
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
		o.Name = s.getRelPath(*v)
		o.Type = ObjectTypeDir

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

func (s *Storage) nextPrefixPage(ctx context.Context, page *ObjectPage) error {
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
	if opt.HasReadCallbackFunc {
		rc = iowrap.CallbackReadCloser(rc, opt.ReadCallbackFunc)
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
	o.Name = path
	o.Type = ObjectTypeFile

	o.SetSize(service.Int64Value(output.ContentLength))
	o.SetUpdatedAt(service.TimeValue(output.LastModified))

	if output.ContentType != nil {
		o.SetContentType(service.StringValue(output.ContentType))
	}
	if output.ETag != nil {
		o.SetETag(service.StringValue(output.ETag))
	}

	if v := service.StringValue(output.XQSStorageClass); v != "" {
		setStorageClass(o, v)
	}

	return o, nil
}

func (s *Storage) statistical(ctx context.Context, opt *pairStorageStatistical) (statistic *StorageStatistic, err error) {
	statistic = NewStorageStatistic()

	output, err := s.bucket.GetStatisticsWithContext(ctx)
	if err != nil {
		return
	}

	if output.Size != nil {
		statistic.SetSize(*output.Size)
	}
	if output.Count != nil {
		statistic.SetCount(*output.Count)
	}
	return statistic, nil
}

func (s *Storage) write(ctx context.Context, path string, r io.Reader, opt *pairStorageWrite) (n int64, err error) {
	if opt.HasReadCallbackFunc {
		r = iowrap.CallbackReader(r, opt.ReadCallbackFunc)
	}

	input := &service.PutObjectInput{
		ContentLength: &opt.Size,
		Body:          io.LimitReader(r, opt.Size),
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
	return opt.Size, nil
}

func (s *Storage) writeIndexSegment(ctx context.Context, seg Segment, r io.Reader, index int, size int64, opt *pairStorageWriteIndexSegment) (err error) {
	rp := s.getAbsPath(seg.Path)

	if opt.HasReadCallbackFunc {
		r = iowrap.CallbackReader(r, opt.ReadCallbackFunc)
	}

	_, err = s.bucket.UploadMultipartWithContext(ctx, rp, &service.UploadMultipartInput{
		PartNumber:    service.Int(index),
		UploadID:      service.String(seg.ID),
		ContentLength: &size,
		Body:          io.LimitReader(r, size),
	})
	if err != nil {
		return
	}
	return
}
