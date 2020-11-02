package qingstor

import (
	"context"
	"io"

	"github.com/pengsrc/go-shared/convert"
	"github.com/qingstor/qingstor-sdk-go/v4/service"

	"github.com/aos-dev/go-storage/v2/pkg/headers"
	"github.com/aos-dev/go-storage/v2/pkg/iowrap"
	typ "github.com/aos-dev/go-storage/v2/types"
)

func (s *Storage) delete(ctx context.Context, path string, opt *pairStorageDelete) (err error) {
	rp := s.getAbsPath(path)

	_, err = s.bucket.DeleteObjectWithContext(ctx, rp)
	if err != nil {
		return
	}
	return nil
}
func (s *Storage) initIndexSegment(ctx context.Context, path string, opt *pairStorageInitIndexSegment) (seg typ.Segment, err error) {
	input := &service.InitiateMultipartUploadInput{}

	rp := s.getAbsPath(path)

	output, err := s.bucket.InitiateMultipartUploadWithContext(ctx, rp, input)
	if err != nil {
		return
	}

	id := *output.UploadID

	seg = typ.NewIndexBasedSegment(path, id)
	return seg, nil
}

type listObjectInput service.ListObjectsInput

func (i *listObjectInput) ContinuationToken() string {
	return convert.StringValue(i.Marker)
}

func (s *Storage) listDir(ctx context.Context, dir string, opt *pairStorageListDir) (oi *typ.ObjectIterator, err error) {
	marker := ""
	delimiter := "/"
	limit := 200

	rp := s.getAbsPath(dir)

	input := &listObjectInput{
		Limit:     &limit,
		Marker:    &marker,
		Prefix:    &rp,
		Delimiter: &delimiter,
	}

	return typ.NewObjectIterator(ctx, s.listNextDir, input), nil
}
func (s *Storage) listNextDir(ctx context.Context, page *typ.ObjectPage) error {
	input := page.Status.(*listObjectInput)
	serviceInput := service.ListObjectsInput(*input)

	output, err := s.bucket.ListObjectsWithContext(ctx, &serviceInput)
	if err != nil {
		return err
	}

	for _, v := range output.CommonPrefixes {
		o := &typ.Object{
			ID:   *v,
			Name: s.getRelPath(*v),
			Type: typ.ObjectTypeDir,
		}

		page.Data = append(page.Data, o)
	}

	for _, v := range output.Keys {
		// add filter to exclude dir-key itself, which would exist if created in console, see issue #365
		if convert.StringValue(v.Key) == *input.Prefix {
			continue
		}
		o, err := s.formatFileObject(v)
		if err != nil {
			return err
		}

		page.Data = append(page.Data, o)
	}

	if service.StringValue(output.NextMarker) == "" {
		return typ.IterateDone
	}
	if output.HasMore != nil && !*output.HasMore {
		return typ.IterateDone
	}
	if len(output.Keys) == 0 {
		return typ.IterateDone
	}

	input.Marker = output.NextMarker
	return nil
}
func (s *Storage) listPrefix(ctx context.Context, prefix string, opt *pairStorageListPrefix) (oi *typ.ObjectIterator, err error) {
	marker := ""
	limit := 200

	rp := s.getAbsPath(prefix)

	input := &listObjectInput{
		Limit:  &limit,
		Marker: &marker,
		Prefix: &rp,
	}

	return typ.NewObjectIterator(ctx, s.listNextPrefix, input), nil
}
func (s *Storage) listNextPrefix(ctx context.Context, page *typ.ObjectPage) error {
	input := page.Status.(*listObjectInput)
	serviceInput := service.ListObjectsInput(*input)

	output, err := s.bucket.ListObjectsWithContext(ctx, &serviceInput)
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
		return typ.IterateDone
	}
	if output.HasMore != nil && !*output.HasMore {
		return typ.IterateDone
	}
	if len(output.Keys) == 0 {
		return typ.IterateDone
	}

	input.Marker = output.NextMarker
	return nil
}

type listMultipartUploadsInput service.ListMultipartUploadsInput

func (i *listMultipartUploadsInput) ContinuationToken() string {
	return convert.StringValue(i.UploadIDMarker)
}
func (s *Storage) listPrefixSegments(ctx context.Context, prefix string, opt *pairStorageListPrefixSegments) (si *typ.SegmentIterator, err error) {
	limit := 200

	rp := s.getAbsPath(prefix)

	input := &listMultipartUploadsInput{
		Limit:  &limit,
		Prefix: &rp,
	}

	return typ.NewSegmentIterator(ctx, s.listNextPrefixSegments, input), nil
}

func (s *Storage) listNextPrefixSegments(ctx context.Context, page *typ.SegmentPage) error {
	input := page.Status.(*listMultipartUploadsInput)
	serviceInput := service.ListMultipartUploadsInput(*input)

	output, err := s.bucket.ListMultipartUploadsWithContext(ctx, &serviceInput)
	if err != nil {
		return err
	}

	for _, v := range output.Uploads {
		// TODO: we should handle rel prefix here.
		seg := typ.NewIndexBasedSegment(*v.Key, *v.UploadID)

		page.Data = append(page.Data, seg)
	}

	input.KeyMarker = output.NextKeyMarker
	input.UploadIDMarker = output.NextUploadIDMarker
	if service.StringValue(input.KeyMarker) == "" && service.StringValue(input.UploadIDMarker) == "" {
		return typ.IterateDone
	}
	if output.HasMore != nil && !*output.HasMore {
		return typ.IterateDone
	}

	return nil
}
func (s *Storage) metadata(ctx context.Context, opt *pairStorageMetadata) (meta *typ.StorageMeta, err error) {
	meta = typ.NewStorageMeta()
	meta.Name = *s.properties.BucketName
	meta.WorkDir = s.workDir
	meta.SetLocation(*s.properties.Zone)
	return meta, nil
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
func (s *Storage) stat(ctx context.Context, path string, opt *pairStorageStat) (o *typ.Object, err error) {
	input := &service.HeadObjectInput{}

	rp := s.getAbsPath(path)

	output, err := s.bucket.HeadObjectWithContext(ctx, rp, input)
	if err != nil {
		return
	}

	o = s.newObject(true)
	o.ID = rp
	o.Name = path
	o.Type = typ.ObjectTypeFile

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
func (s *Storage) writeIndexSegment(ctx context.Context, seg typ.Segment, r io.Reader, index int, size int64, opt *pairStorageWriteIndexSegment) (err error) {
	p, err := seg.(*typ.IndexBasedSegment).InsertPart(index, size)
	if err != nil {
		return
	}

	rp := s.getAbsPath(seg.Path())

	if opt.HasReadCallbackFunc {
		r = iowrap.CallbackReader(r, opt.ReadCallbackFunc)
	}

	_, err = s.bucket.UploadMultipartWithContext(ctx, rp, &service.UploadMultipartInput{
		PartNumber:    service.Int(p.Index),
		UploadID:      service.String(seg.ID()),
		ContentLength: &size,
		Body:          io.LimitReader(r, size),
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

func (s *Storage) abortSegment(ctx context.Context, seg typ.Segment, opt *pairStorageAbortSegment) (err error) {
	rp := s.getAbsPath(seg.Path())

	_, err = s.bucket.AbortMultipartUploadWithContext(ctx, rp, &service.AbortMultipartUploadInput{
		UploadID: service.String(seg.ID()),
	})
	if err != nil {
		return
	}
	return
}
func (s *Storage) completeSegment(ctx context.Context, seg typ.Segment, opt *pairStorageCompleteSegment) (err error) {
	parts := seg.(*typ.IndexBasedSegment).Parts()
	objectParts := make([]*service.ObjectPartType, 0, len(parts))
	for _, v := range parts {
		objectParts = append(objectParts, &service.ObjectPartType{
			PartNumber: service.Int(v.Index),
			Size:       service.Int64(v.Size),
		})
	}

	rp := s.getAbsPath(seg.Path())

	_, err = s.bucket.CompleteMultipartUploadWithContext(ctx, rp, &service.CompleteMultipartUploadInput{
		UploadID:    service.String(seg.ID()),
		ObjectParts: objectParts,
	})
	if err != nil {
		return
	}
	return
}
func (s *Storage) statistical(ctx context.Context, opt *pairStorageStatistical) (statistic *typ.StorageStatistic, err error) {
	statistic = typ.NewStorageStatistic()

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
