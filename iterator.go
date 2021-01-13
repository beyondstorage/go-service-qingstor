package qingstor

import (
	"strconv"
)

type objectPageStatus struct {
	delimiter string
	limit     int
	marker    string
	prefix    string
}

func (i *objectPageStatus) ContinuationToken() string {
	return i.marker
}

type segmentPageStatus struct {
	delimiter      string
	keyMarker      string
	limit          int
	prefix         string
	uploadIdMarker string
}

func (i *segmentPageStatus) ContinuationToken() string {
	return i.keyMarker + "/" + i.uploadIdMarker
}

type storagePageStatus struct {
	limit    int
	offset   int
	location string
}

func (i *storagePageStatus) ContinuationToken() string {
	return strconv.FormatInt(int64(i.offset), 10)
}

type partPageStatus struct {
	prefix           string
	limit            int
	partNumberMarker int
	uploadID         string
}

func (i *partPageStatus) ContinuationToken() string {
	return strconv.FormatInt(int64(i.partNumberMarker), 10)
}
