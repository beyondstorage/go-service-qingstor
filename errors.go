package qingstor

import (
	"errors"
)

var (
	// ErrInvalidBucketName will be returned while bucket name is invalid.
	ErrInvalidBucketName = errors.New("invalid bucket name")

	// ErrInvalidWorkDir will be returned while work dir is invalid.
	// Work dir must start and end with only one '/'
	ErrInvalidWorkDir = errors.New("invalid work dir")

	// ErrInvalidEncryptionCustomerKey will be returned while encryption customer key is invalid.
	// Encryption key must be a 32-byte AES-256 key.
	ErrInvalidEncryptionCustomerKey = errors.New("invalid encryption customer key")

	// ErrAppendNextPositionEmpty will be returned while next append position is empty.
	ErrAppendNextPositionEmpty = errors.New("next append position is empty")

	// ErrAppendOffsetNotSet will be returned while append offset is not set.
	ErrAppendOffsetNotSet = errors.New("append offset is not set")
)
