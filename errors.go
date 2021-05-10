package qingstor

import "github.com/aos-dev/go-storage/v3/services"

var (
	// ErrInvalidBucketName will be returned while bucket name is invalid.
	ErrInvalidBucketName = services.NewErrorCode("invalid bucket name")

	// ErrInvalidWorkDir will be returned while work dir is invalid.
	// Work dir must start and end with only one '/'
	ErrInvalidWorkDir = services.NewErrorCode("invalid work dir")

	// ErrInvalidEncryptionCustomerKey will be returned while encryption customer key is invalid.
	// Encryption key must be a 32-byte AES-256 key.
	ErrInvalidEncryptionCustomerKey = services.NewErrorCode("invalid encryption customer key")

	// ErrAppendNextPositionEmpty will be returned while next append position is empty.
	ErrAppendNextPositionEmpty = services.NewErrorCode("next append position is empty")

	// ErrAppendOffsetNotSet will be returned while append offset is not set.
	ErrAppendOffsetNotSet = services.NewErrorCode("append offset is not set")
)
