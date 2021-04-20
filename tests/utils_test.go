package tests

import (
	"os"
	"testing"

	ps "github.com/aos-dev/go-storage/v3/pairs"
	"github.com/aos-dev/go-storage/v3/types"
	"github.com/google/uuid"

	"github.com/aos-dev/go-service-qingstor/v2"
)

func setupTest(t *testing.T) types.Storager {
	t.Log("Setup test for qingstor")

	store, err := qingstor.NewStorager(
		ps.WithCredential(os.Getenv("STORAGE_QINGSTOR_CREDENTIAL")),
		ps.WithEndpoint(os.Getenv("STORAGE_QINGSTOR_ENDPOINT")),
		ps.WithName(os.Getenv("STORAGE_QINGSTOR_NAME")),
		ps.WithWorkDir("/"+uuid.New().String()+"/"),
	)
	if err != nil {
		t.Errorf("new storager: %v", err)
	}
	return store
}
