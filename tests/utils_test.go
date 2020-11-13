// +build integration_test

package tests

import (
	"os"
	"testing"

	"github.com/aos-dev/go-service-qingstor"
	ps "github.com/aos-dev/go-storage/v2/pairs"
	"github.com/aos-dev/go-storage/v2/pkg/credential"
	"github.com/aos-dev/go-storage/v2/pkg/endpoint"
	"github.com/aos-dev/go-storage/v2/types"
	"github.com/google/uuid"
)

func setupTest(t *testing.T) types.Storager {
	t.Log("Setup test for qingstor")

	cred, err := credential.Parse(os.Getenv("STORAGE_QINGSTOR_CREDENTIAL"))
	if err != nil {
		t.Errorf("credential parse: %v", err)
	}

	ep, err := endpoint.Parse(os.Getenv("STORAGE_QINGSTOR_ENDPOINT"))
	if err != nil {
		t.Errorf("endpoint parse: %v", err)
	}


	store, err := qingstor.NewStorager(
		ps.WithCredential(cred),
		ps.WithEndpoint(ep),
		ps.WithName(os.Getenv("STORAGE_QINGSTOR_NAME")),
		ps.WithWorkDir("/"+uuid.New().String()+"/"),
	)
	if err != nil {
		t.Errorf("new storager: %v", err)
	}
	return store
}
