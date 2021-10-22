# go-services-qingstor

[QingStor Object Storage](https://www.qingcloud.com/products/objectstorage/) service support for [go-storage](https://github.com/beyondstorage/go-storage).

## Notes

**This package has been moved to [go-storage](https://github.com/beyondstorage/go-storage/tree/master/services/qingstor).**

```shell
go get go.beyondstorage.io/services/qingstor/v4
```

## Install

```go
go get github.com/beyondstorage/go-service-qingstor/v3
```

## Usage

```go
import (
	"log"

	_ "github.com/beyondstorage/go-service-qingstor/v3"
	"github.com/beyondstorage/go-storage/v4/services"
)

func main() {
	store, err := services.NewStoragerFromString("qingstor://bucket_name/path/to/workdir?credential=hmac:access_key_id:secret_access_key&endpoint=https:qingstor.com")
	if err != nil {
		log.Fatal(err)
	}
	
	// Write data from io.Reader into hello.txt
	n, err := store.Write("hello.txt", r, length)
}
```

- See more examples in [go-storage-example](https://github.com/beyondstorage/go-storage-example).
- Read [more docs](https://beyondstorage.io/docs/go-storage/services/qingstor) about go-service-qingstor.
