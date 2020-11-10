module github.com/aos-dev/go-service-qingstor

go 1.14

require (
	bou.ke/monkey v1.0.2
	github.com/aos-dev/go-storage/v2 v2.0.0-beta.0.20201109081513-e0c34ddcb239
	github.com/golang/mock v1.4.4
	github.com/google/uuid v1.1.2
	github.com/pengsrc/go-shared v0.2.1-0.20190131101655-1999055a4a14
	github.com/qingstor/qingstor-sdk-go/v4 v4.1.1-0.20200907090207-bb758948ae4a
	github.com/stretchr/testify v1.6.1
)

replace github.com/aos-dev/go-storage/v2 => ../go-storage
