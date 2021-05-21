# Change Log

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/)
and this project adheres to [Semantic Versioning](https://semver.org/).

## [v2.1.0] - 2021-04-24

### Added

- *: Implement proposal unify object metadata (#25)
- storage: Normalize iterator next function names (#27)
- pair: Implement default pair support for service (#29)
- *: Set default pair when init (#31)
- storage: Implement Create API (#33)
- storage: Set multipart attributes when create multipart (#34)
- *: Add UnimplementedStub (#35)
- storage: Implement SSE support (#37)
- tests: Introduce STORAGE_QINGSTOR_INTEGRATION_TEST (#39)
- storage: Implement GSP-40 (#41)

### Changed

- storage: Clean up next page logic
- build: Make sure integration tests has been executed
- docs: Migrate zulip to matrix
- docs: Remove zulip
- ci: Only run Integration Test while push to master
- storage: Rename SSE related pairs to meet GSP-38 (#38)

### Fixed

- storage: Fix multipart integration tests (#36)

### Removed

- *: Remove parsed pairs pointer (#28)

### Upgrade

- build(deps): bump github.com/qingstor/qingstor-sdk-go/v4 (#26)

## [v2.0.0] - 2021-01-17

### Added

- tests: Add integration tests (#17)
- storage: Implement Fetcher (#19)
- storage: Implement proposal Unify List Operation (#20)
- *: Implement Segment API Redesign (#21)
- storage: Implement proposal Object Mode (#22)

### Changed

- Migrate to go-storage v3 (#23)

## v1.0.0 - 2020-11-12

### Added

- Implement qingstor services.

[v2.1.0]: https://github.com/beyondstorage/go-service-qingstor/compare/v2.0.0...v2.1.0
[v2.0.0]: https://github.com/beyondstorage/go-service-qingstor/compare/v1.0.0...v2.0.0
