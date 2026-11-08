// The BRC-22 boundary harness is its own module so that the bridge module never
// imports an overlay engine; only this test does.
module github.com/lightwebinc/overlay-bridge/tests/tpnil/brc22

go 1.26.2

require (
	github.com/bsv-blockchain/go-overlay-services v1.3.8
	github.com/bsv-blockchain/go-sdk v1.7.3
	github.com/gofiber/fiber/v2 v2.52.15
)

require (
	github.com/apapsch/go-jsonmerge/v2 v2.0.0 // indirect
	github.com/clipperhouse/uax29/v2 v2.7.0 // indirect
	github.com/go-resty/resty/v2 v2.17.2 // indirect
	github.com/google/uuid v1.6.0 // indirect
	github.com/klauspost/compress v1.20.1 // indirect
	github.com/mattn/go-colorable v0.1.16 // indirect
	github.com/mattn/go-isatty v0.0.24 // indirect
	github.com/mattn/go-runewidth v0.0.31 // indirect
	github.com/molecule-man/go-brrr v1.2.0 // indirect
	github.com/mrz1836/go-whatsonchain v1.3.0 // indirect
	github.com/oapi-codegen/runtime v1.7.0 // indirect
	github.com/philhofer/fwd v1.2.0 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/stretchr/testify v1.12.1 // indirect
	github.com/tinylib/msgp v1.6.5 // indirect
	github.com/valyala/bytebufferpool v1.0.0 // indirect
	github.com/valyala/fasthttp v1.75.0 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/crypto v0.58.0 // indirect
	golang.org/x/net v0.61.0 // indirect
	golang.org/x/sync v0.24.0 // indirect
	golang.org/x/sys v0.49.0 // indirect
)
