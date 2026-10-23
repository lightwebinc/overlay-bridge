# syntax=docker/dockerfile:1.28.0@sha256:bb22d9815c728170f72750f4e5b0d672e06176142e1d602c7e66c050100b7e5b
#
# Multi-stage Dockerfile for overlay-bridge. Produces a single static binary
# at /usr/local/bin/overlay-bridge on a distroless nonroot base.
#
# The builder is pinned by digest and is a LATER toolchain than go.mod's floor.
# The floor is held fleet-wide on purpose, while the stdlib actually shipped
# comes from here, so scanning sees what we ship rather than what we compile
# against.
#
# No ENV defaults are baked in: the bridge is configured entirely by flags, so
# pass them as the container command / Helm `args`. See docs/configuration.md.

FROM golang:1.27-alpine@sha256:b8c9b82affbbb82a020bbcf998dad2ba9516944ac7e883524642a481a9505e9c AS builder
RUN apk add --no-cache git ca-certificates
WORKDIR /src

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    go mod download

COPY . .
ARG VERSION=dev
ARG TARGETOS=linux
ARG TARGETARCH=amd64
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    set -eux; \
    mkdir -p /out; \
    CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
      go build -trimpath -buildvcs=false \
        -ldflags "-s -w -X main.Version=${VERSION}" \
        -o /out/overlay-bridge ./cmd/overlay-bridge

FROM gcr.io/distroless/static:nonroot@sha256:d62067879de890a1def9da0e7766b7d20810a5e5555cef8506b752437d7a3e88
USER nonroot:nonroot
COPY --from=builder /out/ /usr/local/bin/

# The licences travel with the image. The binary above statically contains
# go-sdk, whose licence requires its own text to be included in any copy or
# substantial portion of the software, and static linking leaves that file
# behind. Source-tree-only compliance does not cover a published image.
COPY LICENSE NOTICE LICENSE-THIRD-PARTY /usr/share/doc/overlay-bridge/
# Object lane 9171 and header lane 9172 (the edge DIALS these, so they are
# listeners here), the submit facade 9175, the header read API 9178, then the
# metrics and health listener 9179, which also serves /healthz and /readyz.
EXPOSE 9171 9172 9175 9178 9179
ENTRYPOINT ["/usr/local/bin/overlay-bridge"]
