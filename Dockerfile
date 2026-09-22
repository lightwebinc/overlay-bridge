# syntax=docker/dockerfile:1.26@sha256:ecfaec9ed6d810b56388c508f4121597bfbba70d41a6dfeee4d8cad5f295fc32
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

FROM golang:1.27-alpine@sha256:cf6fca6641884b8433441b2b0652976f975e1d0fdd26d177eaaf8596087f3125 AS builder
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

FROM gcr.io/distroless/static:nonroot@sha256:1c2c046bc09ed40fad370b599a0b1ae7987f55b01e247cf27a7c27cd97e5bbc7
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
