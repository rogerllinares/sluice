# syntax=docker/dockerfile:1

# ---- builder stage ----
# Pin the Go version to match go.mod (1.25). CI's docker-build step keeps this
# honest. Build a static binary so the runtime stage can be minimal/distroless.
FROM golang:1.25-alpine AS builder

WORKDIR /src

# Cache deps separately from source.
COPY go.mod go.sum* ./
RUN go mod download

COPY . .

# CGO disabled -> a fully static binary that runs on a scratch/distroless base.
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/sluice ./cmd/sluice

# ---- runtime stage ----
# distroless static = no shell, no package manager, tiny attack surface.
# Swap for alpine if a shell is needed for debugging.
FROM gcr.io/distroless/static-debian12:nonroot AS runtime

WORKDIR /app
COPY --from=builder /out/sluice /app/sluice

EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/app/sluice"]
