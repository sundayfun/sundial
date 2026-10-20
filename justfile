project-root := `git rev-parse --show-toplevel`
golangci-lint-version := "v2.14.0"
golangci-lint-package := "github.com/golangci/golangci-lint/v2/cmd/golangci-lint@" + golangci-lint-version
golangci-lint-dir := project-root / "bin" / ("golangci-lint-" + golangci-lint-version)
golangci-lint := golangci-lint-dir / "golangci-lint"

default:
    @just --list

# Install the repository-pinned golangci-lint binary.
ensure-golangci-lint:
    #!/usr/bin/env bash
    set -euo pipefail

    bin="{{ golangci-lint }}"
    expected_version="{{ golangci-lint-version }}"
    expected_go="$(go env GOVERSION | cut -d. -f1,2)"
    if [[ -x "$bin" ]]; then
        version="$($bin version)"
        if [[ "$(awk '{print "v" $4}' <<< "$version")" == "$expected_version" ]] && \
            [[ "$(awk '{print $7}' <<< "$version" | cut -d. -f1,2)" == "$expected_go" ]]; then
            exit 0
        fi
    fi

    mkdir -p "$(dirname "$bin")"
    GOBIN="$(dirname "$bin")" go install {{ golangci-lint-package }}

# Check code without modifying files.
lint: ensure-golangci-lint
    {{ golangci-lint }} run ./...

# Format code with the configured golangci-lint formatters.
fmt: ensure-golangci-lint
    {{ golangci-lint }} fmt ./...

# Run the same lint and race-test gate used by CI.
test: lint
    go test -race ./...
