.PHONY: lint test test-e2e deadcode check

MAX_FILE_LINES := 500

# Run all checks: lint + deadcode + file length + tests
check: lint deadcode check-file-length test

# Run golangci-lint
lint:
	golangci-lint run ./...

# Run deadcode analysis (golang.org/x/tools/cmd/deadcode).
# Uses -test to include test executables as entry points since this is a library.
# Uses -tags e2e to include S3 integration code paths.
deadcode:
	deadcode -test -tags e2e ./...

# Enforce maximum file length to keep files focused and reviewable.
check-file-length:
	@over=$$(find . -name '*.go' -not -path './vendor/*' -exec awk -v max=$(MAX_FILE_LINES) \
		'END { if (NR > max) print FILENAME ": " NR " lines (max " max ")" }' {} \;); \
	if [ -n "$$over" ]; then echo "Files exceeding $(MAX_FILE_LINES) lines:" && echo "$$over" && exit 1; fi

# Run unit tests
test:
	go test -count=1 -race ./...

# Run E2E tests against real S3.
# Usage: AWS_PROFILE=your-profile DUREX_TEST_BUCKET=your-bucket make test-e2e
test-e2e:
	go test -tags e2e -count=1 -race -v ./...
