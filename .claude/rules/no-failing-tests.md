# No failing tests

`go test ./...` must pass before any merge. A single failure blocks the claim "done."

Never skip a test to make the suite green. If a test is genuinely flaky, fix the flakiness -- don't mark it as skipped without a linked issue.

Run the race detector on anything touching shared state: `go test -race ./...`
