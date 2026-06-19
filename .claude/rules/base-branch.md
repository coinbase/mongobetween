# Base Branch

The base branch is **`main`** on `origin` (Optioryx/mongobetween).

- All PRs target `origin/main`. Never `upstream/master`. Never force-push to `main`.
- New feature/fix branches are created from `origin/main`.

## Upstream (coinbase/mongobetween)

The `upstream` remote points at `coinbase/mongobetween`. It exists for reference only -- to check how the original handles something, or to cherry-pick a fix.

```bash
git fetch upstream          # fetch latest coinbase commits
git log upstream/master     # browse upstream history
git cherry-pick <sha>       # pull in a specific upstream commit
```

Never open a PR against `upstream`. Never push to `upstream`. Optioryx changes stay on `origin`.
