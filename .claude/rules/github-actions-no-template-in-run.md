---
description: When writing or editing GitHub Actions workflows or composite actions
globs: .github/**/*.yml
---

# No template expressions in `run:` blocks

Never use `${{ }}` expressions directly inside `run:` blocks. This applies to all expression contexts: `inputs.*`, `github.*`, `needs.*.outputs.*`, `steps.*.outputs.*`, and `env.*`.

Instead, pass values through `env:` mappings at the step or job level, then reference them as quoted shell variables (`"$VAR_NAME"`) in the `run:` block.

## Why

GitHub Actions evaluates `${{ }}` expressions via string interpolation before the shell sees the command. A malicious or unexpected value (e.g. a branch name containing `"; rm -rf /; echo "`) gets injected directly into the shell command. Using `env:` mappings causes the value to be set as an environment variable, which the shell handles safely.

## Correct

```yaml
- name: Deploy stack
  env:
    AWS_REGION_INPUT: ${{ inputs.aws-region }}
    GITHUB_REF_NAME: ${{ github.ref_name }}
    ENV_SHORT: ${{ needs.set-env-short.outputs.env-short }}
  run: |
    aws cloudformation deploy \
      --stack-name "flux-${ENV_SHORT}" \
      --region "$AWS_REGION_INPUT"
```

## Incorrect

```yaml
- name: Deploy stack
  run: |
    aws cloudformation deploy \
      --stack-name flux-${{ needs.set-env-short.outputs.env-short }} \
      --region ${{ inputs.aws-region }}
```

## Notes

- `${{ }}` is fine in `if:`, `with:`, `uses:`, and YAML-level `env:` mappings (the value side) -- just never inside `run:`.
- Always quote shell variable references (when possible): `"$VAR"` not `$VAR`.
- Workflow-level `env:` constants (e.g. `${{ env.AWS_DEFAULT_REGION }}`) are technically safe if they contain only hardcoded values, but should still use `$VAR` shell syntax for consistency since the env var is already available to the shell.
