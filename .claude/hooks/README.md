# Hooks

## README-before-PR check

Enforces the "always check README.md before opening a PR" rule from `CLAUDE.md` automatically, instead of relying on it being remembered.

- **Config**: `.claude/settings.json` → `hooks.PreToolUse` (matcher `Bash`, `if: "Bash(gh pr create*)"`).
- **Type**: `agent` — runs a verification pass right before any `gh pr create` command executes. The agent reads the diff against `develop`, reads the current root `README.md`, and blocks the PR creation if the README is stale or structurally inconsistent with the diff (with an explanation of what needs updating).
- Only fires on `gh pr create*` commands — it does not run on every Bash call.

To disable or adjust, edit the hook entry in `.claude/settings.json` directly, or run `/hooks` inside a Claude Code session in this repo.
