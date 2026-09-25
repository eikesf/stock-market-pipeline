#!/usr/bin/env bash
# PreToolUse guard: refuse `gh pr create` when the branch changes code but not README.md.
#
# Reads the hook input JSON on stdin and only ever acts on a `gh pr create` command; anything
# else exits silently so the normal permission flow is untouched. Fails open by design: any
# missing tool, unreadable input or git error exits 0 rather than blocking real work.

set -uo pipefail

# Paths that mean "this branch changed how the project is built, configured or behaves".
CODE_PATH_PATTERN='^(src/|docker/|airflow/|soda/|Makefile$|pyproject\.toml$)'

input="$(cat)" || exit 0

command_line="$(printf '%s' "$input" | jq -r '.tool_input.command // empty' 2>/dev/null)" || exit 0
[ -n "$command_line" ] || exit 0

# Only a PR-opening command is in scope for this check.
case "$command_line" in
*"gh pr create"*) ;;
*) exit 0 ;;
esac

project_dir="${CLAUDE_PROJECT_DIR:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." 2>/dev/null && pwd)}"
[ -n "$project_dir" ] || exit 0
cd "$project_dir" 2>/dev/null || exit 0

if git rev-parse --verify --quiet develop >/dev/null 2>&1; then
    base_ref="develop"
elif git rev-parse --verify --quiet origin/develop >/dev/null 2>&1; then
    base_ref="origin/develop"
else
    exit 0
fi

merge_base="$(git merge-base "$base_ref" HEAD 2>/dev/null)" || exit 0
[ -n "$merge_base" ] || exit 0

changed_files="$(git diff --name-only "$merge_base...HEAD" 2>/dev/null)" || exit 0
[ -n "$changed_files" ] || exit 0

# README already updated alongside the code: nothing to enforce.
# -F matters: without it the dot is a regex wildcard and e.g. "READMEXmd" would match.
if printf '%s\n' "$changed_files" | grep -qxF 'README.md'; then
    exit 0
fi

code_changes="$(printf '%s\n' "$changed_files" | grep -E "$CODE_PATH_PATTERN")" || exit 0
[ -n "$code_changes" ] || exit 0

reason="README.md was not updated, but this branch changes code:
$(printf '%s\n' "$code_changes" | sed 's/^/  - /')

Read the root README.md in full and update it to match the diff (commands, config/env vars,
architecture/layers, or behavior), fitting the existing section structure. Then retry opening
the PR. If the README genuinely needs no change, say so explicitly and retry."

jq -n --arg reason "$reason" '{
  hookSpecificOutput: {
    hookEventName: "PreToolUse",
    permissionDecision: "deny",
    permissionDecisionReason: $reason
  }
}' 2>/dev/null || exit 0
