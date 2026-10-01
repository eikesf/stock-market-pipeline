# Hooks

## README-before-PR check

Enforces the "always check README.md before opening a PR" rule automatically, instead of relying on it being remembered.

- **Config**: `.claude/settings.json` → `hooks.PreToolUse` (matcher `Bash`, `if: "Bash(gh pr create*)"`).
- **Type**: `command`, running `.claude/hooks/check-readme-before-pr.sh`. Deterministic — no model call, no judgement.

### What it does

1. Reads the hook input JSON on stdin and extracts `tool_input.command`.
2. If that command does not contain `gh pr create`, it exits immediately with no output, so every other Bash command follows the normal permission flow untouched. The `if` filter in `settings.json` already narrows this, and the script re-checks so a filter change can never turn it into a blanket guard.
3. Otherwise it resolves the merge base against `develop` (falling back to `origin/develop` when no local `develop` exists) and lists the changed files with `git diff --name-only <merge-base>...HEAD`.
4. It denies the `gh pr create` call only when **both** hold:
   - at least one changed file counts as a code change — anything under `src/`, `docker/`, `airflow/`, `soda/`, or the `Makefile` or `pyproject.toml`; and
   - `README.md` is **not** among the changed files.
   The denial reason lists the offending paths so the fix is obvious.
5. In every other case it exits 0 with no output, which allows the command.

### Fails open

Any unexpected condition — `jq` or `git` missing, unreadable hook input, no `develop` ref, an empty diff — exits 0 with no output. A broken hook will never block work; the worst case is that the check silently stops running.

Note this replaced an earlier `agent`-type hook, which asked a model to make the judgement. That version denied unrelated Bash commands whenever the `if` filter let one through, because its prompt assumed every command it saw was a PR command. A script with an explicit early exit cannot fail that way.

### Managing it

Edit the hook entry in `.claude/settings.json`, or run `/hooks` inside a Claude Code session in this repo to review or disable it. Test it directly by piping sample input:

```bash
echo '{"tool_input":{"command":"gh pr create --title x"}}' | .claude/hooks/check-readme-before-pr.sh
```
