# pi-manager

A Hermes plugin that delegates work to the [Pi](https://github.com/badlogic/pi-mono) coding agent
and supervises it — durably, without blocking, and without spending the
orchestrator's turns.

Pi runs as a long-lived RPC subprocess. The plugin owns its lifecycle: a
SQLite registry that survives restarts, a layered stall watchdog, a separate
verification step, a passive notification rail, and exactly **one** wake back
into the session that dispatched the task.

## Why not `delegate_task` or the subagent lifecycle API?

Hermes' native subagent API manages **in-process** children. Pi is an external
process, so it is out of scope for that API — and the API is weaker on the axes
this plugin exists for: handles do not survive a restart, per-launch timeouts
are rejected, there are no hooks for observing intermediate progress, and
`wait()` blocks. Blocking is the thing to avoid: a blocked turn is a burnt turn.

## Design invariants

**Zero agent turns for anything but the result.** Progress notices go out
through a durable outbox that calls the host's `send_message_tool` directly —
no tool registry entry, no model-visible surface, no turn. The plugin
deliberately registers no `send_message` tool.

**One wake per task, carrying the verdict.** When a task reaches a terminal
state *after* verification, the originating session is resumed once. The wake
carries the gate result and the semantic check, so a clean outcome needs no
follow-up call at all:

```
Task `pi-abc` reached its terminal state (execution_state=SETTLED,
verification_state=PASS). Gate: gate passed (exit 0). Semantic check: no LSP
errors detected in 4 touched file(s). This is the complete outcome — continue the
parent workflow autonomously without re-reading the task.
```

**Semantic verification Pi cannot do itself.** Pi ships no language server.
After settlement the plugin runs the host's LSP servers over the files the task
touched (`git diff` + untracked) and folds the result into the same wake. This
is an in-process call — zero tokens.

**Execution and verification are distinct axes.** `execution_state` says
whether the agent finished; `verification_state` says whether the gate passed.
A task can settle cleanly and still fail its gate.

**Crash-safety over convenience.** A wake dispatch that survives a process
death becomes `uncertain` and is never retried: the gateway may already have
accepted it, and a duplicate orchestrator turn is the failure this state
exists to prevent.

## Requirements

- Hermes Agent **v0.21.0+** — earlier hosts have no `session_key` on
  `inject_message`, so terminal continuation is disabled with a logged error
  (the rest of the plugin still works).
- The `pi` binary on `PATH`. Without it the tools are hidden from the model
  entirely rather than offered and failing.
- For terminal continuation, the gateway-injection grant below.

## Install

```bash
hermes plugins install kamilfurtak/hermes-pi-manager --enable
```

Pin a revision for reproducibility:

```bash
hermes plugins install kamilfurtak/hermes-pi-manager --ref <full-commit-sha> --enable
```

## Required configuration

Terminal continuation needs an explicit grant. It is a plain config flag, not
a declared capability, so **the install flow will not prompt for it** — without
this the wake is refused and tasks end in `wake_exhausted`:

```yaml
plugins:
  entries:
    pi-manager:
      allow_gateway_injection: true
```

## Tools

| Tool | Purpose |
|---|---|
| `pi_task` | Start one Pi task; returns immediately |
| `pi_status` | Current registry state for a task |
| `pi_digest` | ~2 KB account of what a task DID, instead of its transcript |
| `pi_abort` | Kill switch: RPC abort → SIGTERM → SIGKILL |
| `pi_steer` | Send a steer command to a live task |
| `pi_resume` | Re-run the recovery algorithm for one task |
| `pi_verify` | Run the verification step once a task has settled |

There is deliberately no blocking wait tool.

## Host internals

Two files call Hermes internals rather than the documented `PluginContext`
surface, because the public API exposes no equivalent:

- `host_adapter.py` → `tools.send_message_tool` — the passive notification rail.
- `lsp_check.py` → `agent.lsp.get_service` — semantic verification.

Both are isolated to a single file and degrade to "unavailable" on any error,
so a Hermes upgrade that moves them costs a feature, never a crash. Everything
else uses documented APIs only.

## Development

```bash
hermes plugins doctor . --ci                 # manifest + registration contract
cd tests && for f in test_*.py; do python3 -m unittest "${f%.py}" -q; done
```

## License

MIT — see [LICENSE](LICENSE).
