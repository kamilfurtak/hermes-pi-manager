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

**Zero agent turns for anything but the result.** A durable outbox sends
messaging notices through the host's `send_message_tool`; ordinary Desktop
chats receive native in-app notices. Neither path invokes a model or registers
a `send_message` tool. Desktop notices replace one toast per task and expire
after 20 seconds; they are not persistent transcript lines or OS alerts.

**One wake per task, carrying the verdict.** When a task reaches a terminal
state *after* verification, one continuation is requested for its originating
session. The wake
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

**Crash-safety over convenience.** A wake dispatch whose owner died, or whose
Desktop admission raised after a possible start, becomes `uncertain` and is
never automatically retried. A live dispatcher is preserved when another Hermes
process scans the shared registry. Acceptance is distinct from finishing the
turn; Desktop records the latter as a `wake_turn_finished` event.

## Requirements

- Hermes Agent **v0.21.0+** — earlier hosts have no `session_key` on
  `inject_message`, so terminal continuation is disabled with a logged error
  (the rest of the plugin still works).
- The `pi` binary on `PATH`. Without it the tools are hidden from the model
  entirely rather than offered and failing.
- For terminal continuation, the gateway-injection grant below.
- Ordinary Desktop/TUI support requires the native backend contract checked by
  `desktop_host.py` (verified on Hermes 0.21.1, cores `b2aa855b` and `13c58042`). A missing
  contract leaves delivery pending; it never redirects the result elsewhere.

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

## Delivery by host

| Host | Passive progress | Terminal continuation |
|---|---|---|
| Telegram/gateway | Existing native messaging adapter | Existing `inject_message(session_key=...)` |
| Classic interactive CLI | No passive progress destination when no platform/chat is present | Owning CLI's pending-input/interrupt rail |
| Ordinary Desktop/TUI | `notification.show` to the session's native transport | Native prompt admission in that same backend |

Desktop waits until the current turn, queued human prompts and scheduled native
continuation are clear. An absent owner does not spend the retry budget or block
other sessions. Reopening a durable conversation (including its compression tip, in the same profile)
can make pending delivery eligible again. A reused UI tab is not sufficient
identity. The grant above also gates Desktop turns.

Restart/reconnect the affected Desktop backend and reload other long-lived
Hermes processes after upgrading this plugin. Existing Python processes retain
their loaded code. Check active work before restarting; an already `accepted`
or `uncertain` historic wake is not automatically replayed by an upgrade.

## Host internals

These files call Hermes internals rather than the documented `PluginContext`
surface, because the public API exposes no equivalent:

- `host_adapter.py` → `tools.send_message_tool` — the passive notification rail.
- `lsp_check.py` → `agent.lsp.get_service` — semantic verification.
- `desktop_host.py` → the already loaded `tui_gateway.server` session table,
  native event transport and `_run_prompt_submit(..., terminal_callback=...)`.
  It also checks the existing injection-grant predicate. This is a guarded
  compatibility adapter, not official generic Desktop `inject_message` support.
- `wake_worker.py` reads the context's manager to determine whether this process
  actually has the gateway injector.

Hermes upgrades require rechecking these boundaries. Missing Desktop capability
or session ownership leaves rows pending; uncertain admission requires diagnosis.
No second backend is created to steal an owned session, no Bot Chat substitution
is made, and the retired private completion queue is not restored.

## Development

```bash
hermes plugins doctor . --ci                 # manifest + registration contract
cd tests && for f in test_*.py; do python3 -m unittest "${f%.py}" -q; done
```

For a manual channel check, see [the notification and continuation smoke prompt](docs/manual-channel-smoke.md).
The first progress notice is eligible after 90 seconds, later ones at least
180 seconds apart, and each requires observed progress. A brief task normally
produces only its terminal notice. Classic CLI continuation does not imply
passive progress delivery; Desktop/TUI and messaging have distinct adapters.

## License

MIT — see [LICENSE](LICENSE).
