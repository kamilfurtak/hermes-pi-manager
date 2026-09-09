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

## Live activity in Desktop

The optional Desktop half renders `::pi-live{task="pi-…"}` as an updating card
inside the assistant's start acknowledgement. `pi_task` supplies that directive
for native Desktop/TUI origins. A Desktop with the frontend installed renders
it; other clients retain their existing notices and terminal continuation.

Expand **Pokaż przebieg** to follow the same chronological log as the CLI:
timestamps, executed commands/arguments, visible Pi messages, incremental output
from each tool, durations and errors. Desktop uses its native `LogView` component.
New complete lines arrive about once a second, including while the parent
conversation is idle. Reading earlier lines pauses auto-scroll; **Do najnowszych**
resumes it. Collapsing the card stops downloading log content; the compact status,
latest message preview and completed-tool counter continue to refresh.

The expanded view requests only new bytes after its last cursor, retains up to
256 KiB of recent text and explicitly marks a reset after rotation or a display
limit. Repeated cumulative RPC results do not duplicate output. A navigation or
connection change discards in-flight replies. Polling waits for the final log
flush as well as execution/verification settlement. This is a bounded display
log, not an unbounded raw Pi session: private thinking is excluded and complete
lines are redacted before publication.

Older workers/backends without the new transcript retain the existing formatted
recent-message view (eight history entries, foldable completed tool results and
foreground-tool output). The counter always counts completed tool calls,
including overlapping calls, not messages or an estimated percentage.
Execution and verification stay separate.

Buttons, status dots and tool logs use the corresponding Desktop SDK components;
older SDKs retain the text/HTML fallback. The collapsed preview starts at the
beginning of the latest Pi message. This is a plugin transcript contribution:
Desktop does not expose its built-in subagent roster as an external provider API.

Install the Python plugin on the backend as usual and reload that backend after
updating it. On the computer running Desktop, copy `desktop/plugin.js` into
`~/.hermes/desktop-plugins/pi-manager/plugin.js`, then use **Settings → Plugins**
or **⌘K → Reload desktop plugins**. This reload replaces frontend contributions
without restarting the backend. No frontend build or npm install is needed.
If both halves run on one computer, the existing unified plugin directory is
also discovered; enable its Desktop half in Settings.

For Desktop over SSH, the JS file belongs on the laptop and the Python files
belong on the remote host. `ctx.rest` uses Desktop's existing authenticated
connection to native `hermes serve`; no additional listener or external WebUI
is required. The read-only endpoint rejects tasks from another conversation or
profile, supports compression continuations, and never initializes a manager.
The frontend drops in-flight responses when navigation changes its owner.

`state/pi-manager/activity/` contains disposable, redacted, private snapshots.
One coalescing writer per manager publishes them atomically; event handling
never performs network or filesystem I/O for the view. Snapshots expire after
seven days and are capped at 256 files; each is at most 128 KiB. A presentation
failure does not change task settlement, verification, notices or wake policy.
Older tasks without snapshots still display their registry status.

Validation: `python -m unittest discover -s tests`; for the React view,
`cd desktop && npm ci && npm test`. The latter dependencies are only for tests;
Desktop supplies React and the SDK at runtime.

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
| Telegram/gateway | Native messaging adapter; latest visible stage and completed tool count | Existing `inject_message(session_key=...)` |
| Classic interactive CLI | Native subagent dock and live inspector; passive text fallback on older hosts | Owning CLI's normal FIFO, after the parent is idle |
| Ordinary Desktop/TUI | `notification.show` to the session's native transport | Native prompt admission in that same backend |

The CLI adapter adds Pi rows to the existing `SubagentMonitor` instance. It uses
the host's dock, theme, fullscreen viewer and keybindings: **Ctrl+T / F6** opens
the roster, **Enter** opens the selected live tail, **Esc** returns, and **F7**
collapses the dock. Native subagents retain their original rows and controls.
**s** queues guidance to Pi; **x**, then the native confirmation, stops it.
Opening/closing the viewer preserves the composer draft and does not stop Pi.
After successful attachment, `pi_task` tells the parent to end its turn without
a separate startup acknowledgement, unless the user explicitly requested one.
Older hosts retain the normal acknowledgement when attachment is unavailable.
The native spinner refreshes activity about once a second even while the parent
is idle or working on another request. Once no agents remain it stops repainting
the idle prompt. No model turn is used for monitoring.

Only the owning CLI process and conversation (or its compression tip) receive
Pi rows and notices. `/new`, a closed CLI or a foreign process cannot receive
them. When the native view is available, progress receipts do not also print
repeated messages above the prompt. Terminal notices still use the native
renderer, but wait until the inspector releases the terminal. They never cancel
prompt_toolkit's shared terminal queue on a delivery timeout. This prevents a
finished task from leaving a stale dock and hiding later conversation output.
On older hosts the existing passive text notices remain available.
Wakes wait for the parent, queued user input and native modals/inspector to clear;
they enter the normal FIFO, never the interrupt queue. A scope guard checks the
conversation again when a queued wake is consumed. A session switch can discard
an already admitted wake; it is not replayed into a different conversation.
CLI queue states are isolated from the older workers’ pending/leased states,
so long-lived processes with an older plugin cannot claim local notices.
The CLI adapter captures identity when a new task is dispatched: reopen the
CLI process to load an updated plugin before testing. Reconnecting Herdr to the
same surviving process does not reload Python. Desktop frontend changes require
copying/reloading its JS half as described above.

All three views use the same bounded, redacted projection for compact status.
The CLI inspector and expanded Desktop card additionally read an append-only event log: timestamps,
executed tool arguments, visible assistant text, incremental tool output,
per-tool durations and errors. Cumulative RPC tool updates are reconciled by
call ID, so parallel tools and repeated snapshots do not duplicate output.
Completed lines appear while a tool runs; an unfinished line is held until its
newline or the end of the message/result, then redacted before publication.
Private thinking and speculative tool arguments never enter this log.

The native viewer follows the newest **32 KiB** and supports its normal scrolling;
it no longer receives a replacement of the last eight compact entries. The
private `state/pi-manager/cli-transcripts/<task-hash>.log` preserves more history,
rotating at 2 MiB into one `.log.1` previous part. Both are 0600, retained for
seven days/up to 256 task pairs. Oversize lines or a saturated display queue
produce explicit omission markers; display I/O cannot change task outcomes.
Existing tasks without this log retain their previous snapshot-based preview.

Native Telegram provides message delivery rather than a terminal-style live inspector;
the plugin does not impersonate a built-in subagent to manufacture one.
Passive notices remain rate-limited, and execution/verification remain separate.
Tool errors appear in live output; a failed command that Pi handles is not
automatically a failed task. Task failures and verifier outcomes use terminal
notices without waiting for the progress interval (delivery still requires a
live channel and the normal outbox drain).

Execution ownership is separate from notification ownership. A plugin-owned OS
lock protects each task from before STARTING through verification, so loading a
second Hermes host cannot reopen live Pi work. Legacy tasks with a live worker
PID are left alone; lock ownership is released on completion or process exit.
Recovery still validates session identity after an owner is gone. These are
Unix advisory locks alongside the registry, not changes to Hermes source.

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
- `cli_host.py` → the owning CLI application and normal input FIFO, with scope
  checks at enqueue and consumption; passive notices use the native renderer.
- `cli_monitor.py` → instance-local adapters for native `SubagentMonitor.refresh`
  and `control`. The native UI/keybindings are reused; methods are restored on
  teardown. No native delegation registry entries or Hermes files are changed.
  Readable tails are private, redacted projections, not raw Pi session transcripts.
- `live_transcript.py` → a plugin-owned display log assembled from RPC events.
  It reuses the activity writer, preserves tool identity and incremental output,
  and keeps up to 256 task log pairs for seven days. It never controls execution.

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
The first passive progress notice is eligible after 90 seconds, later ones at least
180 seconds apart, and each requires observed progress. A brief task normally
produces only its terminal notice. The native CLI dock and Desktop card update
independently, about once a second. CLI, Desktop/TUI and messaging have distinct
host adapters; terminal continuation remains a separate path.

## License

MIT — see [LICENSE](LICENSE).
