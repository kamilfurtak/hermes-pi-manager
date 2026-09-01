## pi-manager installed

**One more step is required** — terminal continuation needs an explicit grant
that the install flow cannot ask for (it is a config flag, not a declared
capability). Without it the wake back into your session is refused and tasks
end in `wake_exhausted`:

```yaml
plugins:
  entries:
    pi-manager:
      allow_gateway_injection: true
```

Then check the prerequisites:

- `pi` on `PATH` — without it the tools stay hidden from the model.
- Hermes **v0.21.0+** — older hosts log an error and run without continuation.

Start a task with `pi_task`, and let the single terminal wake come back to you.
There is no blocking wait tool by design.
