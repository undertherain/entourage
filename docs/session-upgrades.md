# Upgrading long-lived sessions

2026-09-22. Implemented as `rebind` on the session backend commit, `upgrades` on
`Executable`, and lazy migration in `Dispatcher`. Enumeration (`list_sessions`)
landed alongside for runner use.

## The problem

A definition contract fingerprints the entrypoint, declared resources and
config. Any prompt or config edit is therefore a new version, and a new version
does not serve sessions bound to the old one. A resident Concierge session is
months long; the first prompt tweak would strand it. `development: true` skips
fingerprints but still binds config.

## The mechanism

A commit may carry `rebind="concierge:v2"`. From that checkpoint the session
belongs to the new definition. The saved state is the first state v2 sees; the
session ID, revision history and retained input IDs are unchanged, so nothing is
replayed; unincorporated mail stays pending for v2. The target must be bound in
the store, must differ from the current definition, and cannot accompany
completion.

Two ways to reach it:

1. **Voluntary handoff.** Old code proposes `context.propose(state, rebind=...)`
   at a point it chooses, typically after compacting its own history.
2. **Lazy migration.** The new version declares how to leave old ones:

```yaml
definition: concierge:v2
entrypoint: agent.py:resume
upgrades:
  concierge:v1: agent.py:migrate
```

`migrate(context, state, mail)` receives the old state and any pending mail and
returns an ordinary proposal; the dispatcher forces `rebind` to its own version.
`context.upgrading_from` names the old definition, `context.definition` and
`context.config` are the new version's. Old code need not be loadable. A
dispatcher refuses to both serve a definition and upgrade it away.

## Why lazy is correct, not a compromise

The dispatcher claims superseded sessions only when they are ready: pending
mail, a due deadline or an expired attempt. A quiet parked session is not
migrated until it wakes. That is the only moment a definition matters, because
the system prompt is re-sent on every model call rather than stored in state.
There is nothing to apply to a session that is not running.

It is also the safe moment. Migration runs as a normal leased activation between
turns, never inside a tool loop, and commits atomically with a revision bump. If
it fails, the old checkpoint stands and the session retries after lease expiry.

## Compaction is the natural handoff

Compaction rewrites the conversation projection into a summary. Doing the rebind
at the same time has two benefits. Changing the system prompt invalidates the
prompt-cache prefix for the whole context; compaction discards that prefix
anyway, so bundling them costs one re-cache instead of two. And the new persona
starts from a summary rather than inheriting turns written under the old one.

The `migrate` function is where that lives. It has the old state, can compact it
into the handoff shape v2 expects, and may leave the newly arrived mail
unincorporated so v2 answers it under the new prompt.

Compaction for context-size reasons remains an application concern inside the
handler and does not need a rebind.

## What this does not do

- It does not split prompt files out of the code identity. A prompt edit still
  requires a new version and a migration, even when the state shape is unchanged.
  A `migrate` that returns the state unchanged is one line; a later manifest
  distinction between fingerprinted code and adoptable resources could remove
  even that.
- The shard runner does not yet emit an `upgrade` event or warm-migrate eager
  sessions. `list_sessions(executable="concierge:v1")` lets it find them; whether
  to wake them early is a residency policy decision.
- Chains are explicit. v3 must declare upgrades from v1 and v2 if both may still
  exist, or v1 sessions wait until they wake under a dispatcher that knows them.
