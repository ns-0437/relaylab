# Contributing

Read the [architecture](docs/ARCHITECTURE.md) and the relevant
[decision record](docs/decisions/) before changing ownership or replay behavior.

1. Describe a concrete failure or capability and its acceptance condition.
2. Keep changes scoped; do not rewrite applied SQL migrations.
3. Run build, typecheck, and unit tests on Node 24.
4. For store/messaging/execution changes, run relevant real-infrastructure tests
   sequentially against disposable storage. Include failure-path assertions.
5. Report commands, runtime, passes, failures, and skips. Label historical results.
6. Update runbooks when operations or recovery behavior changes.

Preserve receipt authorization, ownership fencing, commit-before-ACK ordering,
and identity binding between verified Git state and durable checkpoints.

Never include credentials, kubeconfig, generated environment files, raw private
traces, or unrelated workspace files. Read [SECURITY.md](SECURITY.md).

The initial import groups existing work by responsibility. Subsequent PRs should
represent actual changes, not empty commits or invented development history.
No license grant has been selected for this repository; public visibility alone
does not grant an open-source license.
