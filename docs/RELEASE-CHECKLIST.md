# Publication and release checks

Run `npm run check:publication` from the repository root. It checks tracked paths
for generated runtime files, recognizes a small set of high-confidence credential
formats, and checks local README links. Potential values are never printed.
It is a useful guard, not comprehensive secret detection.

Before sharing a new revision:

- Run Node 24 typecheck, build, and unit tests.
- Run affected real-service scenarios when application behavior changes.
- Review `git diff --stat` and the actual staged diff.
- Keep generated credentials, kubeconfig, traces, and temporary output untracked.
- Label historical evidence and current results separately.
- Verify the README describes the current deployment boundary.
- Use a real PR with behavior and validation details; do not invent review history.

CI runs publication checks once that script is present. It does not create GitHub
releases, merge PRs, publish packages, or deploy infrastructure.
