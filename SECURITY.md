# Security and trust boundaries

RelayLab is a local prototype with a fixed, project-controlled verification
command. Do not run unknown repositories or expose its development services.

## What the controls cover

- Lease/epoch checks fence durable task mutations.
- Receipt authorization prevents another named owner from replaying a receipt.
- Git and artifact identity checks bind restored data to expected checkpoints.
- Verification uses non-root containers with resource/network restrictions.
- Final-state integrity checks detect persistent changes to verification input.

## What they do not cover

Caller names are not cryptographic authentication. Local worker/coordinator code
is trusted. Containers are not VM-grade hostile-code isolation. The extracted
input can be made writable by its owner; a tamper-then-restore attack can evade
the final-state check. Durable fencing does not stop arbitrary external effects.
The local cluster is not multi-tenant, highly available, or backed up by default.

## Repository hygiene

Generated Kubernetes credentials, kubeconfig, port-forward process files, raw
traces, dependency directories, and build products are excluded from publication.
Compose and .env.example contain deliberate local development credentials only.
Never reuse them for public or production services.

## Reporting

Do not put tokens, kubeconfig, private source, or exploitable credentials into a
public issue. If GitHub private vulnerability reporting is available on the
repository Security tab, use it. Otherwise open a minimal issue requesting a
private contact route without disclosing sensitive details. No response SLA or
bug bounty is offered.
