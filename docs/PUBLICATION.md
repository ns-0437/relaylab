# Publication provenance

RelayLab was developed locally before this public repository existed. The first
implementation PR imports that existing implementation in responsibility-based
commits. Those commits are made at publication time, not backdated to suggest
an earlier public development history. Intermediate import commits can depend
on later commits in the same PR; validation applies to the complete import.

Subsequent publication PRs add actual README/design assets, CI, onboarding,
contribution templates, security guidance, repository checks, and validation
documentation. They are maintainer-created changes; merge does not imply an
independent reviewer approved them.

Historical milestone reports live in `evidence/phase1-evidence.md` and the
development documents. They are imported evidence, not fresh results from this
publication session. [VALIDATION.md](VALIDATION.md) records fresh checks separately.

The publication copy excludes generated credentials, kubeconfig, process IDs,
raw local trace captures, dependency/build directories, and raw machine-specific
test logs. Some machine paths and generated credentials in historical documents
are redacted. Source implementation and SQL migrations are imported unchanged.

The working source directory is preserved separately. No existing Git history
was rewritten. No external company affiliation or proprietary source is claimed.
