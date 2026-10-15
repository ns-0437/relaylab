# Continuous integration

The **Build and unit checks** workflow runs on pull requests and pushes to main.
It installs the lockfile with dependency lifecycle scripts disabled, typechecks
application and test code, builds the application, and runs unit tests on Node 24.
Permissions are read-only; no deployment credentials are supplied.

The publication check also verifies tracked-file hygiene and local README links.
Its limited credential-pattern matching is not a comprehensive secret scan.

Integration, crash, and Kubernetes tests require real infrastructure and are not
part of this workflow. Run them using the quickstart and Kubernetes runbook.
Never interpret the badge as end-to-end infrastructure certification.

The workflow uses major-version action tags. They are not immutable action pins.
Failures must be investigated; do not remove assertions merely to obtain green CI.
