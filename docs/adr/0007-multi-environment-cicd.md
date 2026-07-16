# 7. Multi-environment CI/CD with a service principal

Date: 2026-07

## Status

Accepted

## Context

The lakehouse must be delivered across three environments (dev, staging,
production) with automated quality gates, in a single Databricks workspace (the
trial does not allow isolated workspaces). Deployments must not run under a
personal identity, and environment-specific and sensitive values must not be
committed.

## Decision

Deploy the bundle across three targets, each isolated in its own Unity Catalog
(`food_delivery_<env>`) with its own Lakebase project. Drive delivery with two
GitHub Actions workflows: **CI** (ruff, pytest, `bundle validate`) as a required
gate on pull requests, and **CD** mapping each branch to its environment
(`develop → dev`, `homolog → stg`, `main → prod`) with a manual approval gate on
production and on-demand redeploys via `workflow_dispatch`. Deploy as a dedicated
**service principal**. Inject configuration (Kafka endpoint, synced table pipeline
id) at deploy time from GitHub environment variables; never commit it.

Because the three targets share one workspace, give each a distinct `name_prefix`
and `root_path` so resource names and deployment state do not collide.

## Consequences

- Every change is linted and tested before merge, and promoted through
  environments by a repeatable, auditable process rather than manual deploys.
- Deploy identity is a service principal, not a person — jobs are not tied to an
  individual, matching production practice.
- Separation of concerns is explicit: platform provisioning (catalogs, service
  principal, grants) is bootstrap; the pipeline handles application deployment.
- Sharing one workspace required per-target naming and state isolation to avoid
  collisions — a constraint of the trial, documented rather than hidden.
- Configuration lives in the environment, not the code, keeping the public
  repository free of endpoints and identifiers.

## Alternatives considered

- **One workspace per environment.** The production-shaped ideal (full isolation),
  unavailable on the trial; approximated with a catalog and Lakebase project per
  environment plus per-target naming.
- **Personal access token for deploys.** Simpler, but ties deployments to an
  individual and is an anti-pattern for automated delivery.
- **Committing per-environment config.** Convenient but leaks endpoints and
  identifiers into a public repository; injection at deploy time avoids it.
