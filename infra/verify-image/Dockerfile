# Phase 2C: the verification execution image. An EXPLICIT, pinned Node
# 24 tag -- `node:24.21.0-alpine3.24`, matching this project's own
# Node 24.21.0 install exactly (docs/PROGRESS.md Phase 2A.1) -- never
# `node:24`, `node:lts`, or `node:latest` (all floating). Building this
# image records the RESOLVED image ID actually produced
# (`docker image inspect --format '{{.Id}}'`) -- see
# src/worker/docker-verify.ts and docs/EXECUTION.md -- rather than
# trusting the tag name alone at verification time.
#
# This image carries NO verification logic of its own: the exact
# command (`node verify.js`) and its working directory are supplied as
# explicit `docker run` arguments by the adapter, never baked into a
# CMD/ENTRYPOINT -- the image is a pinned RUNTIME, not a task-specific
# script carrier.
FROM node:24.21.0-alpine3.24

# Alpine's own node:* image already defines a non-root `node` user
# (uid/gid 1000) -- reused rather than inventing a new one. Combined
# with `--user` and `--read-only` at `docker run` time (belt-and-
# suspenders: the adapter passes `--user node` explicitly too, so this
# USER line is not the ONLY thing enforcing it).
USER node
WORKDIR /verify
