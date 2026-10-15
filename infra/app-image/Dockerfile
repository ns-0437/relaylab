# Phase 3A Part B: the application image for the two long-lived
# coordination services (publisher-main.ts, consumer-main.ts) deployed to
# Kubernetes. Same EXPLICIT pinned base as infra/verify-image/Dockerfile
# (`node:24.21.0-alpine3.24`) -- never `node:24`, `node:lts`, or
# `node:latest`. Built from the REPO ROOT as build context (needs
# package.json/package-lock.json/tsconfig.json/src), via
# infra/k8s/scripts/01-build-image.sh, never implicitly built/pulled.
#
# This image carries ONLY the publisher/consumer entrypoints. The
# one-shot CLI workers (src/worker-main.ts, src/restore-worker-main.ts)
# are DELIBERATELY NOT part of this image -- they stay on the host (they
# need a local Git repository and the host's own Docker verification
# adapter; see docs/KUBERNETES.md "deployment boundary").

# ---- deps+build stage: full devDependencies, compiles src/ -> dist/ ----
FROM node:24.21.0-alpine3.24 AS build
WORKDIR /app
COPY package.json package-lock.json tsconfig.json ./
RUN npm ci
COPY src ./src
RUN npm run build

# ---- prod-deps stage: a SEPARATE, clean install with devDependencies omitted ----
FROM node:24.21.0-alpine3.24 AS prod-deps
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev

# ---- runtime stage: no compiler, no devDependencies, non-root ----
FROM node:24.21.0-alpine3.24
WORKDIR /app
ENV NODE_ENV=production
# Alpine's own node:* image already defines a non-root `node` user
# (uid/gid 1000) -- reused rather than inventing a new one, same
# convention as infra/verify-image/Dockerfile.
USER node
COPY --chown=node:node package.json ./
COPY --chown=node:node --from=prod-deps /app/node_modules ./node_modules
COPY --chown=node:node --from=build /app/dist ./dist
# No ENTRYPOINT/CMD: which service this container runs (publisher vs.
# consumer) is supplied as an explicit `command`/`args` by the Kubernetes
# Deployment manifest, never baked into the image -- the SAME image backs
# both Deployments. See infra/k8s/manifests/40-publisher.yaml /
# 50-consumer.yaml.
