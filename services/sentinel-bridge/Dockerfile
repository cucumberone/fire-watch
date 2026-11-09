# sentinel-bridge image.
# Multi-stage: install prod deps, then copy only src + node_modules.
FROM docker.io/library/node:22-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json* ./
# No lockfile is committed yet; `npm install` is acceptable for a first build.
# Replace with `npm ci` once package-lock.json exists.
RUN npm install --omit=dev --no-audit --no-fund

FROM docker.io/library/node:22-alpine
WORKDIR /app

# Run as an arbitrary UID: OpenShift assigns one, and 65532 matches the
# deployment's runAsUser. No shell, no package manager needed at runtime.
ENV NODE_ENV=production
COPY --from=deps /app/node_modules ./node_modules
COPY package.json ./
COPY src ./src

USER 65532:65532
EXPOSE 8081
ENTRYPOINT ["node", "src/index.js"]
