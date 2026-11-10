# http-ingress image.
FROM docker.io/library/node:22-alpine AS deps
# Paths are relative to the REPO ROOT, because the build context is the root (see
# .github/workflows/http-ingress.yaml): the image needs schemas/, which sits
# outside this service directory.
WORKDIR /app
COPY services/http-ingress/package.json services/http-ingress/package-lock.json* ./
RUN npm install --omit=dev --no-audit --no-fund

FROM docker.io/library/node:22-alpine
WORKDIR /app
ENV NODE_ENV=production
COPY --from=deps /app/node_modules ./node_modules
COPY services/http-ingress/package.json ./
COPY services/http-ingress/src ./src
# The ingest contract is read AT RUNTIME, not inlined at build time
# (docs/event-schema.md). normalise.js resolves it as
#   join(dirname(src/normalise.js), '..', '..', '..', 'schemas', ...)
# which from /app/src is /schemas — NOT /app/schemas. The destination is therefore
# absolute. Getting this wrong yields an image that starts fine and then answers
# every EVT_* request with ENOENT, so the path is load-bearing.
COPY --chown=65532:65532 schemas /schemas

USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["node", "src/index.js"]
