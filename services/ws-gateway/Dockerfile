# ws-gateway image.
FROM docker.io/library/node:22-alpine AS deps
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install --omit=dev --no-audit --no-fund

FROM docker.io/library/node:22-alpine
WORKDIR /app
ENV NODE_ENV=production
COPY --from=deps /app/node_modules ./node_modules
COPY package.json ./
COPY src ./src

USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["node", "src/index.js"]
