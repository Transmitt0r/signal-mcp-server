# ---- Build stage ----
FROM node:22-alpine AS builder
WORKDIR /app
RUN corepack enable && corepack prepare pnpm@latest --activate
COPY pnpm-lock.yaml pnpm-workspace.yaml package.json ./
RUN pnpm install --frozen-lockfile
COPY tsconfig.json ./
COPY src/ src/
RUN pnpm build

# ---- Runtime stage ----
FROM node:22-alpine
WORKDIR /app
COPY --from=builder /app/dist ./dist
COPY --from=builder /app/package.json ./
EXPOSE 3100
ENV SIGNAL_HTTP_URL=http://127.0.0.1:8080
CMD ["node", "dist/index.js"]