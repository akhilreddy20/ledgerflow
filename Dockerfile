# One image per service, built from the monorepo:
#   docker build --build-arg SERVICE=ledger -t ledgerflow/ledger .
FROM node:22-alpine AS build
WORKDIR /app
COPY package.json package-lock.json tsconfig.base.json ./
COPY libs/common/package.json libs/common/
COPY services/ledger/package.json services/ledger/
COPY services/payments/package.json services/payments/
COPY services/notifications/package.json services/notifications/
COPY services/psp-simulator/package.json services/psp-simulator/
RUN npm ci --no-audit --no-fund
COPY libs ./libs
COPY services ./services
RUN npm run build && npm prune --omit=dev

FROM node:22-alpine
ARG SERVICE
ENV NODE_ENV=production SERVICE=${SERVICE}
WORKDIR /app
COPY --from=build /app/node_modules ./node_modules
COPY --from=build /app/package.json ./
COPY --from=build /app/libs/common/package.json ./libs/common/
COPY --from=build /app/libs/common/dist ./libs/common/dist
COPY --from=build /app/services/${SERVICE}/package.json ./services/${SERVICE}/
COPY --from=build /app/services/${SERVICE}/dist ./services/${SERVICE}/dist
COPY --from=build /app/services/${SERVICE}/migrations ./services/${SERVICE}/migrations
USER node
# --require loads OpenTelemetry before any other module so http/pg/redis get auto-instrumented.
CMD ["sh", "-c", "exec node --enable-source-maps --require ./libs/common/dist/otel.js services/${SERVICE}/dist/main.js"]
