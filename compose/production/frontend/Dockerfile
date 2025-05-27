FROM node:22.11.0-bookworm-slim AS base
ENV GENERATE_SOURCEMAP=false
ENV NODE_OPTIONS=--max_old_space_size=8192
ENV NODE_ENV=production

FROM base AS dependencies
ENV GENERATE_SOURCEMAP=false
ENV NODE_OPTIONS=--max_old_space_size=8192
WORKDIR /app
COPY ./frontend/package.json /app
COPY ./frontend/yarn.lock /app
RUN yarn --non-interactive --ignore-optional --network-timeout 500000

FROM base AS builder
WORKDIR /app
COPY --from=dependencies /app/node_modules /app/node_modules
COPY ./frontend /app
RUN yarn run build

FROM base AS runner
WORKDIR /app
COPY --from=builder /app /app
RUN mkdir -p /app/.next/cache && chmod -R 777 /app/.next/cache

EXPOSE 3000
CMD ["yarn", "start"]
