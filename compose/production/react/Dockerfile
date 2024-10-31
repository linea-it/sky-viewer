FROM node:22.11.0-bookworm-slim AS base

ENV GENERATE_SOURCEMAP=false
ENV NODE_OPTIONS=--max_old_space_size=8192

# COPY ./compose/production/react/start /start
# RUN sed -i 's/\r$//g' /start
# RUN chmod +x /start

RUN mkdir /app && chown -R node:node /app

COPY ./frontend /app
WORKDIR /app

# USER node

RUN yarn -v
RUN yarn --non-interactive --ignore-optional --network-timeout 500000
RUN yarn run build
CMD ["yarn", "start"]