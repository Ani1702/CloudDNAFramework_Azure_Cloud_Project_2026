FROM node:22-alpine

RUN apk add --no-cache python3 make g++

WORKDIR /app/apps/backend/.medusa/server

RUN corepack enable && corepack prepare pnpm@12.4.1 --activate

COPY apps/backend/.medusa/server/package.json ./
COPY apps/backend/.medusa/server/pnpm.yaml ./

RUN pnpm install --prod --no-frozen-lockfile --ignore-scripts && \
    pnpm add applicationinsights --ignore-scripts

COPY apps/backend/.medusa/server .

RUN echo 'const appInsights = require("applicationinsights"); if (process.env.APPLICATIONINSIGHTS_CONNECTION_STRING) { appInsights.setup(process.env.APPLICATIONINSIGHTS_CONNECTION_STRING).setAutoCollectRequests(true).setAutoCollectPerformance(true).setAutoCollectExceptions(true).setAutoCollectDependencies(true).start(); }' > appinsights-init.js

EXPOSE 9000

ENV NODE_ENV=production

CMD ["node", "-r", "./appinsights-init.js", "node_modules/@medusajs/cli/cli.js", "start"]