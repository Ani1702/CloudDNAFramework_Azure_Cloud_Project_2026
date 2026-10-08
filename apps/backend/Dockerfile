FROM node:22-alpine
WORKDIR /app
RUN corepack enable && corepack prepare pnpm@12.4.1 --activate
COPY package.json pnpm-lock.yaml pnpm-workspace.yaml ./
COPY apps/backend/package.json apps/backend/package.json
RUN pnpm install --frozen-lockfile
COPY apps/backend/.medusa/server apps/backend/.medusa/server
COPY apps/backend/.env apps/backend/.env
WORKDIR /app/apps/backend
EXPOSE 9000
CMD ["pnpm","start"]
COPY apps/backend/medusa-config.ts apps/backend/medusa-config.ts
COPY apps/backend/.medusa/server/medusa-config.js apps/backend/medusa-config.js
