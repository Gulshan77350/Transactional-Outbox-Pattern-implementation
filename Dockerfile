FROM node:24-alpine

WORKDIR /app
ENV NODE_ENV=production

COPY package.json package-lock.json ./
RUN npm ci --omit=dev --ignore-scripts && npm cache clean --force

COPY src ./src

USER node

# Each compose service overrides this with its own entrypoint.
CMD ["node", "src/entrypoints/api.js"]
