FROM node:20-alpine
WORKDIR /deps
COPY package.json .
RUN npm install
WORKDIR /app
COPY . .
ENV PATH="/deps/node_modules/.bin:$PATH"
ENV NODE_PATH="/deps/node_modules"
CMD ["node", "index.js"]