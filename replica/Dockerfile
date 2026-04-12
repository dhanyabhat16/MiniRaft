FROM node:20-alpine
WORKDIR /deps
COPY package.json .
RUN npm install
WORKDIR /app
ENV PATH="/deps/node_modules/.bin:$PATH"
ENV NODE_PATH="/deps/node_modules"
CMD ["nodemon", "index.js"]