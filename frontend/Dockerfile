FROM node:20.17-alpine AS build

WORKDIR /app

COPY frontend/package*.json ./
RUN npm ci --ignore-scripts

COPY frontend ./
RUN npm run build

FROM nginxinc/nginx-unprivileged:1.27-alpine

ENV PORT=8080
ENV API_UPSTREAM=http://backend:8000

COPY --chown=101:101 frontend/nginx.conf /etc/nginx/templates/default.conf.template
COPY --from=build --chown=101:101 /app/dist /usr/share/nginx/html

EXPOSE 8080

CMD ["nginx", "-g", "daemon off;"]
