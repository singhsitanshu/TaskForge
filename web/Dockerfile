FROM node:22-alpine AS build
WORKDIR /app
ARG VITE_GRAFANA_PORT=3001
ARG VITE_PROMETHEUS_PORT=9090
ENV VITE_GRAFANA_PORT=$VITE_GRAFANA_PORT
ENV VITE_PROMETHEUS_PORT=$VITE_PROMETHEUS_PORT
COPY package.json package-lock.json ./
RUN npm ci
COPY . .
RUN npm run build

FROM nginx:1.27-alpine
COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 8080
