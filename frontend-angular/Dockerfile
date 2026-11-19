# Etapa 1: Build de Angular 21
FROM node:22-alpine AS builder

WORKDIR /app
COPY package.json ./
RUN npm install --legacy-peer-deps

COPY . .
RUN npm run build

# Etapa 2: Nginx con Reverse Proxy para Rust API/SSE y Airflow REST API
FROM nginx:1.31.9-alpine

COPY nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=builder /app/dist/swiftroute-angular-dashboard/browser /usr/share/nginx/html

EXPOSE 80
CMD ["nginx", "-g", "daemon off;"]
