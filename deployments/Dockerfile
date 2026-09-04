# LinkIM 通用服务镜像：build 阶段按 ARG SVC 编译对应 cmd，运行阶段仅携带二进制。
# 用法：docker build -f deployments/Dockerfile --build-arg SVC=comet -t linkim/comet .
FROM golang:1.26-alpine AS build
WORKDIR /src
# 与宿主一致的模块代理（可被 --build-arg 覆盖）。
ARG GOPROXY=https://goproxy.cn,direct
ENV GOPROXY=${GOPROXY}
COPY go.mod go.sum ./
RUN go mod download
COPY . .
ARG SVC=account
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /app ./cmd/${SVC}

FROM alpine:3.20
# 内网服务间无 TLS，不需要 CA 证书包；仅建非 root 运行用户。
RUN adduser -D -u 10001 app
COPY --from=build /app /app
COPY configs /configs
USER app
WORKDIR /
ENTRYPOINT ["/app"]
