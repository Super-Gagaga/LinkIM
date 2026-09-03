# LinkIM 构建入口。帮助：make（或 make help）
# 依赖：go >= 1.22、golangci-lint、golang-migrate(migrate)、docker compose

GO ?= go
BIN_DIR := bin
COMPOSE_FILE := deployments/docker-compose.yml

# golang-migrate 的 MySQL DSN（S2 起使用；multiStatements 支持多语句迁移脚本）
MYSQL_DSN ?= mysql://root:linkim123@tcp(127.0.0.1:3307)/linkim?multiStatements=true

.PHONY: help build test lint proto migrate-up migrate-down compose-up compose-down clean

help: ## 显示所有可用目标
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z0-9_-]+:[^#]*## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":[^#]*## "}; {printf "  %-15s %s\n", $$1, $$2}'

build: ## 编译四个服务二进制到 bin/
	@mkdir -p $(BIN_DIR)
	$(GO) build -o $(BIN_DIR)/ ./cmd/...

test: ## 运行全部单元测试（含 race 检测）
	$(GO) test ./... -race -count=1

test-int: ## 运行集成测试（需先 compose-up + migrate-up，连 docker 依赖）
	$(GO) test -tags integration ./internal/account/ -race -count=1 -v

lint: ## 运行 golangci-lint 静态检查
	golangci-lint run

proto: ## 由 api/*.proto 生成 pkg/pb Go 代码（优先 buf，未安装则回退 protoc）
	@if command -v buf >/dev/null 2>&1; then \
		echo ">> buf generate"; \
		cd api && buf generate; \
	else \
		echo ">> buf 未安装，回退 protoc"; \
		protoc -I api --go_out=pkg/pb --go_opt=paths=source_relative api/protocol.proto; \
	fi
	@echo "等价 protoc 命令（buf 不可用时手动执行）:"
	@echo "  protoc -I api --go_out=pkg/pb --go_opt=paths=source_relative api/protocol.proto"

migrate-up: ## 应用全部数据库迁移（迁移文件自 S2 提供）
	migrate -path migrations -database "$(MYSQL_DSN)" up

migrate-down: ## 回滚最近一个数据库迁移
	migrate -path migrations -database "$(MYSQL_DSN)" down 1

compose-up: ## 启动本地依赖 MySQL/Redis/Kafka 并等待 healthy
	docker compose -f $(COMPOSE_FILE) up -d --wait --wait-timeout 300

compose-down: ## 停止并移除依赖容器（保留数据卷）
	docker compose -f $(COMPOSE_FILE) down

clean: ## 清理构建产物
	rm -rf $(BIN_DIR)
