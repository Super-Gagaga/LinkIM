# LinkIM K8s 部署清单

前提：集群内已可用的 MySQL / Redis / Kafka（服务名 mysql/redis/kafka，
或修改 configmap.yaml 中地址），镜像 linkim/{account,logic,comet,job} 已推送到仓库。

```bash
kubectl apply -f namespace.yaml
kubectl apply -f configmap.yaml
kubectl apply -f account.yaml -f logic.yaml -f job.yaml -f comet.yaml
```

说明：
- comet advertise_addr 需为 Pod IP：部署时用环境变量覆盖
  `LINKIM_SERVER_ADVERTISE_ADDR=$(POD_IP):9000`（downward API 注入），此处占位。
- logic/job 挂 HPA（CPU 70%）；job 扩容上限受 Kafka 分区数约束。
- comet preStop sleep 10 + terminationGracePeriodSeconds 40 配合进程内 drain（12.2）。
