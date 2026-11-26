# TLS 热升级 example

[English](README.md) | 简体中文

独立的 TLS 服务端与客户端示例，支持 TLS 1.2 / TLS 1.3。服务端使用本模块的 `mtls` 和 `NewServer` 迁移活跃连接；客户端使用 Go 标准 `crypto/tls`，校验证书和主机名，只连接一次，持续发送请求。

响应展示服务端 PID、二进制版本和连接会话计数。升级后 PID/版本变化，计数连续增长，原 TLS 连接继续工作。

服务端采用 `DelimiterDecoder("\n") → OnMessage → DelimiterEncoder("\n")`，入站和出站独立配置，业务只实现一个完整消息处理函数。拆包、消息计数、会话和 TLS 快照及恢复由框架完成；省略 Encoder 则原样返回业务字节。客户端应匹配各方向的格式，其他自定义格式可分别实现 Decoder/Encoder 接口。

以下命令均在项目根目录执行。

## 一键验证

```bash
go run scripts/smoke_connections.go -example ./examples/tls
```

脚本生成临时证书、编译 v1/v2/v3，分别验证 TLS 1.2 和 TLS 1.3 的连续升级、旧进程退出、原连接和半包保留、会话计数连续、新连接可用，以及示例客户端的证书/主机名校验。结束后清理临时目录和测试进程。

## 1. 编译并生成测试证书

```bash
mkdir -p bin/tls
go build -ldflags '-X main.version=v1' -o bin/tls/server ./examples/tls
./bin/tls/server -mode cert
```

生成 `bin/tls/server.crt` 和 `bin/tls/server.key`，证书包含 `localhost`、`127.0.0.1`、`::1`，有效期一年。私钥权限为 `0600`；已有证书/私钥不会被覆盖。默认输出在已被 Git 忽略的 `bin/` 目录。

## 2. 启动服务端

终端一：

```bash
./bin/tls/server -addr 127.0.0.1:9443 -upgrade-socket /tmp/tls-upgrade.sock
```

默认使用 TLS 1.3 和上一步的证书。自定义证书路径可传入 `-cert`、`-key`。`-upgrade-socket` 在所有代中保持一致，默认 `/tmp/graceupgrade-tls.sock`。

## 3. 保持 TLS 客户端运行

终端二：

```bash
./bin/tls/server -mode client -addr 127.0.0.1:9443
```

客户端默认信任 `bin/tls/server.crt` 并校验主机名 `localhost`，无需关闭证书校验。日志展示连接两端地址、TLS 版本、密码套件和 ALPN；每 500ms 输出一次响应：

```json
{"pid":12345,"version":"v1","count":1,"payload":"message-1"}
{"pid":12345,"version":"v1","count":2,"payload":"message-2"}
```

`-ca` 可以指定其他 CA/受信证书文件，`-server-name` 指定验证的服务主机名。`-count 10` 发送 10 次后退出，默认持续运行；`-interval 100ms` 调整间隔。连接断开或会话计数不连续时客户端报错退出，不会重连。

## 4. 直接启动新版本热升级

终端三：

```bash
go build -ldflags '-X main.version=v2' -o bin/tls/server.v2 ./examples/tls
./bin/tls/server.v2 -addr 127.0.0.1:9443 -upgrade-socket /tmp/tls-upgrade.sock
```

新版本启动时通过 UDS 发现旧版本，自动恢复监听端口、活跃连接、半包和 TLS 状态。终端二持续输出，变为新的 PID 和 `version=v2`，`count` 接续上一代。终端一显示 `TLS connections moved ... remaining=0` 后退出，新服务端在终端三继续使用原 TLS 连接。

继续构建 v3 并使用同一 UDS 启动，可以测试连续升级，无需发送任何升级信号。发布时保留相同证书文件，客户端继续验证相同服务身份。新版本初始化或恢复失败会退出，旧版本恢复服务。

## TLS 1.2 与停止服务

重新启动服务端和客户端，双方指定 TLS 1.2：

```bash
./bin/tls/server -tls-version 1.2
./bin/tls/server -mode client -tls-version 1.2
```

在当前服务端所在终端使用 Ctrl+C 停止服务；客户端也可用 Ctrl+C 退出。示例不需要 PID 文件。
