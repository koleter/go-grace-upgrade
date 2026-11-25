# go-grace-upgrade

Go 服务的 UDS 热升级模块，支持 Linux / macOS，使用 Go 1.25.11。部署新版本时，直接启动使用相同 UDS 路径的新实例，框架完成资源交接，业务无需监听升级信号。

- 自定义 TCP/TLS：迁移监听端口、已有连接、未消费半包、待发送数据及 TLS 1.2/1.3 状态，客户端无需重连。
- 标准 HTTP：新实例接管监听端口，框架自动排空旧实例的请求，客户端重新建连。
- 业务默认只提供完整消息处理函数；消息序号和 `Session.Values` 由框架自动保存、恢复。

## 接入自定义 TCP/TLS

```go
decoder, err := graceupgrade.DelimiterDecoder("#") // 入站：按 # 拆包
if err != nil { return err }
encoder, err := graceupgrade.DelimiterEncoder("\n") // 出站：追加换行
if err != nil { return err }

manager, err := graceupgrade.New(graceupgrade.Options{
    UpgradeSocket: "/tmp/my-service.sock", // 每一代保持相同路径，父目录须已存在
    ReadyTimeout: 15 * time.Second,
})
if err != nil { return err }
defer manager.Close()

server, err := manager.NewServer("service", "tcp", ":9090", graceupgrade.ServerOptions{
    Decoder: decoder,
    Encoder: encoder, // 省略则原样发送 OnMessage 返回的字节
    OnMessage: func(ctx context.Context, session *graceupgrade.Session, message []byte) ([]byte, error) {
        return []byte(fmt.Sprintf("reply-%d: %s", session.Sequence, message)), nil
    },
    // TLSConfig: &mtls.Config{...},
})
if err != nil { return err }
go server.Serve(ctx)
if err := manager.Ready(ctx); err != nil { return err }

<-manager.Done()
// 所有托管连接已经交接，清理应用资源并返回 main。
```

消息处理借鉴 [Netty Pipeline 的入站/出站分离](https://netty.io/4.1/api/io/netty/channel/ChannelPipeline.html)，流程为 `网络输入 → Decoder → OnMessage → Encoder → 网络输出`。业务分别配置收包格式、处理函数和回包格式，框架统一管理缓冲和 socket I/O。

`Decoder` 和 `OnMessage` 必填，`Encoder` 可选，默认 `RawEncoder()`：直接发送业务返回的字节，不追加任何内容。例如接收 `hello#world#` 时，业务依次收到 `hello` 和 `world`；使用 `DelimiterEncoder("\n")` 时响应追加换行，省略 Encoder 时直接返回原始响应。业务也可以在 OnMessage 中组装完整回包，包括换行，然后使用默认原样发送。原样发送时，接收端需按自身协议识别响应边界。

`DelimiterDecoder(delimiter)` 按原始字节精确匹配并去掉包尾分隔符，例如 `"#"`、`"\r\n"`、`"<END>"` 或中文字符串。支持粘包和分隔符本身被拆开的半包；请求分隔符不限制响应内容，按 `#` 收包时响应可以包含 `#`。

`DelimiterEncoder(delimiter)` 只负责出站的分隔符封装。它不执行转义：响应正文包含出站分隔符，或正文尾部与追加的分隔符形成更早的匹配时，编码返回错误，避免客户端误拆包。非 nil 空响应编码为单独的分隔符。两种分隔符构造函数都返回 `(接口, error)`，分隔符不能为空；完整包（含分隔符）不能超过 `MaxBuffer`，最多 256 KiB。

自定义格式只需实现所需方向的接口，分别赋给 `ServerOptions.Decoder` 和 `ServerOptions.Encoder`：

```go
type Decoder interface {
    Name() string
    Decode(input []byte) (consumed int, message []byte, err error)
}

type Encoder interface {
    Name() string
    Encode(message []byte) ([]byte, error)
}
```

`Name()` 标识各自方向的协议格式、配置及版本，长度为 1–256 字节，在服务运行期间保持稳定。热升级同时校验 Decoder 和 Encoder，任一侧改变时拒绝恢复并回退旧实例。`Decode` 每次只返回一个完整消息及其占用的字节数，半包返回 `0, nil, nil`，不提前消费输入或保存连接私有解析状态。`Encode` 只处理响应。实例会被不同连接并发调用，方法必须并发安全，不能保存输入切片或连接私有状态。

长度前缀格式分别使用 `LengthPrefixDecoder(width)` 和 `LengthPrefixEncoder(width)`，支持 1/2/4/8 字节大端无符号长度，长度不包含前缀本身。各方向的构造函数独立使用，也可以搭配不同格式。按行收发使用 `DelimiterDecoder("\n")` 配合 `DelimiterEncoder("\n")`。双向协议标识不变时，会话快照保持兼容；默认 Encoder 和显式 `RawEncoder()` 也可相互恢复。

`OnMessage` 收到完整消息，返回响应正文，框架交给 Encoder 后发送。返回 nil 表示不响应且不调用 Encoder，非 nil 空切片仍会调用 Encoder；使用 RawEncoder 时空响应不产生网络字节。同一连接的回调串行执行；不同连接可以并行。

`Session.Sequence` 自动递增。需要保留认证用户等小量会话数据时，写入 `Session.Values`，例如 `session.Values["user"] = []byte(userID)`。不要修改 Sequence，不要在回调外访问 Session，也不要保存输入切片；需要保留时先复制。

每连接需要迁移的业务状态应保存到 `Session.Values`，由框架统一快照和恢复；Decoder/Encoder 实例本身不会被迁移。

原 `Factory` 接入需要将协议解码迁移到 Decoder、响应编码迁移到 Encoder、业务处理迁移到 OnMessage、会话状态迁移到 Session.Values。旧自定义 Handler 快照不能直接恢复为 Session，升级时会拒绝并回退旧实例。

业务需要配置 socket 选项时，可通过 `ServerOptions.Listen` 提供原始 `*net.TCPListener` 创建函数。只在冷启动时调用，升级实例会自动认领继承的 listener。TLS 通过本模块的 `TLSConfig` 配置。

## 接入标准 HTTP

业务提供 `http.Server`，框架负责 Serve、热升级后的 Shutdown 和超时 Close，无需监听升级事件或信号：

```go
manager, err := graceupgrade.New(graceupgrade.Options{
    UpgradeSocket: "/tmp/my-http-service.sock",
})
if err != nil { return err }
defer manager.Close()

server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second}
if err := manager.ServeHTTP("http", "tcp", ":8080", server, graceupgrade.HTTPOptions{
    DrainTimeout: 30 * time.Second,
}); err != nil { return err }
if err := manager.Ready(context.Background()); err != nil { return err }
return manager.Wait()
```

`ServeHTTP` 在每一代的 `Ready` 前注册并启动服务；传入尚未启动的 `http.Server`，之后不要自行调用其 Serve、Shutdown 或 Close。框架只在交接提交后开始排空，升级失败时旧 HTTP 服务继续运行。`DrainTimeout` 默认 30 秒；超时会关闭剩余 HTTP 连接，并由 `Wait` 返回排空错误。

`Done()` 仍表示交接完成或 Manager 已关闭，HTTP 请求此时可能仍在执行；`Wait()` 会继续等待所有托管 HTTP 服务排空，并返回服务异常或排空错误。多个 HTTP 服务并行排空。宿主主动停止时调用 `manager.Close()`，然后等待 `manager.Wait()`；库不安装操作系统信号处理器，也不调用 os.Exit。HTTP 示例无需信号代码即可升级，直接 Ctrl+C/SIGTERM 停止示例使用操作系统默认行为。WebSocket 等 Hijack 连接仍需要业务单独管理。

## 升级流程

1. `New` 连接配置的 UDS。没有旧端点时取得所有权锁并冷启动；有旧端点时必须完成协议握手。
2. 新实例接收监听资源，初始化业务并调用 `Ready`。提交前不接收业务流量、不读写继承连接。
3. 旧实例暂停 Accept 和连接读写，交接网络缓冲、会话及 TLS 状态；新实例整批恢复成功后才提交。
4. 成功后旧实例关闭自己的迁移 FD 副本并关闭 `Done`。托管 HTTP 自动排空旧请求，`Wait` 返回后业务清理退出。
5. 新实例初始化或恢复失败时中止启动，旧实例恢复服务，允许重试。

UDS 监听 FD 和所有权锁 FD 一起交接，旧实例退出不会删除新实例的端点。只有确认没有活跃持有者时才清理残留 socket；普通文件、权限错误、活跃端点的握手失败不会被当作冷启动。`<socket>.lock` 文件保留以稳定锁的 inode，并发接管返回 `ErrBusy`。

升级等待正在执行的回调完成；超时会回退。框架不迁移 Go 调用栈、后台任务或任意业务对象，也不会重放已经执行的回调或数据库写入。所有权提交后新实例崩溃，不会自动复活旧实例。

## 示例与验证

每个示例只演示一种接入方式，均通过直接启动新实例升级：

| 示例 | 内容 | 验证命令 |
| --- | --- | --- |
| [examples/tcp](examples/tcp/main.go) | TCP 请求按 `#` 分割、响应追加换行、单次建连客户端 | `go run scripts/smoke_connections.go` |
| [examples/tls](examples/tls/README.md) | TLS 1.2/1.3、证书生成与校验 | `go run scripts/smoke_connections.go -example ./examples/tls` |
| [examples/http](examples/http/main.go) | 标准 net/http、旧请求排空 | `python3 scripts/smoke_http.py` |

```bash
go test -race -timeout=90s ./...
go vet ./...
```

测试覆盖 UDS 并发启动、残留端点、失败回退与重试，以及 TCP/TLS 半包、待发送明文/密文、TLS 记录碎片、exporter、多连接恢复失败。连接 smoke 会构建 v1/v2/v3，在旧进程真正退出后继续使用原连接；HTTP smoke 检查旧请求排空和持续新请求。

手动运行 TCP，在三个终端分别启动 v1、客户端和 v2：

```bash
# 终端一
go build -ldflags '-X main.version=v1' -o bin/tcp.v1 ./examples/tcp
./bin/tcp.v1 -addr 127.0.0.1:9090 -upgrade-socket /tmp/tcp-upgrade.sock

# 终端二：只建立一次连接
go run ./examples/tcp -mode client -addr 127.0.0.1:9090

# 终端三：直接启动新版本
go build -ldflags '-X main.version=v2' -o bin/tcp.v2 ./examples/tcp
./bin/tcp.v2 -addr 127.0.0.1:9090 -upgrade-socket /tmp/tcp-upgrade.sock
```

v1 自动退出，客户端输出切换到新 PID/version，计数连续增长。各代由部署工具独立监管；停止当前实例使用其所在终端的 Ctrl+C。无需 PID 文件或升级信号。

## 代码结构与边界

| 文件 | 职责 |
| --- | --- |
| `manager.go` | New、监听注册、Ready、Done、Close 和生命周期 |
| `http.go` | HTTP 服务托管、自动排空与 Wait |
| `uds.go` | 发现旧实例、端点所有权、交接协调 |
| `server.go` | Accept、每连接读写循环、TLS 和缓冲 |
| `connections.go` | 暂停、连接快照、整批恢复与回退 |
| `message.go` | 入站 Decoder、出站 Encoder、业务回调与自动会话状态 |
| `protocol.go` / `fd_*.go` | 控制消息、监听清单、FD 传递及平台实现 |
| `mtls/` | 独立 TLS 实现及 Snapshot/Restore |

内部升级协调按“暂停服务 → 等待当前操作结束 → 同步快照 → 交接 → 提交或恢复”顺序执行。`sync.Mutex` 和 `sync.Cond` 统一保护暂停状态、工作计数和生命周期，连接之间不再通过 channel 请求、回复快照。网络 I/O 和业务回调仍按连接并发执行，执行期间不持有这些状态锁；`Done()` 保留对外的完成通知。

- 一代内只创建一个 Manager；各代使用相同服务名称、network、请求地址和兼容的 Decoder/Encoder/状态格式。控制协议仅支持 v3。
- 托管 socket 仅支持 TCP。UDP 不纳入托管，其启停由业务安排。
- 裸 `Manager.Listen / ListenWith` 只交接监听端口，已有连接由业务排空。不能仅包装任意 net.Conn 就捕获上层 bufio、标准 crypto/tls、HTTP/2 或 WebSocket 的私有状态。
- 明文/密文缓冲上限默认且最多 256 KiB；会话序列化状态及进程级 Snapshot 各最多 1 MiB。最多 128 个业务 listener、65536 条托管连接。
- TLS 迁移使用本模块 mtls，不能外包一层标准 tls.Listener。支持 TLS 1.2 AEAD/CBC 与 TLS 1.3，不迁移 TLS 1.0/1.1 或 RC4。
- mtls 来自 MOSN 的 Go 1.12.7 TLS 分支，不会自动获得当前标准库的安全修复，需要独立维护与审查。

本模块借鉴 MOSN 的 FD、网络缓冲和 TLS 迁移机制，采用整批交接，没有实现旧进程残留响应转发，详见[设计说明](docs/mosn-migration.md)。根目录 MIT 许可与 TLS 文件原有许可分别适用，见 [mtls/NOTICE](mtls/NOTICE)。
