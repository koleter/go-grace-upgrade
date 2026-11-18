# MOSN 来源与实现边界

参考源码为 `arch_wcloud_mosn`，TLS 来源 revision 记录在 [mtls/NOTICE](../mtls/NOTICE)。MOSN 的公开说明见 [平滑升级原理解析](https://mosn.io/docs/products/structure/smooth-upgrade/)。

## 复用的思路与代码

- 网络 socket 通过 Unix socket 的 SCM_RIGHTS 传递 FD。
- 迁移内容包含未消费输入、待发送输出以及 TLS 记录状态；仅迁移监听端口不足以保留已有连接。
- `mtls` 改编自 MOSN 基于 Go 1.12.7 的 TLS 分支，增加独立的 TLS 1.2/1.3 快照和恢复。具体改动、许可及维护限制见 NOTICE 和 README。

## 当前实现与 MOSN 的差异

| 能力 | 参考 MOSN 实现 | 本模块 |
| --- | --- | --- |
| 升级协调 | 通过监听/连接迁移 domain socket 协调，可分进程启动 | 新实例通过配置的 UDS 主动发现旧实例并接管，只有这一种升级入口 |
| 连接交接 | 支持迁移的协议在读循环中逐连接交接 | 暂停托管连接，整批恢复后提交；恢复失败整批回退 |
| 未完成请求 | 可把旧进程后续收到的上游响应按新连接 ID 转发给新进程 | 等待当前业务回调完成，再交接待发送响应；不实现旧进程残留响应转发 |
| 半包 | 网络层共享 IoBuffer，codec 完整消费后 Drain，未消费字节迁移 | Codec 仅返回完整消息，未消费字节始终留在框架缓冲中 |
| 业务会话 | 不自动迁移任意业务对象 | 框架自动迁移 Session，业务状态保存到 Session.Values |
| HTTP/1.1 | 本地参考版本禁用连接迁移，使用 Connection: close 排空 | ServeHTTP 自动调用标准 net/http.Server.Shutdown 排空，不自实现 HTTP 解析器 |
| HTTP/2 | 本地参考版本禁用连接迁移，走 GOAWAY/GracefulShutdown | 不支持 HTTP/2 原连接迁移 |

本模块没有宣称对所有协议提供无条件的原连接迁移。Go 调用栈、任意外部缓冲、业务后台任务、标准 crypto/tls 的私有状态不会因为包装 Listener/Conn 自动变成可迁移状态。

## 低成本接入自定义 TCP/TLS

使用 `ServerOptions{Codec: ..., OnMessage: ...}`，两项均为必填。业务提供完整消息处理函数；常见 LF/长度前缀格式可以直接使用内置 Codec。自定义格式实现 Codec 接口的 Name、Decode、Encode 方法，只描述拆包和编码，不持有每连接解析状态。Name 标识协议版本及配置，升级时用于兼容性校验。

框架自动保存消息序号和 Session.Values。回调在每连接中串行执行，升级在回调边界暂停；回调仍在执行时不会从另一进程重放。超时取消升级，不会强制把业务执行到一半的状态交给新进程。任意业务对象需要主动映射到 Session.Values，Codec 实例本身不会被迁移。

监听 socket 可以由 ServerOptions.Listen 或 Manager.ListenWith 的创建函数提供，首次启动才调用，升级时按名称认领继承资源。TLS 必须由本模块的 TLSConfig 管理。

验证覆盖独立启动的新旧进程中的 TCP、TLS 1.2/1.3、框架缓冲内半包、连续消息、自动会话恢复、协议不兼容回退及重试。完整 smoke 脚本还验证替换二进制后的连续升级，并在旧进程真正退出后继续使用原连接。

## UDS 自动发现与生命周期

配置 `Options.UpgradeSocket` 后，New 优先连接旧端点。端点不存在或拒绝连接时，必须取得进程生命周期内持有的所有权锁，才能清理残留路径并冷启动。如果锁仍被持有，则重试连接，超时失败也不删除路径。活跃端点必须通过协议握手，失败不会降级。普通文件和符号链接不会被当作残留 socket 删除，权限错误不会被忽略。

新实例通过同一连接接收业务监听 FD、升级 UDS 的监听 FD 和所有权锁 FD，完成业务初始化后调用 Ready。旧实例在收到 Ready 后暂停连接并传递快照，新实例整批恢复成功才提交。UDS 和 flock 锁沿用同一组内核资源；旧实例只关闭自己的副本，不 unlink 新实例的端点。失败的新实例尚不拥有 UDS 路径，Close 不能误删旧实例的端点，也不会释放旧实例仍持有的锁。

旧实例交接完成后关闭 Manager.Done。通过 ServeHTTP 注册的标准 HTTP 服务由框架自动调用 Shutdown，多个服务并行排空，超时调用 Close；Manager.Wait 等待托管 HTTP 排空结束后返回。Done 保留交接完成的语义，不代表 HTTP 请求已完成。交接提交前失败时不排空旧服务。裸 Listen 接入仍由业务自行排空。库不安装操作系统信号处理器、不调用 os.Exit。进程级 Snapshot 和失败 OnUpgradeError 回调均为可选。

正常退出只删除本代拥有且 inode 仍匹配的 socket 文件，然后释放所有权锁；锁文件保留，以便所有启动过程锁住同一个 inode。并发热升级返回 ErrBusy，不会让两个新实例同时消费同一批连接。

协议仅支持 v3，业务监听 FD、UDS 监听 FD 和所有权锁 FD 一起交接。UDS 测试使用独立启动的三代进程，不依赖升级信号、父进程 exec 或环境变量继承。

