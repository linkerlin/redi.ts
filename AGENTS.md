# AGENTS.md — redi.ts 协作约定

> 面向在此仓库工作的 AI 编码代理与人类贡献者。先读本文，再动代码。

## 项目定位

TypeScript 1:1 复刻 Redisson（面向 Redis 8.x）。**wire format（key 布局 / Lua
算法 / 值编码）必须与 Java Redisson 4.6.x + `JsonJacksonCodec` 互操作**，并与
`redi.py` / `redi.go` 互通。Java 源码（`C:/GitHub/redisson`）为最终标准；
`redi.go` 的 Lua（已对 Redisson 4.6.1 双向实测）为直接移植基线；`redi.py`
仅作 codec/hash 交叉验证（其裸 JSON 对象写法 Java 读会抛 `missing type id`，
本库写端跟 Go 做 `@class`/`ArrayList` 包装）。

## 命令

```sh
npm run build   # tsc
npm test        # 全量（live-* 需本机 Redis 127.0.0.1:6379 + mvn + interop/ 探针；集群项另需 wsl bash scripts/cluster-up.sh 起的 3 主节点 7100-7102；缺失时 live 用例带日志 skip，绝不静默过）
npx vitest run test/wire.test.ts test/redisson.test.ts test/interop.test.ts  # 无 Redis 可跑子集
bash scripts/redis88-up.sh   # 可选：WSL docker 起 redis:8(≥8.8, AR.*/CMS.*/TS.*) 于 6390；REDIS_URL=redis://127.0.0.1:6390 npm test 全量走新面
```

## 铁律（违反即回归）

1. **编码层（`src/codec.ts`）**：对象加 `@class: java.util.LinkedHashMap`、
   数组包 `["java.util.ArrayList",[...]]`、超 int32 整数包
   `["java.lang.Long",v]`；解码端递归剥离一切包装与 `@class`。改动必须过
   `test/wire.test.ts` + `test/interop.test.ts`（live python）。
2. **禁止手算二进制布局**：`struct.pack` 等价一律用 `Buffer.writeDoubleLE` /
   `writeBigUInt64LE` / `writeUInt32LE`（小端），经 `latin1` 透传，禁用 UTF-8。
3. **每次获取必须随机 id**：RateLimiter / PES 等去重结构每次
   `randomBytes(16)`（固定 id 会 ZADD 塌缩）。
4. **companion key 一律 hash-tag**：`prefixName` / `suffixName`。
5. **订阅不可批处理**：pub/sub 走 `duplicate()` 的真实连接；`RBatch` 仅
   pipeline，不承载订阅。
6. **ioredis 陷阱**：`eval` 的 Lua `nil` 回 `null`；`TIME` 取 server 时间；
   `RFairLock` 的 `ARGV[3]/[4]` 在 try/acquire 两脚本中顺序相反；fenced 返回
   `[ttl|−1, token]` 数组；mock 缺 `PFADD/GEOADD` 等命令时测试 skip 而非 fail。
   订阅（MapCache 事件等）只挂 `messageBuffer`（ioredis 总会发射它；另注册
   `message` 会双派发）；发布含 u64LE 长度前缀的负载必须传 `Buffer` 参数
   （字符串按 UTF-8 编码会破坏字节）。**ioredis-mock 的 `TIME` 把墙钟秒与
   `process.hrtime` 亚秒拼在一起（相位无关，±999ms 跳变），会静默打爆一切
   `serverNowMs` TTL 比较**——测试一律经 `mockClient()`（redisson.test.ts）
   修正为同一次 `Date` 调用；真 Redis 无此问题。RESP2 的模块批量回复（如
   `TS.RANGE`）值是扁平 `[ts, val]`，嵌套形态断言两种都要兼容。
7. **二进制成员必须走 Buffer**：struct-packed 值（MapCache `<dQ`、TimeSeries、
   DelayedQueue 成员、BinaryStream/BitSet 原始字节）在真实 Redis 上只能用
   `*Buffer` 命令 + Buffer 参数传输——ioredis 字符串按 UTF-8 编解码，会破坏
   随机 id/double 字节；mock 是进程内透传无此问题。反之 mock 的 `eval` 不接受
   Buffer 参数：mock 覆盖路径的 Lua 入参保持字符串（值部分须为合法 UTF-8 文本，
   长度前缀一律在 Lua 内用 `string.len` 自洽计算）。
8. **Adder 求和语义**：`sum()` 只返回 `GETDEL counter`（请求者自身也是订阅者，
   自 flush 已包含在内；`n==0` 无订阅者时才返回本地值）。counter/semaphore
   键须 `suffixName` hash-tag；订阅须先挂 `message` handler 再 await ack。
9. **SCAN MATCH 无花括号**：Redis glob 的 `{a,b}` 是交替语法——companion 键
   （`prefix:{name}` / `{name}:suffix`）清理/查询一律走精确 `DEL`/`GET`，
   不得塞进 `SCAN MATCH` 模式，否则会静默漏删导致幂等测试假失败
   （`trySetCapacity`/`tryInit` 二次调用合法返回 false）。
   Multimap 更须用 `clear()`：集合键 `{name}:<id>` 对裸名模式不可见，
   且内部 id 是确定性的——残留集合会被复用追加（Java 同行为），pattern 清理必漏。
   且 `clear()` 必须在 pattern 清理之前（它经 HASH 反查集合，HASH 先被删就反查为空）。
10. **TTL 余量按最坏抖动给**：全量套件多文件并行、JVM 逐个冷启，JVM→TS 交接
    抖动可达数秒。`visible-before` 断言（先见到再消失）的 TTL 取 ≥5s；
    `gone-after` 方向天然抗抖。1.5s 量级只在空载单测里成立。
11. **探针命令先对 Java 4.6.1 javap 再写**：同名 API 在 4.x/5.x 形状不同
    （`ReturnType.INTEGER`→`LONG` 无别名、`StreamCreateGroupArgs` 须 `makeStream`；
    新增命令的 `args[i]` 个数守卫逐个核对，`mcnative-put` 曾差一位吞掉 TTL）。
    exec 插件按空格切参——含空格参数换无空格写法（`return(40+2)`）；新命令先冒烟再进用例。
    Function 须 `WRITE` 模式（`FCALL_RO` 拒无 no-writes 标记函数）。
12. **TimeSeries label 是 codec 编码载荷、无 mark 前缀（4.6.1 形状）**：
    blob = 编码后 label 原样（`"jlbl"` 带引号），n=2/3 即 flag 本身；新版
    n=4+flag 前缀形状会破坏 4.6.1 读，反之被 DECODE_LABEL 显式兼容——
    写侧一律用 4.6.1 形状。读侧按 DECODE_LABEL 分支（n==4 看首字节 flag；
    n==2 无；其他取整段），再 codec 解码。4.6.1 真实 Lua 以 jar 常量池为准
    （`C:/GitHub/redisson` 是新版源码）。4.8 族 API（`add(TimeSeriesAddArgs)`
    等）写 flag-4 单调 id 成员（seq 为 HASH），旧 API 保持 flag-2/3（seq 为
    STRING）——**同一系列勿混用两族**。带 retention 的用例时间戳必须落在
    窗口内（新模块写时即裁剪，1970 纪元值会消失）。
13. **全局命名空间也要按文件隔离**：vitest 多文件并行，Redis Functions 按库名
    全局共享——phase6/phase7 同名 `p6lib/p6answer` 曾互相 REPLACE 覆盖致假失败；
    函数库/函数名加阶段后缀（`p6lib7/p6answer7`），跨文件共享键一律阶段前缀。

## 新增结构的流程

1. 先查 Java 源码（`C:/GitHub/redisson/.../RedissonXxx.java`）：key 名、channel
   名、Lua 原文、消息格式；再对 `C:/GitHub/redi.go/*.go` 的同名脚本逐行对齐。
2. 实现：继承 `RedisDataStructure`，走 `codec` 编解码，Lua 经 `luaEval`。
3. `Redisson` 加工厂方法（Redisson 风格名 + `R` 前缀别名）。
4. 测试：mock 功能 + wire 契约（`wire.test.ts`）+ 跨语言断言
   （`interop.test.ts` python 侧）。
5. 文档同步：README 特性表、COMPATIBILITY.md 矩阵行、CHANGELOG。

## 已知不做（勿重复提案，与 redi.go 一致）

- `RSortedSet`（LIST+Comparator）、`RTransaction`（快照回滚）、`LiveObject`、
  reactive/Rx、`RExecutorService`/`RRemoteService` 真协议、`RTransferQueue`
  阻塞握手（本库为队列迁移 `GO_ONLY`）、`RPriority*` Comparator 协议
  （本库 ZSET+score `GO_ONLY`）。
- PRO-only：`RReliableQueue`、`RLocalCachedMapCache`、`RReliablePubSubTopic`、
  `RBitVectorStore`、`RLeasedMap`、`RReliableFanout`（抛错）。
- `RClientSideCaching`：PARTIAL（ioredis 直通，无 Java EvictionPolicy）。
- `RLocalCachedMap`：数据层 WIRE_OK，失效为本地 JSON 广播；混部请
  `disableNearCache`。
