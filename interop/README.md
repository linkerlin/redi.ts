# interop — redi.ts ↔ Java Redisson 互操作探针

Maven（非 Spring Boot，零业务依赖）：`Redisson 4.6.1` + `JsonJacksonCodec`，
与 `redi.py/redi.go` 的探针同版本，保证三方结论可比。Spring Boot 非必需——
互操作验证的是 Redis 线序而非框架集成；如需 Boot 集成可在此 pom 上加
`spring-boot-starter-data-redis`（线序不变）。

## 布局

```
interop/
├── pom.xml                     Redisson 4.6.1 + Jackson + exec 插件
└── src/main/java/io/github/redits/interop/
    ├── TsProbe.java            CLI: clean|write-java|read-ts|roundtrip [ns]
    └── TsFixtures.java         两侧 fixtures 契约
```

命名空间：`TsJavaT:`；Java 写 `TsJavaT:j:`，TS 写 `TsJavaT:t:`。

## TS 写入契约（Java `read-ts` 期望）

| key（`TsJavaT:t:` 后缀） | TS 写法 | Java 断言 |
|---|---|---|
| `bucket:str/int/long/double/bool/list` | `"hello from ts"`, `7`, `5000000000`, `1.41`, `false`, `[4,5,6]` | 相等 |
| `atomiclong` / `atomicdouble` | `set(200)` / `set(6.28)` | 相等 |
| `map` | `put(name,redi.ts)`, `put(n,1)`, `put(flag,true)`（size 3） | `get/size` |
| `set` | `add(x,99,y)`（size 3） | `contains/size` |
| `list` | `add(a,b,c)` | `readAll` |
| `queue` / `bqueue` | 首元素 `qa` / `tbq1` | `peek` |
| `deque` | `[d_first, d_last]` | `readAll` |
| `lex` | `add(tx1,tx2)`（size 2） | `contains/size` |
| `zset` | `add(10,a)` | `getScore` |
| `bloom` | `tryInit(1000,0.03)` + `add(tsbloom1)` | `contains` |
| `mapcache` | `put(tk,tv)` | `get` |
| `setcache` | `add(tsc1)` | `contains` |
| `latch` | `trySetCount(4)` | `getCount` |
| `sem` | `trySetPermits(8)` | `availablePermits` |
| `hll` | `add(7,8,9)`（count 3） | `count` |
| `geo` | Palermo 坐标 + `TPalermo` | `contains` |
| `stream` | `add({tfield:tvalue})` | 非空 range |

## 探针命令（阶段 2-4）

锁/同步：`lock-try <name>`、`lock-hold <name> <ms>`、`rw-{read,write}-{try,hold} <name>`、
`fenced-token <name>`、`sem-try <name>`、`pes-available <name>`、
`rate-set <name> <rate> <intervalMs>`、`rate-try <name> <permits>`、
`dq-offer <dest> <v> <delayMs>`、`dq-poll <dest> <timeoutMs>`（内置 DelayedQueue 包装）、
`rt-publish` / `rt-listen <topic> <timeoutMs> <resultKey>`、
`ts-add` / `ts-range`、`longadder-cohort <name> <delta> <signalKey>`。

其余：`topic-publish` / `topic-listen <topic> <timeoutMs> <resultKey>`、
`stream-group-create` / `stream-group-read <s> <g> <c> <resultKey>`（建组须 `makeStream`）、
`multimap-put`、`batch-map-put`、`bq-capacity`/`bq-offer`、
`idgen-init`/`idgen-next`、`mc-put-ttl`、`ring-setcap`/`ring-add`。
阶段七：`bstream-set/get`、`rt-fanout`、`mc-get/size`、`mm-get`、`zset-score`、
`map-get`、`atomic-get`、`stream-add/size`、`bbq-take`、`dq-offer2`。
阶段八：`sc-contains`、`bucket-get`、`deque/list/set-read`、
`atomicdouble-add/get`、`lmm-get`。

## 运行

```powershell
# 1. Java 写，TS 读（TS 侧见 test/live-interop.test.ts）
mvn -f interop/pom.xml -q compile exec:java "-Dexec.args=write-java TsJavaT:"

# 2. TS 写，Java 读
mvn -f interop/pom.xml -q compile exec:java "-Dexec.args=read-ts TsJavaT:"

# 3. 延迟队列消费（注意：必须先创建 DelayedQueue 包装类才会调度迁移任务，
#    直接 poll 目标队列不会迁移——dq-poll 已内置该步骤）
mvn -f interop/pom.xml -q compile exec:java "-Dexec.args=dq-poll <destQueue> 12000"

# 4. 长加法器跨语言求和（adder 是本地缓冲 + 广播 flush，单次性 JVM 会丢失
#    本地值；longadder-cohort 常驻等待 TS 侧求和完成后再求和，结果同时打印
#    并写入 <signal>:result，READY 写入 <signal>:ready）
mvn -f interop/pom.xml -q compile exec:java "-Dexec.args=longadder-cohort <name> 100 <signalKey>"

# 5. 可靠主题监听（verdict 同时打印并写入 <resultKey>，防偶发 JVM 早退丢输出）
mvn -f interop/pom.xml -q compile exec:java "-Dexec.args=rt-listen <topic> 15000 <resultKey>"

# 5. 清理
mvn -f interop/pom.xml -q compile exec:java "-Dexec.args=clean TsJavaT:"
```

## 测试键清理

所有 live 用例键都在 `TsJavaT:` 命名空间下（各阶段再加后缀）。用例开始时自清，
跑完会有残留，一键清扫：

```powershell
redis-cli --scan --pattern 'TsJavaT:*' | ForEach-Object { redis-cli del $_ } | Out-Null
redis-cli --scan --pattern '{TsJavaT:*' | ForEach-Object { redis-cli del $_ } | Out-Null
```

第二行专清花括号 companion 键（`SCAN MATCH` 对裸名模式不可见，见 AGENTS.md 第 9 条）。
