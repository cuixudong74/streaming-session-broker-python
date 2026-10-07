# streaming-session-broker

云游戏串流会话的接入与调度服务。串流节点各自只有有限的并发槽位，本服务在保证
**不超卖** 的前提下完成会话安置、排队、租约维持、断线重连、节点排空与重启恢复，
且调度结果 **可复现**。

## 运行与测试

```bash
uv sync
uv run pytest            # 全部自动化测试
uv run python main.py    # 启动 HTTP 服务
```

## 会话生命周期语义

```
                 request_session
                 │            │
            容量充足        容量不足
                 │            │
                 ▼            ▼
             ACTIVE ──────► QUEUED ──容量释放(严格FIFO)──► ACTIVE
               │  ▲
   renew_lease │  │ reconnect(宽限期内, 同一会话, 不重新安置)
               ▼  │
             ENDED（原因可区分，见 ReleaseReason）
```

- **安置（Placement）**：在 ACTIVE 且有空闲槽位的节点中，选择空闲槽位最多者，
  并列时按节点 id 字典序取最小。同一批输入 + 同一份容量状态 ⇒ 安置结果完全一致。
  容量刚好用完时不超卖；容量不足时进入等待队列（可配置为直接拒绝）。
- **排队（Queue）**：严格 FIFO（按单调递增 seq）。容量释放时只尝试队首，
  任何后来的申请都不可能插队，因此等待中的申请不会饿死。
  出队与安置在同一把锁内原子完成，同一申请不会被安置到两个节点。
- **租约（Lease）**：会话占用槽位期间持有租约（`lease_ttl`），客户端通过
  `renew_lease` 或 `reconnect` 续租。租约到期未续 ⇒ 释放，原因 `LEASE_EXPIRED`。
- **心跳（Heartbeat）**：节点需周期性心跳；超过 `heartbeat_timeout` 未心跳 ⇒
  节点置为 OFFLINE，其上会话全部释放，原因 `NODE_HEARTBEAT_LOST`。
- **释放原因（ReleaseReason，可区分）**：`CLIENT_RELEASE`（客户端主动释放）、
  `LEASE_EXPIRED`（租约到期）、`NODE_HEARTBEAT_LOST`（节点心跳丢失）、
  `NODE_OFFLINE`（节点被显式下线）、`NODE_DRAIN`（排空时无迁移目标而结束）。
- **断线重连（Reconnect）**：安置时签发 HMAC-SHA256 凭证，有效期 =
  租约到期 + `reconnect_grace`。宽限期内携带有效凭证重连 ⇒ 拿回 **同一个会话、
  同一个节点**，并续租，绝不重新安置。拒绝原因可区分：
  `TOKEN_INVALID`（伪造/畸形）、`TOKEN_EXPIRED`（超过宽限期）、
  `SESSION_ENDED`（会话已结束）、`SESSION_NOT_FOUND`（未知会话）。
  校验失败不修改任何状态，不影响已有会话。

## 节点维护与排空（Drain）

- `start_drain(node)` 后节点进入 DRAINING：不再接受新会话；等待队列中的在途
  申请 **从不绑定到具体节点**，因此排空不会丢失它们，它们仍会被安置到其他节点。
- 排空节点上的已有会话：正常释放（`CLIENT_RELEASE`）或租约到期时 **优先迁移**
  到有空闲槽位的其他节点（迁移目标的选择同样确定性）；无迁移目标时才以
  `NODE_DRAIN` 结束。
- **排空完成条件（明确）**：节点处于非 ACTIVE 状态 **且** 其上会话数为 0。
  由 `drain_complete(node)` / `GET /nodes/{id}/drain` 报告。在途排队申请不占用
  该节点槽位，因此不会阻塞排空完成。

## 并发与一致性

所有变更操作（申请、释放、心跳、过期扫描、排空）由同一把可重入锁串行化；
节点的已用容量不是独立计数器，而是从权威状态（`Node.sessions` /
`Session.node_id`）派生，因此不会出现“会话已结束仍占槽位”或“槽位重复计入”。
`check_invariants()`（`GET /admin/invariants`）随时可校验：不超卖、无双向
引用不一致、无重复计入、队列有序。

## 持久化与重启恢复

- 每次状态变更先写 WAL（`wal.log`，逐行 JSON + SHA256 校验和，fsync 落盘），
  再应用到内存；`snapshot()`（`POST /admin/snapshot`）将全量状态原子写入
  `snapshot.json`（临时文件 + rename）并截断 WAL。
- 重启恢复：先加载快照，再按序重放 WAL 记录（作为 **事实** 重放，不重新运行
  调度，因此不会重复安置）；已释放的会话以 `session_ended` 记录还原，不会被
  漏掉或复活。WAL 尾部若有崩溃留下的半途记录（JSON 截断或校验和不符），
  一律丢弃，不会被当成完整状态使用。
- **幂等重放（关键语义）**：快照的 rename 与 WAL 截断不是一次原子操作，
  服务在两者之间被强杀时，新快照已经生效而旧 WAL 尚未截断，重启后会同时看到
  快照和它所覆盖的那部分 WAL 记录（内容重复、彼此重叠的历史变更）。因此每条
  WAL 记录的重放都是 **幂等** 的：同一条记录无论被还原一次还是多次，最终内存
  状态完全一致。具体规则：
  - `request_enqueued`：队列中已存在相同 `request_id` 则跳过 —— 等待中的申请
    在队列里至多出现一次，恢复后只会被安置一次、安置到一个节点；
  - `session_admitted`：`session_id` 已存在则跳过 —— 不会复活已结束的会话，
    也不会重复占槽；
  - `session_migrated`：仅当会话仍挂在源节点上才搬移 —— 重叠的旧迁移记录不会
    把会话同时计到两个节点；
  - `heartbeat` / `lease_renewed`：取较大值，单调不回退；
  - `add_node` / `node_draining`：不重置已有节点状态（OFFLINE 不会回退）；
  - `session_ended` / `request_dequeued`：本身就是幂等操作。
- **覆盖的崩溃时序**：干净退出；WAL 尾部半途记录（截断/校验和错误）；快照
  临时文件写了一半（rename 未发生，旧快照+完整 WAL）；快照 rename 完成但 WAL
  未截断（重复/重叠记录，靠幂等重放兜底）；WAL 已截断（纯快照恢复）。
  无论崩溃发生在哪个时刻，恢复出的都是某个自洽的真实状态：不超卖、已结束会话
  不占槽、槽位不重复计入、队列保持 FIFO 顺序。
- 恢复后：未过期会话及其租约、节点容量占用、等待队列及其顺序全部还原；
  重启前签发的重连凭证仍然有效（同一 secret）；申请、释放、续租、重连、
  排空均不受影响。
- **验证方法**：`tests/test_recovery.py` 覆盖干净退出与尾部截断；
  `tests/test_crash_recovery.py` 覆盖写盘途中崩溃 —— 包括快照/WAL 重叠记录、
  同一记录连续多次重放、重复入队记录下的队列顺序保持，以及恢复后继续
  申请/释放/续租/重连/排空的完整链路，每个用例都以 `check_invariants()`
  和逐项计数断言收尾。

## 可配置项

| 环境变量 | 默认 | 含义 |
|---|---|---|
| `BROKER_LEASE_TTL` | 30 | 会话租约时长（秒） |
| `BROKER_HEARTBEAT_TIMEOUT` | 45 | 节点心跳超时（秒），超时节点置 OFFLINE |
| `BROKER_RECONNECT_GRACE` | 60 | 断线重连宽限期（秒，租约到期后额外） |
| `BROKER_ALLOW_QUEUE` | 1 | 容量不足时是否排队；0 则直接拒绝（`NO_CAPACITY`） |
| `BROKER_SECRET` | dev-secret | 重连凭证 HMAC 密钥 |
| `BROKER_STATE_DIR` | （空=不持久化） | WAL/快照目录 |

## HTTP API 摘要

| 方法/路径 | 说明 |
|---|---|
| `POST /nodes` | 注册节点 `{node_id, capacity}` |
| `POST /nodes/{id}/heartbeat` | 节点心跳 |
| `POST /nodes/{id}/drain` / `GET /nodes/{id}/drain` | 开始排空 / 查询排空是否完成 |
| `POST /nodes/{id}/offline` | 显式下线节点 |
| `POST /sessions` | 申请会话；返回 `placed`（含重连凭证）或 `queued` |
| `POST /sessions/{id}/release` | 释放会话 |
| `POST /sessions/{id}/renew` | 续租 |
| `POST /reconnect` | 携带凭证重连，拿回同一会话 |
| `POST /admin/sweep` | 触发过期/心跳扫描（驱动租约到期与迁移） |
| `POST /admin/snapshot` | 写快照并截断 WAL |
| `GET /admin/invariants` | 校验全部一致性不变量 |

## 代码结构

```
broker/models.py       数据类型与枚举（纯定义，无逻辑）
broker/credentials.py  重连凭证签发/校验（HMAC-SHA256）
broker/store.py        WAL + 快照持久化与恢复
broker/broker.py       调度核心：安置/排队/租约/心跳/排空/恢复/不变量
main.py                FastAPI 接入层
tests/                 容量争抢、节点失效、断线重连、排空、并发、重启恢复
                       （含写盘途中崩溃的幂等重放）
```
