## 1.关于rawNode为啥要写ready

**etcd/TinyKV 的设计哲学是：**
		Raft 只是个大脑（纯内存逻辑），它没有手（不能写磁盘），也没有嘴（不能发消息）。我之前写raft的时候其实就是纯粹的大脑，没有任何的行动力。

​		当大脑思考完（比如决定要发心跳，或者决定要追加日志）后，它**不能直接干**，而是把这些决定**打包**成一个包裹，叫 **`Ready`**

- **Raft (大脑)**: “我思考完了，这是 `Ready` 包裹，里面有 3 条要存盘的日志，和 2 条要发给 Follower 的消息。”
- **上层应用 (手和嘴)**: “收到！我负责把日志写进 BadgerDB，把消息通过 gRPC 发出去。”
- **Advance**: 手和嘴干完活了，告诉大脑：“搞定！你可以进行下一轮思考了。”

​		所以综合的来说就是一个通讯机制，rawNode通过ready往上通知具体操作，Advance告诉raft已经搞定啦。

​		Raft 节点的状态分为两类，一类丢了也没事（易失性），一类丢了就完了（持久性）。

- 包含:，Lead` (谁是大哥), `RaftState` (我是啥角色: Follower/Leader)。
- 特点，不需要存磁盘如果节点崩溃重启，它变成 Follower 就行了，不需要记得上次谁是 Leader，等收到新 Leader 的心跳自然就知道了。
- 在 Ready 里的作用，主要用于**UI展示**或者**触发上层回调**。比如上层代码发现 `SoftState.Lead` 变了，可能需要打印一条日志：“Leader 变更为节点 2 了！”。

#### `HardState` (硬状态 / 持久性)

- **包含**: `Term` (任期), `Vote` (投给谁了), `Commit` (提交进度)。
- 必须存磁盘（WAL）且必须在发网络消息，之前存盘（Safety 要求）。
  - **Term & Vote**: 防止重启后重复投票（如果忘了投过票，可能会导致一个 Term 选出两个 Leader）。
  - **Commit**: 防止重启后已提交的数据回滚。
- 在 Ready 里的作用:上层收到 `Ready` 后，如果发现 `HardState` 不为空，**必须**把它写入 BadgerDB。

说白了HardState是在进行具体操作之前必须要**同步WAL存盘**，因为安全性咯

----

## 2. RawNode 为什么要存 `prevSoftState` 和 `prevHardState`？

因为 `RawNode` 的 `Ready()` 方法可能会被频繁调用，但我们**不想每次都全量输出**。

- **场景**: `tick()` 触发了，但没有任何状态改变。
- **如果没有 prev**: `Ready()` 每次都要构造一个新的 SoftState/HardState 返回出去，上层拿到一看，跟上次一样，白忙活一场（写磁盘是要开销的！）。
- 有了 prev
  - `RawNode` 会比较：`Current.Term == prevHardState.Term`？
  - 如果一样，那 `Ready.HardState` 就留空。
  - 上层看到空的 HardState，就知道不用写磁盘了，性能提升巨大。

---

## 日志的全部结构

​		Raft 的日志并不是简单的“一层层往下漏”，而是一个**时间窗口**的概念。我们可以把整个日志历史想象成一条无限长的传送带，但是我们只能保留其中的一小段。

#### 层级 1: `Unstable` (内存 - 缓冲区)

- **位置**: `RaftLog.entries` 中 `Index > stabled` 的部分。
- **物理**: 纯内存切片。
- 来源
  - **Leader**: 刚从客户端收到的 `Propose` 请求。
  - **Follower**: 刚从 Leader 那里 `AppendEntries` 同步过来的。
- **命运**: 它们非常危险，断电就丢。所以 `Ready()` 会第一时间把它们送去持久化。

#### 层级 2: `Stable` (内存 + 磁盘镜像)

- **位置**: `RaftLog.entries` 中 `Index <= stabled` 的部分。

- **物理**: 也在 `RaftLog.entries` 内存里！但同时已经在 `Storage`（BadgerDB）里有一份拷贝了。

- 意义: 既然磁盘有了，为啥内存还要留着？

  为了读得快！

  - 如果 Follower 落后了，找 Leader 要 Log Index=100 的日志，Leader 直接从内存 `entries` 里拿，不用去查磁盘，性能极高。

- **命运**: 随着时间推移，这部分日志越积越多，内存撑不住了，就要进行 **Truncate (截断)**，也就是 **Compact**。这个Compact的本质就是一个merge操作，将一大堆数据统合成一个关键的快照

#### 层级 3: `Storage` (磁盘 - 归档区)

- **位置**: 在 BadgerDB 里。
- **物理**: 硬盘上的 SST 文件。
- 意义: 它是Stable日志的全量备份
  - 当 `Stable` 日志从内存中被 Compact 掉之后，如果还需要查旧日志（比如有个 Follower 掉线了一万年，现在才回来），就只能去 Storage 里查了（这很慢）。
  - 或者，如果旧日志实在太老了，Storage 里也会把它删掉，只保留一个 **Snapshot**。

#### 层级 4: `Snapshot` (快照 - 压缩包)

- **本质**: **“时间被压缩了”**。
- **意义**: Index=1 到 Index=10000 的所有日志，执行完的结果就是 `x=5, y=10`。那存这 10000 条日志太浪费了，直接存 `Index=10000, State={x=5, y=10}` 就行了。
- 关系
  - 当 `Stable` 日志太多时 -> 触发 Compact -> 生成 Snapshot -> 删除旧日志。
  - `PendingSnapshot`: 这是刚收到（或者刚生成）但还没存稳的快照。

----

## pendingSnapshot的意义

简单说：**PendingSnapshot 是个“外来户”或者“刚出炉的热乎货”，它短暂地存在于内存中，等待被安家落户（存盘）。**

我们可以分两种场景来看 `pendingSnapshot` 的来源：

### 场景 1：Leader 发给我的快照 (2C 主要场景)

- **背景**: 我是一个落后很久的 Follower，我的 NextIndex 是 5，但 Leader 的 Log 已经从 1000 开始了（1-999 都 Compact 掉了）。
- 过程
  1. Leader 发给我一个 `MsgSnapshot`。
  2. Raft 收到后，调用 `handleSnapshot`。
  3. Raft 发现：“卧槽，这快照太新了，我的旧日志全废了。”
  4. Raft 把这个快照赋值给 `l.pendingSnapshot`。
  5. **此时**: 快照数据在内存里（在 `pendingSnapshot` 变量里），还没进 BadgerDB。
  6. **Ready**: Raft 把 `pendingSnapshot` 打包进 `Ready`。
  7. **RaftStore**: 上层应用拿到 Ready，把 Snapshot **写入磁盘**，同时清空旧数据。
  8. **Advance**: 调用 `stableSnapTo`。
  9. **结果**: `l.pendingSnapshot = nil`（因为已经存盘了），`stabled` 推进到快照的 Index（因为之前的日志都被快照替代了，逻辑上算是“存稳”了）。

### 场景 2：我自己生成的快照 (Project 2C 也会涉及)

- **背景**: 我是 Leader（或者 Follower），我的 Log 太长了，我要 Compact。
- 过程:
  1. 上层应用（RaftStore）调用 `storage.Snapshot()`。这通常是异步的，可能涉及到扫描 BadgerDB 里很多 Key 来生成状态镜像。
  2. 这部分 TinyKV 里是由 `PeerStorage` 处理的，Raft 核心逻辑**不用管**快照是怎么生成的。
  3. Raft 只需要知道：“哦，Storage 里现在有一个 Index=1000 的快照了。”
  4. 然后 Raft 调用 `Compact(1000)`，把内存里 Index<=1000 的日志删掉。

综上这个pendingSnapshot本质上只是一个在内存中的中继罢了，一个触发场景是在Storage中的数据非常多的时候跳到



------

- 在处理信息时候，任期的匹配性问题永远是我们需要第一时间考虑的
- 无论是follower还是leader，这里含有一个很重要的性质就是raft日志一定要有严格的递增性
- 在处理日志数据的时候我们一定需要考虑一下，当前的缓冲区的窗口大小是否可以满足要求，是否会因为Entries过大被compact从而找不到日志？所以我们往往需要考虑从之前的快照部分和storage部分来进行查找也就是将快照发给follower让follower找就是了
- 只要涉及到数据切片，我们都要考虑一下，当前信息是否已经committed，raft中不能修改已经committed的数据。
- 在我们new一个raft结构的时候我们往往是需要考虑到节点并不是第一次打开，我们要先进行的操作是将之前磁盘中持久化之后的状态拿出来初始化到这个新的节点中
- 我们在对内存中的日志缓冲区进行截断的时候是需要考虑到stable这个指针，新append的数据一定是会影响stable指针的位置，因为新的数据一定一开始是unstable的。
- **Heartbeat immediately after election**

​		在leader被选举出来的瞬间需要广播一次appendMsg，这里的作用其实是让所有的follower在收到appendRequest的时候稍微看一眼自己的日志，哦发现这个msg好像term更高诶？然后就变成follower并且执行append操作去将新leader的日志进行同步一下。

- 这里再说一下appendEntries和对应的response处理的细节，这里大量的细节需要注意，是需要经验和记忆的，自己去碰这样的坑基本是很难很难的。

handleAppendEntries:

这个函数是follower/candidate触发的，大体的流程如下：

1） 先查一下这个msg的term是不是落后自己的？如果是落后，就reject，和leader说一下（这个地方没有冲突检测）

2）再看看，leader原来progress里面记录的follower的最新的index和任期应该是咋样的，和现在follower最新状态对比一下，如果不一样，好家伙leader不对，这个时候就要follower和leader对齐一下咯，就是冲突检测，因为TinyKV没有用rejectHint加速就是一个个回退就好了呗

3）这个时候就可以正式将这部分的entries放到自己的unstable部分的存储了。这个时候又有折断的问题，就是发来的起点可能不是刚刚好顺延自己lastIndex往后的。

4）最后更新当前的committed指针，这个指针是取leader的committed指针和follower的committed指针的较小值。

handleAppendResponse

1) 对于leader，先看看follower是否接受了这个消息，如果拒绝了就要开始回退了，也就是把Progress回退，然后把现在回退之后leader所期待follower的状态发给follower让follower看看现在ok了不？
2) 如果ok了就更新自己的Progress，也就是自身状态，然后尝试修改自身的committed指针
3) 这个committed指针就是这个集群绝大多数节点都认同的日志提交位置，所以这个时候我要遍历所有的progress，然后将所有的follower的Match的位置拿出来，我要找到所有的follower都match的位置作为我现在的commit指针

---

- 对于follower的committed指针更新位置

​		我们会在follower处理AppendLogEntries的时候更新一次committed，但是我们还需要在心跳部分持续的推进更新committed，如果只是在append时候更新，那么follower状态推进的就会很慢，持久化的也会很慢。

- 在我们将Progress中的数据要发送给别人的缓冲区的时候有一个坑，我们progress中记录的全局的日志索引，比如101、102这种，是包含被compact和storage的数据，但是对于别人来说是要放到unstable的数组里面这里就有一个映射，所以要转化一下把自己unstable部分的数据拿出来给别人
- 我们在初始化一个节点的时候，在初始化Progress的时候需要注意我们要从lastIndex开始更新，

```go
r.Prs[p] = &Progress{Next: lastIndex + 1, Match: 0}
```

因为可能会持久化部分的数据要通过下面这个函数拿出来

```go
lastIndex := r.RaftLog.LastIndex()
```

- 一个很恐怖的问题，我们在测试的时候发现Match的推进问题，会导致Commit推不上去。

​		我们更新任何的一个Match的时候其实都是在handlePropose里面进行的，但是有一个特例，一个leader在becomeLeader的时候是会执行一个No-ops的append操作，这个操作我当时没有更新Match，我是直接去bastAppend()的了，让所有的节点知道现在发生的leader的变更（因为心跳不会立刻开始）

​		如果没有更新的后果：Leader 自己都不知道自己拥有这条 No-Op 日志（Match 没跟上），导致 Quorum 计算少了一票，Commit 推不动。

​	-> 所以说明，分布式系统分层非常重要，什么是原生接口，原生接口的功能是啥，如果调用路口太多有时候就会有地方没有更新到位。

