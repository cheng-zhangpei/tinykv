## go1.关于rawNode为啥要写ready

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

---

## 对于RaftStore这一角色在全局的了解

​		raftStore感觉是统领所有raft组的一个管家，raft模块是通过ready与advance向上层通知，ready:我的状态改变了哦，快点把我持久化一下。advance: 上面的模块告诉raft，你的状态持久化好了，可以修改状态（软硬）和指针了。

​		raftStore的整体架构设计是Workers异步化，这是为了避免raftStore的主线程堵塞，可以将耗时操作异步化。具体的架构如下：

```
[ TinyKV Server ]
      |
      | (gRPC 请求: Put/Get/Snap)
      v
[ RaftStore (Manager) ] <---- 下面不同worker负责不同部分
      |
      +--- 路由 (Router): 根据 RegionID 找到对应的 Peer
      |
      +--- 驱动 (Ticker): 给所有 Peer 发 Tick 信号
      |
      v
[ Peer (Worker) ]  <---- 每一个 Region 的代言人
      |
      +--- [ Raft Module ] (你写的 2A 代码)
      |         ^
      |         | (Input: Step)
      |         | (Output: Ready)
      |         v
      +--- [ PeerStorage ] (你写的 2B 代码)
      |         |
      |         v
      +--- [ BadgerDB ] (磁盘)
```



**RaftStore 的核心设计：Batch System (批处理)**

- 它用**少量线程**（通常是 CPU 核数）来驱动**大量 Region**。
- 它有一个 Loop，不断轮询：“Region 1 有消息吗？Region 2 有消息吗？”
- 如果有，就拿出来执行 `Step()`，收集 `Ready`。
- 然后把这一批 Ready 攒起来，**一次性**刷到磁盘（Write Batch），**一次性**发到网络。
- **这叫 IO 合并**，是 TiKV 高性能的秘密。
- 它负责心跳的驱动，由其统一触发心跳信号给所有的raft节点

-----

这里再将这个架构掰开来看：

```
RaftStore--->raft_worker--->peer_mesg_handler--->peer--->raftGroup->raft->状态变化
													 ---> peerStore->db->状态持久化
				
```

​		RaftStore下面的raft_worker负责详细的传递，本质其实就是不断读raftch这个通道并将数据分发到不同的region中，每一个region其实只要有一个rawnode接收信息就好了，这个peer其实是在raft组的基础上封装了一层的其他的组件，比如ticker就是在peer这层，还有peerStorage，一个peerStorage负责一个raft组的持久化操作。

- 我后面看了一下TIKV的帖子，发现的确蕴含了很多的异步和同步的trade-off
- 我看了一下真正的网络层是在peer_msg_handler里面，这个网络层的具体内容后续有时间还是要深挖一下的。
- 我们在对ready的时候有一个比较有意思的细节就是我们会创建两个WriteBatch，在两个不同的引擎来存状态和事务，这样的设计的确是会存在不一致的问题但是，好像是为了权衡性能：

> Raft Log (WAL) 和 KV Data (LSM-Tree) 的写入模式完全不同：
>
> - **Raft Log**: Append-only，顺序写，极快。
> - **KV Data**: 随机写，可能有 Compaction，慢。
>   如果不把它们分开（存到不同的 Badger 实例或 Column Family），KV 的慢写入会阻塞 Raft 的快写入，导致整个共识协议卡顿。



---

Snapshot 包含两部分：

1. **Metadata**: `Index`, `Term`, `ConfState` (集群成员)。
2. **Data**: 真实的 KV 数据 (Key-Value Pairs)。

​		我们快照生成的逻辑其实是这样的，peer中的peerStorage中有一个Snapshot方法，这个方法就是用来检测快照生成的咋样的，因为快照生成的过程一定是需要与peer这个结构解耦的，不然就会卡死raft的运行。具体的工作是用region_worker来做的，`RegionWorker` 收到任务后，会在后台线程扫描 BadgerDB，生成 Snapshot，然后把结果塞回 `ch` (Notifier)，然后Snapshot再去这个ch中去取快照。

----

- 我们快照以及持久化其实单位都是peer（region）

peerStorage是针对一个region的持久化操作。其实这样想也很正常，对于一个raft group不就是一个由共识算法联系起来的一个状态统一体吗？

综上可以详细复盘一下整个peer中snapshot的调用链路：

### A. 生成快照链路 (Create Snapshot)

**场景**：Leader 或 Follower 发现自己的 Log 太长了（超过了 `RaftLogGcCountLimit`），需要 Compact。

这前面还有一个场景：外部Store中的全局ticker发送数据到raftWorker的raftch通道，这个通道会接受下面这些信息：

1. **Ticker (时钟)**: 会扔 `Msg{Type: MsgTypeTick}`。
2. **Network (网络)**: 会扔 `Msg{Type: MsgTypeRaftMessage}` (收到别的节点消息)。
3. **Client (客户端)**: 会扔 `Msg{Type: MsgTypeRaftCmd}` (Propose)。

​		后面链接的peer_msg_handler本质就是一个消息接受处理的中转站，每一个peer都有一个handler，他用于接受raftWorker的tick用于驱动快照的形成，peer_msg_handler是会对raftch中的数据进行过滤只留下属于自己regionId的内容。所以这里其实是一个1对多的关系，只用若干线程，层层解耦，将计算下放。

-----------------------



1. **PeerMsgHandler (Tick)**: `onRaftGCLogTick` 定时检查日志长度。这个检查长度的代码是在peer_msg_handler中的
2. **触发**: 发现太长 -> 发送 `CompactLogRequest` 给自己 (AdminRequest)。
3. **Propose**: `proposeRaftCommand` -> Raft Log。（这个链路就是进入rawNode了，进入raft组了）

--> **这中间就是raft的的过程， 接收/应用快照。 它不参与快照的生成**

Raft 动作:

- 这个 Request 只是一个普通的 Log Entry。Raft 把它 Append、Broadcast、Commit。这个时候修改了entries，就会生成ready往上汇报。
- **注意**: 此时 Raft **没有调用 `handleSnapshot`**。**Apply**: `HandleRaftReady` 拿到了 `CompactLogRequest`。

1. **Apply**: `HandleRaftReady` -> `processAdminRequest` -> `CompactLog`。

2. PeerStorage: 调用ScheduleCompactLog

   -> 删除旧日志。

   - **关键点**: 这时并没有**生成**完整的 Snapshot 文件，只是截断了日志，修改了 `TruncatedState`。

3. 真正的生成 (当有人要的时候，这个我们也成为lazy generation):

   - **场景**: Leader 想发日志给 Follower，但发现日志被截断了 (`ErrCompacted`)。
   - **动作**: Leader 调用 `r.sendSnapshot` -> `r.RaftLog.storage.Snapshot()`。
   - **PeerStorage**: `Snapshot()` 被调用 -> 发送 `RegionTaskGen` 给 **RegionWorker**。
   - **RegionWorker**: 扫描 BadgerDB -> 生成 `pb.Snapshot` -> 返回给 Leader。
   - **Leader**: 把这个热乎的 Snapshot 封装进 `MsgSnapshot` 发给 Follower。

### B. 应用快照链路 (Apply Snapshot) - 你刚才复盘的那条

**场景**：Follower 落后太多，收到了 Leader 发来的 `MsgSnapshot`。

1. **Leader**: 发送 `MsgSnapshot` (网络消息)。
2. Follower (Raft):Step->handleSnapshot
   - **动作**: 清空 Log，设置 `pendingSnapshot = snapshot`。
3. **Follower (RawNode)**: `Ready()` 发现有 `pendingSnapshot`，打包吐出。
4. **Follower (PeerMsgHandler)**: `HandleRaftReady` 收到 Ready。
5. Follower (PeerStorage): 调用SaveReadyState----->ApplySnapshot
   - **Meta**: 修改 `RaftState`, `TruncatedState` (持久化)。
   - **Data**: 发送 `RegionTaskApply` 给 **RegionWorker** (异步写盘)。
6. **Advance**: 通知 Raft 清除 `pendingSnapshot`。

----

在写raft层的snapshot中需要注意的点：

- sendSnapshot

   其实就是从PeerStorage里面调用Snapshot这个方法去通知RegionWorker干活

​	拿到之后将快照丢给别的follower，但是，这里要注意修改leader去follower的感官，就是Progress中的Next指针要改一下，不然后面同步太麻烦了。

- handleSnapshot

​		其实先做校验，看看这个快照的index不会还不如我现在的日志新吧？如果更新就把现在raftLog中的指针全部都修改了。在更新指针之前还要becomeFollower一下，也就是重置一些状态，不然Vote之类的参数可能还是绑定的，会在后面出现一些奇奇怪怪的问题。核心就是要更新就更新彻底。

----

对于计时器，计时器是一个全局触发的tick，全局ticker的心脏是raftStore中tickerDriver

1. **TickDriver (全局)**:

   - 这是一个独立的 Goroutine。
   - 它维护了一个大循环，或者维护了一个时间轮（Time Wheel）。

   > 甚至这个位置还可以看到时间轮，这个难度真的很恐怖了

   - 它每隔 100ms（`RaftBaseTickInterval`），就会醒一次。

2. **分发 Tick**:

   - TickDriver 醒来后，会遍历所有的 Region（或者通过某些机制知道哪些 Region 该 Tick 了）。
   - 它向对应的 `raftWorker` 发送一个 `Msg{Type: MsgTypeTick, RegionID: ...}`。
   - **关键点**：这个 Tick 消息也是通过 **`raftCh`** 扔进去的！和其他网络消息、客户端请求一样，排队等待处理。

3. **raftWorker (处理)**:

   - 从 `raftCh` 收到 `MsgTypeTick`。
   - 找到对应的 `peerMsgHandler`。
   - 调用 `d.onTick()`。

4. **peerMsgHandler (响应)**:

   - `d.onTick()` 调用 `d.RaftGroup.Tick()`。
   - `Raft.tick()` -> `elapsed++` -> `MsgHup/MsgBeat`。

-----

对于整个流程深埋的网络线：

#### 第一站：Raft 算法层 (raft.go)

- **动作**: `r.sendAppend(to=B)`
- **产物**: `pb.Message{Type: MsgAppend, To: B, Entries: ...}`
- **位置**: 这个消息现在还在 `r.msgs` 这个内存切片里。

#### 第二站：Ready 打包 (rawnode.go)

- **动作**: `RawNode.Ready()`
- **产物**: `Ready{Messages: [MsgAppend]}`
- **位置**: 这个消息被打包在这个 Ready 结构体里，交给上层。

#### 第三站：RaftStore 主循环 (peer_msg_handler.go)

- **动作**: `d.Send(d.ctx.trans, rd.Messages)`
- **关键点**: 这里是 Raft 世界和外界的**分界线**！
- **代码**: `d.ctx.trans.Send(msg)`。
- **Trans 是啥？** 它是 `Transport` 接口。在 TinyKV 里，它通常是一个 `RaftClient`。

#### 第四站：Transport 层 (transport.go / server.go)

- **动作**: `RaftClient.Send(msg)`
- 转换: 这里发生了一次关键的封装！
  - Raft 的 `pb.Message` 被塞进了一个更大的信封：**`RaftMessage`** (定义在 `raft_serverpb.proto`)。
  - `RaftMessage` 包含了：`RegionID`, `FromPeer`, `ToPeer`, 以及最核心的 `Message` (刚才那个 Raft Msg)。
- **发送**: 调用 gRPC 的 `Snapshot` 或 `Raft` 接口，真正通过 TCP 发给 Peer B 的 IP:Port。

#### 第五站：Peer B 的 gRPC Server (server.go)

- **动作**: `Raft(stream)`
- **接收**: Server 收到 `RaftMessage`。
- **分发**: 调用 `router.Send(regionID, msg)`。

#### 第六站：Peer B 的 Router & Worker (raft_worker.go)

- **动作**: Router 根据 `RegionID` 找到对应的 Worker。
- **入队**: 把 `RaftMessage` 扔进 `raftCh`。
- **Worker 线程**: 从 `raftCh` 取出消息。

#### 第七站：Peer B 的 Handler (peer_msg_handler.go)

- **动作**: `HandleMsg(msg)` -> `HandleRaftMessage`。
- **解包**: 把 `RaftMessage` 里的 `pb.Message` 拿出来。
- **投喂**: `d.RaftGroup.Step(msg)`。

#### 终点站：Peer B 的 Raft (raft.go)

- **动作**: `Step(msg)` -> `handleAppendEntries`。
- **闭环**: Peer B 的 Raft 收到了这条日志！

-----

其实只有**两套**核心通讯机制交织在一起：

1. **gRPC 网络通讯 (Node to Node)**:
   - 负责在**不同机器**之间搬运数据。
   - 载体：`RaftMessage`。
   - 管道：`Server` -> `Transport` -> `Server`。
2. **Go Channel 内部通讯 (Thread to Thread)**:
   - 负责在**同一个进程内**的不同线程（Goroutine）之间搬运数据。
   - 载体：`Msg` (包含 RaftMessage, Tick, Cmd)。
   - 管道：`raftCh`, `regionSched`, `applySched`。

----

​		**1 个 Node = 1 个 TinyKV 进程 = 1 个 RaftStore = 多个 Peer (Regions)**。这个对应关系要搞清楚，其实这样看这个封装的架构真的无比复杂.........



-----

​		在peer_msg_handler中的proposeRaftCommand方法，这个方法的作用是下方我们Proposal的，也就是调用rawNode的propose，但是这个位置在不断的网络分区中，Proposal是会失败的，也就是刚刚propose下去就导致分区，propose失败，所以这个时候简单的propose就不行了，否则会导致propose驻留在队列中对后面的类型干扰。



```go
	if err := d.RaftGroup.Propose(data); err != nil {
		if cb != nil {
			// 移除刚刚添加的最后一个 proposal
			d.proposals = d.proposals[:len(d.proposals)-1]
			cb.Done(ErrResp(err))
		}
		return
	}
```

​		所以这个位置要做一个截断，将最后的一个元素给截断掉。这个函数proposeRaftCommand也是由tick去驱动的，而真正append的操作是raft往上ready的内容。

----



做存储开发，当你要更新两个非原子（不在同一个 Batch/DB）的状态时，永远要问自己：

> **“如果写完第一个，还没写第二个就挂了，系统重启后能不能活？”**

- **方案 A（旧）**：记了有数据，没记起点变了 -> **数据空洞（Panic）**。
- **方案 B（新）**：记了起点变了，没记有数据 -> **数据丢失（Empty Log）**。

-----





所以可以看出这句话，系统的设计本质上就是一门艺术，看了TinyKV这个项目才会有深刻的感悟。
