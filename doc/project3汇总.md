# Lab3汇总

这里汇总了Lab3的ABC三个实验的进展与对架构的思考，与Lab2不同，Lab2是我之前写NucleusDB的时候接触过的内容，重点是学习一下corner case以及log处理的细节，所以整体是比较顺利的，lab3是我一直想要学，但是一直不太敢真正动手实操的部分，系统可以将整个的Region分裂，调度等工业特性给搞清楚。

## Lab A

### Leader Transfer

Leader Transfer其实基本都是raft部分的状态变化，我们在proposeRaftCmd的时候单独处理LeaderTransfer命令，这是一  个独立的消息类型。由rawNode转发给raft组。leader（若follower收到转发给leader）收到TransferLeadMsg时需要进行下面两个步骤：

1) Leader接收Propose之后对比leader与transferee之间的日志是否Index相等，也就是日志是否匹配，如果匹配就发送一个Timeout信息让transferee直接开始选举，这个时候Leader不能接收信息。匹配的过程其实就是发送AppendEntrie进行日志对齐
2) Leader在收到AppendResponse的时候，如果有遇到匹配的话，还要做一次sendTimeout，这个教研是一个坑，日志对齐之后还需要再次触发选举
3) Transferee再次选举就是发一个Hub再次选举就好了

### ConfChange

> todo：TinyKV为了教学简化只是实现了单个Node的加减，但是etcd中为了保证配置变化的灵活性往往是使用JointConsensus这个机制来保证配置过度的平滑。后续是否可以将TinyKV的配置变换用jointConsensus机制给替代？

首先，ConfChange论文中的统一做法是使用JointConsensus进行的，可以同时进行多个节点的变化，TinyKV为了简化操作，配置变化只能同时删减一个节点。Raft论文中证明了，ConfChange如果每次只修改一个节点是不会出现脑裂的问题的。

我们先从上层的Placement Driver开始复盘一下TiKV整个的ConfChange的全过程：

假设集群中有3个物理服务器：Node1、Node2、Node3。Node对应的是一个物理机，也是一个RaftStorage实例，一个RaftStorage本质上就是一个很大的存储KV（由大量的raft实例共享），专门负责状态和日志的应用与持久化。RaftStorage下面的worker会连接多个Region，Region中对应多个的peer实例。peer往往分布在不同的物理机上，而peer之间的消息交互通过Node的Transport层用于消息的传达。

每一个Store也就是每一个机器都会有一个统一的tickerDrive统一给该节点中的peer发送时钟信号，在Node的本身还有一个heartBeat线程，用来接收调度器的。下面是单次confChange的架构：

1）首先PD会监控物理服务器负载，比如Node1中有一个Region的负载过高，将触发Sacle up,这个时候我会挑选一个负载比较低的Node（或者自己指定）创建一个副本就是replica，PD 给 **Node 1 ** 对应的region发送一个 Operator（指令）：`AddPeer(Store=4)`

2） 收到PD指令之后Node自己的心跳会触发一个AdminCmdType_ChangePeer，往后就是ProposeRaftCmd，这里对ConfChange进行特殊处理，其实就是A.构造一个特殊的Entries，新的config会被以json的形式封装到data里面，往下放到raft中。

3）entries会被Append到raft中，作为一个日志存在。在raft中需要用pendingConfChangeIndex记录这个日志的index值。在raft收到日志的时候需要做阶段，我需要判断上一个confChange的entries是否已经apply了（一次只能有一个confChange），如果没有apply就对日志进行截断因为在这个日志之后的数据都是新config之后的操作，但是这个confChange已经abort了，所以要截断

4）现在正式通过ready往上通知，在processCommittedEntries中处理这个特定的entries，这里的处理一共分为两部分：A：调用ApplyConfChange往下raft层通知，这个本质上就是通过rawNode直接调用addNode和removeNode，这两个函数本质上就是操作Prs罢了，然后在删除的时候**需要更新committed指针**。B）需要持久化对应的conf和修改peer与raftStore中的对于这个region的定义（region的Peers数组）。

-----

截止到上面这些内容，其实新的peer其实还没有出现，那这个peer是如何被优雅的加入集群中？

1）这个时候这个节点只是在Ptr里面注册了，leader在Append的过程中发现这个新节点和他差距似乎非常大诶？所以就直接发送自己的快照Snapshot。

2）这个Msg最终转到raftStore的部分，raftStore查了一下Store发现似乎没有这个peer诶？这个时候会触发**MaybeCreatePeer**在本地创建一个peer实例（分配内存，初始化 Raft 状态机、Snapshot），这部分是由RaftStore中的StoreWorker完成的

3）peer此时才真正创建，应用完日志之后就开始与leader进行日志的同步

> 所以我们前面做的更多的是逻辑意义上的准入，物理意义上的准入是由快照驱动的，这种设计方式真的非常优雅，但是这个系统难度的确是非常大的

------

## LabB 

分片的触发逻辑：在TinyKV中，tickDriver所触发的时钟发送给peer_msg_handler的时候会由时钟触发两个函数：一个函数是往d.onSplitRegionCheckTick(),这个函数是检查是否达到分裂的阈值的，如果 需要分裂就发送一个请求给regionCheckWorker,让worker用迭代器扫描一遍startKey到endKey之间的key，如果超过阈值的话，就去这个阈值处作为SplitKey返回给Leader，这个时候返回的handMsg的msgType是MsgTypeSplitRegion，这里将数据丢给onPrepareSplitRegion这个函数，这个函数发向PD的，大概的意思是要PD为这个分裂的新的节点生成一下编号包括store的编号和peer的编号。原来达到阈值的region就保持不变就好了。返回的时候将相关的信息比如NewRgionId之类的填写到信息里面去。 

拿到这个信息之后就开始下放这个resp，也就是去做propose-> raft -> ready -> handleReadyStatus -> procressiCommittedEntries -> proposeAdminRequest，从这个函数开始开始真正的分裂集群。

-----------

>ok, 稍微停一下，这里复盘一下这些持久化的元数据
>
>**RaftDB (`raftWB`)**：
>
>1. **RaftLog** (Key: `z{regionId}_{index}`): 日志条目。这是共识的基础。
>2. **RaftLocalState** (Key: `r{regionId}s`): 包含 `HardState` (Term, Vote, Commit) + `LastIndex`。这是 Raft 重启后能“接上断点”的关键。
>
>**KvDB**
>
>1. **用户数据** (Key: `z{key}`): 也就是 Client `Put` 进来的数据。
>2. **RegionLocalState** (Key: `r{regionId}l`): 包含了 `Region` 的完整定义（StartKey, EndKey, Epoch, Peers）。
>
>上面这两个db（本质是一个db但是不同的wb）都是存在一个Store物理机器上的。调度器是有自己独立的存储单元比如etcd的。调度器会存j几个东西：
>
>	1. 所有store的状态与路由
>	1. 所有peer的状态与路由
>
>**GlobalContext** 是由所有的peer共享的上下文，Peer是可以修改这个context
>
>	1. 所有region的map
>	1. 所有region的key范围（这里是用一个B树来维护的）

我在做Region分裂的时候产生了一个疑问，我们新region在分裂的时候都是位于同一个的Store中的，新的region不应该触发调度吗？案例来说在创建之处为啥不将新的peer放到其他Node中？

答案：因为 Split 的本质是 **“逻辑切分”**，而不是 **“数据搬运”**。region的数据是位于同一个store上的，所以两个region数据在一开始是相邻的。如果要同步将数据迁移的话，会占用大量网络IO。

------------

## 3A与3B过程中遇到的corner case

#### Leader Transfer：The Missing TimeoutNow

- 现象：Leader 收到 Transfer 请求，发现与follower的日志没有对齐，发了 Append 给 Follower 追数据，然后就没有然后了。Transfer 超时。

- **原因**：handleTransferLeader` 只执行一次。如果 Follower 当时没追平，Leader 发了 Append 就结束了。当 Follower 追平并回复 `AppendResponse` 时，Leader 忘了检查“是不是正在 Transfer”，没发 `TimeoutNow

#### ConfChange ：The Unapplied Pending

- **现象**：`reject conf change because pending > applied`。
- **原因**：Leader 提议了 ConfChange，但 Apply 线程卡住或挂了，导致 `PendingConfIndex` 无法清零。后续请求一直被拒。
- **解法**：确保 `ApplyConfChange`（清零 Pending）和 `destroyPeer`（自杀）的执行顺序正确。**更重要的是，要在 Propose 阶段就拦截掉非法的并发 ConfChange。** 

​	

#### B-Tree 的索引损坏

- **现象**：`GetCF` 能读到数据，但 `Iterator.Seek` 读不到。
- **原因**：BadgerDB 底层对 `Default` CF 里的 Key 自动加了 `default_` 前缀。`GetCF` 会自动处理，但 `NewIterator` 是 Raw 的，需要手动加前缀才能 Seek 到正确位置。
- **解法**：在 `Entries` 方法里手动拼接 `seekKey = "default_" + startKey`。

#### “诈尸” (The Zombie Peer)

- **现象**：Peer 自杀后，重启时又复活了，而且状态错乱 (`LastIndex < AppliedIndex`)。
- **原因**：`destroyPeer` 删除了 `ApplyState`，但 `HandleRaftReady` 循环还在继续，后面的逻辑又把 `ApplyState` 写回去了。
- **解法**：在 `HandleRaftReady` 循环里，每次 Apply 后检查 `d.stopped`。如果已死，立即 `return`，切断后续写入。

####  持久化顺序 (Crash Consistency

- **现象**：重启后 Panic `Unavailable`（Log 空洞）。
- **原因**：先写了 RaftDB (LastIndex 更新)，后写 KvDB (Snapshot/TruncatedIndex 更新)。中间断电，导致 LastIndex 很大，但 Log 其实被截断了。
- **解法**：**先 KV 后 Raft**。宁可丢数据（Log 没写进），不可存脏指针（Log 没写进但 Index 变了）。

