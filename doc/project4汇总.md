# Project4 事务部分汇总

### 前置内容——关于TinyKV的锁机制

#### 单机视角

以每一个Store的角度主要提供对KV中的Key加锁的能力，整个事务都是由Store的锁机制锁保证的。

TinyKV 有一个 **`Latches`** 结构（在 `kv/transaction/latches/latches.go`）。
它的作用类似于行锁（Row Lock），但是是内存级别的。

- **目的**：防止两个并发请求同时修改同一个 Key 造成 BadgerDB层面的竞争，或者逻辑上的混乱。
- **实现**：它其实就是一个巨大的 Hash Map 或者分片 Map（为了减少锁粒度）。
- 流程：
  1. Request 来了，解析出所有涉及的 Key。
  2. `Latches.Acquire(keys)`: 尝试锁住这些 Key。如果有人在用，就排队（WaitGroup）。
  3. 执行事务逻辑 (Prewrite/Commit)。
  4. `Latches.Release(keys)`。

为了实现 MVCC 和事务，BadgerDB（Store） 里的数据被分到了三个不同的抽屉（CF）里：

1. **Default CF (数据区)**
   - **Key**: `UserKey + StartTS`
   - **Value**: `UserValue`
   - **作用**：只存数据，不存状态。因为数据可能很大，所以单独放。
2. **Lock CF (锁区)**
   - **Key**: `UserKey`
   - **Value**: `Lock Info` (Primary Key, StartTS, TTL, Type...)
   - **作用**：表示这个 Key 正在被某个事务修改中（Prewrite 阶段）。如果读的时候遇到 Lock，通常要等待或回滚。主键的指针就在这个区中
3. **Write CF (提交记录区)**
   - **Key**: `UserKey + CommitTS`
   - **Value**: `Write Info` (StartTS, Type: Put/Delete/Rollback)
   - **作用**：这是事务提交后的“结案报告”。读数据时，先查这里，找到 CommitTS 对应的 StartTS，再去 Default CF 拿真正的数据。

对于一个key的事务操作：先去Write CF中看看在这个key的StartTs后面有没有提交记录，如果有就冲突。看完再去Lock CF看看有没有人正在更新事务，如果有，冲突。最后才是去Default中去修改数据并且更新Lock CF将锁占有。

#### 跨集群事务

- 传统2PC事务

大体思路可以见之前写的NucleusDB

- Percolator

  ```
  Client (Txn)
     |
     +--> 1. Prewrite Primary (A) -> [Raft Log] -> [BadgerDB Lock CF (Primary)]
     |
     +--> 2. Prewrite Secondary (B) -> [Raft Log] -> [BadgerDB Lock CF (指向 A)]
     |
     | (如果都成功)-> 失败则回滚
     v
     +--> 3. Commit Primary (A)  -> [Raft Log] -> [BadgerDB Write CF] (锁 A 消失)
     |    (此时事务逻辑上已 Commit)
     |
     +--> 4. Commit Secondary (B)-> [Raft Log] -> [BadgerDB Write CF] (锁 B 消失)
     
  ```

- 假设我在 Store 2 上看到了 `Key="balance_B"` 被锁住了。
- 我想知道这个锁到底是有用的还是残留的垃圾？
- 我读取锁里的 `Primary` 字段，发现是 "balance_A"。
- 我去问 PD：“"balance_A" 在哪个 Store？”
- PD 说：“在 Store 1”。
- 我去 Store 1 查 "balance_A" 的状态。

所以这个事务是有一个协调者参与的，这个就是PD，路由是key的范围咯，说白了就是找region，还是好理解的



#### 跨集群事务回滚

在 Percolator (TinyKV) 模型里，**回滚**分为两种情况：

1. **主动回滚 (Prewrite 失败)**：
   - Client 发送 Prewrite，如果某个 Key 冲突了或者网络超时了。
   - Client 决定放弃这次事务。
   - **动作**：Client 给所有涉及的 Key 发送 `Rollback` 请求。
   - Server 做什么：
     1. 删掉 Lock CF 里的锁。
     2. **关键点**：往 Write CF 里写一条 `Type: Rollback` 的记录！(Key=UserKey+StartTS)。
     3. **为什么**？为了防止网络延迟的 Prewrite 请求后来又到了，Server 一看这里有个 Rollback 标记，就知道这个事务已经废了，直接拒绝。这就叫 **“防重放”**。
2. **被动回滚 (Crash Recovery)**：
   - Client 挂了，留下一堆锁。
   - 别的事务读到这些锁，等到 TTL 超时。
   - 别的事务帮它执行 Rollback（也就是上面的动作）。

**重新提交 (Retry)**：
这是 Client 的责任。TinyKV Server 只管拒绝，Client 收到 Error 后，重新去 TSO 拿一个新的 StartTS，重头再来。

------------------

## Lab A

首先我们要知道Store中的Txn定义，Txn中有一个Reader专门用于读取CF中的数据，一个封装的Write数组，这个数组封装了Delete和Put这些写操作，因为我们的MVCC机制是检测写写冲突。

这个实验里面实现了一大堆的Put、Get、Delete。我们在读操作的时候其实本质上就是直接从数据区中拿数据，只不过有时候需要先去Write区里面拿数据，拿到数据后再去DefaultCF中去取数据。只要是Put和Delete操作就只要丢到暂存区就好了。

在事务处理的过程中，Client扮演着重要的作用，用户处理Server所返回的error，从而重新发送事务数据



## Lab 2B

2B的目的是实现一个Store中的MVCC事务。

2B在PreWrite和Commit的时候用Latch又拦了一层的锁，`Latches` 的作用是 **序列化 (Serialize)** 同一个节点上针对 **同一个 Key** 的并发操作。它确保了 **“读-改-写” (Read-Modify-Write)** 这个过程是原子的。

Latches的本质是一个 **分段锁 (Sharded Lock)** 或者 **基于 Hash 的锁**。它把 Key Hash 到 1024 个槽位里，只锁对应的槽位。或者维护一个正在处理的 Key 集合。`sync.Mutex`，锁粒度太大，性能太差，很容易内存爆掉

对于Latches的设计可以学习一下。

后面稍微说一下两个函数一个是KVPreWrite()和KVCommit()函数。

-----





## Lab C

Percolator 把它所有的“跨节点协调”都压在了 **Primary Key** 的状态上。

- **Prewrite 阶段**：Client 必须确保**所有**参与者（无论在哪个集群/节点）都 Prewrite 成功。只要有一个失败，全员回滚。这是经典的 2PC 第一阶段。

- Commit 阶段：Client只需确保

  Primary Key成功 Commit。

  - **Primary Key Commit 成功** => **整个事务成功**。
  - **Primary Key Commit 失败** => **整个事务失败**。

Secondary Key 的命运完全绑定在 Primary Key 上。Secondary Lock 里存储了 Primary Key 的位置。

- 如果 Client 挂了：
  - 别的事务读到 Secondary Lock。
  - 它拿着指针去问 Primary Key：“大哥，你提交了吗？”
  - Primary 说：“我提交了” -> Secondary 也提交。
  - Primary 说：“我没提交” -> Secondary 也回滚。

**这就是跨集群的核心：**
它不需要所有节点之间互相通信。它只需要任意节点都能访问到 Primary Key 所在的那一个节点。

Percolator 把所有跨集群的key的协调全部交给了Client来做....其实逻辑真的不会非常难的感觉。

----

下面梳理一下4C写的函数的细节



