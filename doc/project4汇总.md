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

下面梳理一下4C写的函数的细节,

```go
func (server *Server) KvScan
func (server *Server) KvCheckTxnStatus
func (server *Server) KvBatchRollback
func (server *Server) KvResolveLock
```

- KvScan

这个函数的作用是扫描一个事务中有的key。

这个函数的关键就是Scanner的实现，这个实现还是蛮难的，对于一个Store来说，存储的key的不同版本是并列排在一起的，所以在扫描的过程中不能完全顺下来扫，需要跳过旧版本的数据，而且我们也不能返回是delete的数据，因为已经删了不存于事务中。

1、如何跳过旧版本的数据，我们在Key的末尾的加一个0，表示最大的版本字节（这是由数据在数据库中的排列特性决定的，因为后面是时间戳，所以加一个0可以保证跳过所有的时间戳）

2、我们需要从拿到的write中去将delete类型的数据给跳过。然后就是按照limit拿数据

- KvCheckTxnStatus

​		这个函数的作用是检查主键状态的，在别的事务访问一个Key的时候发现有锁，这个时候就需要去调用这个函数来检查主键的情况从而决定现在要干些啥东西

​		现在我是主键所在的Store，我调了KvCheckTxnStatus，首先我要去查write区看看有没有提交，如果提交了，并且不是RollBack，就返回就好了

​		如果有锁而且没提交，我们要检查是否达到了TTL，如果达到了我们要手动回滚这个事务（范围error给client），如果没有达到的话就把TTL返回给Client就好了

​                                                                                                                                                                                                                                             

- KvBatchRollback

KvBatchRollback这个函数是client发现主键挂了，现在要把这个事务所有其他的key全部都清除的时候，需要清除key的store调用的函数。

如果有锁，先判断是不是需要删除的位置的锁，如果是，就把这把锁删掉，并且附带一个rollback事务就好了。最后把这个事务持久化就好了。

- KvResolveLock

​	这个函数的使用场景是在一个事务查询key的过程中发现，这个key是从键而且有锁，这个时候如何处理这个key就完全取决于主键的状态，如果主键已经提交并且不是回滚的类型，这个时候就commit这个事务在当前store的key就好了，如果已经持有锁但是过期，或者是主键rollback这个时候就回滚。



-----

事务流程：

### 案例 1：Client 在 Prewrite 阶段挂了 (主动/被动回滚)

**场景：**
Client 想要修改 A (Primary) 和 B (Secondary)。

1. **Prewrite(A)**: 成功。A 上有了锁 (StartTS=100)。
2. **Prewrite(B)**: **失败**（比如网络断了，或者 Client 崩溃了）。
3. **结果**：A 被锁住了，B 没锁（或者也没写进去）。整个事务处于“半死不活”状态。

**Txn C (StartTS=120) 进场：**
Txn C 想读 Key A。

1. **读 A**: 发现 A 有锁 (StartTS=100, Primary=A)。
2. **Check**: Txn C 向 Store 1 发送 `KvCheckTxnStatus(Primary=A, StartTS=100)`。
3. Store 1 检查：
   - Lock(A) 还在。
   - Write CF 没有提交记录。
   - 检查 TTL：
     - **如果没过期**：Store 1 告诉 Txn C：“它还没死透，你再等等”。Txn C 只能 Backoff 重试。
     - 如果过期了：Store 1 判定 Txn A 死亡。
       - **Action**: 删除 Lock(A)，写入 `Write(A, StartTS=100, Kind=Rollback)`。
       - **返回**: `Action_TTLExpireRollback`。
4. Txn C Resolve:
   - 既然 A 滚了，那 B 也得滚（如果 B 有锁的话）。
   - Txn C 向 Store 2 发送 `KvResolveLock(StartTS=100, CommitTS=0)`。
   - Store 2 扫描 Lock CF，如果发现有属于 Txn A 的锁（如果有的话），全部删掉并写 Rollback。

**结局**：Txn A 彻底消失，就像没发生过一样。Txn C 可以继续读写 A 了。

------

### 案例 2：Client 在 Commit 阶段挂了 (Primary 未提交)

这是最惊险的时刻。

**场景：**
Client 成功 Prewrite 了 A 和 B。
Client 准备 Commit。

1. **Client 发送 Commit(A)**：**网络超时/丢包**，请求没到 Store 1，或者到了但没处理完 Client 就挂了。
2. 结果：
   - A: 锁还在 (未提交)。
   - B: 锁还在 (未提交)。

**Txn C 进场：**
Txn C 想读 Key B。

1. **读 B**: 发现锁 (Primary=A)。
2. **Check**: 查 A 的状态 (`KvCheckTxnStatus`)。
3. Store 1 检查：
   - Lock(A) 还在。
   - Write CF 无提交记录。
   - **判定**：只要 Primary 没提交，整个事务就是没提交。
   - **处理**：如果 TTL 过期，执行 Rollback (同案例 1)。

**结局**：因为 Primary 没成，所以大家都得死。Txn A 全员回滚。

------

### 案例 3：Client 在 Commit 阶段挂了 (Primary 已提交) —— **反转！**

这是 Percolator 最精髓的地方。

**场景：**
Client 成功 Prewrite 了 A 和 B。

1. Client 发送 Commit(A)：成功！
   - Store 1: A 的锁删了，Write CF 里有了 `Commit(A, CommitTS=110)`。
2. **Client 准备 Commit(B)**：**挂了！**
3. 结果：
   - A: 已提交。
   - B: 锁还在 (Primary=A)。

**Txn C 进场：**
Txn C 想读 Key B。

1. **读 B**: 发现锁 (Primary=A)。
2. **Check**: 查 A 的状态 (`KvCheckTxnStatus`)。
3. Store 1 检查：
   - Lock(A) 没了（因为提交时删了）。
   - 查 Write CF：**找到了！** `Commit(A, TS=110)`。
   - **返回**: `CommitVersion = 110`。
4. Txn C Resolve:
   - Txn C 恍然大悟：“原来大哥 A 已经成了！那小弟 B 也得成！”
   - Txn C 向 Store 2 发送 `KvResolveLock(StartTS=100, CommitTS=110)`。
5. Store 2 执行：
   - 找到 B 的锁。
   - **执行 Commit(B)**。

**结局**：虽然 Client 挂了，但因为 Primary 已经成了，后续来的 Txn C **帮忙** 把剩下的 B 也提交了。Txn A 最终是 **成功** 的。

 总结 "Primary 决定论"

| Primary Key 状态         | Secondary Key 状态 | 最终结果          | 谁来执行                      |
| :----------------------- | :----------------- | :---------------- | :---------------------------- |
| **Lock 还在 (TTL 内)**   | Lock 还在          | **等待**          | 无 (Client 稍后重试)          |
| **Lock 还在 (TTL 过期)** | Lock 还在          | **全员 Rollback** | 别的事务 (CheckTxnStatus)     |
| **已 Commit**            | Lock 还在          | **全员 Commit**   | 别的事务 (ResolveLock)        |
| **已 Rollback**          | Lock 还在          | **全员 Rollback** | 别的事务 (ResolveLock)        |
| **啥都没查到**           | Lock 还在          | **全员 Rollback** | 别的事务 (认为 Prewrite 失败) |

----

## 在测试中遇到的问题

1、我们在获取锁的时候，要注意一个问题，在MVCC的场景下，锁也是有版本的，所以在获取锁的时候一定要记得去比较锁版本

