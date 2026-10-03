# coursewait — 进程内选课候补组件

Go 实现的进程内选课/候补库，包路径 `coursewait/waitlist`。单进程并发安全，
所有操作（含 `Inspect`）均可线性化；不依赖任何外部服务、不监听端口、无持久化。

## 语义

- `Enqueue(ticket, student, section)`：请求先入候补，返回前完成递补。`ticket`
  为调用方提供的唯一正整数，数字越小递补优先级越高。
- `Cancel(ticket)`：撤销候补或已选请求；撤销已选请求释放座位与学分，并在返回前
  完成由此引发的（可能跨教学班的）连锁递补。重复撤销无副作用。
- `Inspect(students, sections)`：返回指定学生与教学班的一致快照（已选关系、
  已选学分、按 ticket 排序的候补队列）。
- 递补规则：每步从全部可递补请求中取 ticket 最小者转为已选，直到没有可递补请求。
  可递补 = 教学班有空位 且 该学生加入后已选学分 ≤ 21。学分不足的早请求不阻塞
  后续符合条件的请求；已选请求不会被更小的 ticket 挤出。

## 并发设计

- 细粒度锁：每个学生、每个教学班各一把互斥锁；操作按固定全局顺序
  （学生按 id 排序在前、教学班按 id 排序在后）一次性收齐所需锁，无死锁。
- 退课连锁递补只触及本次关系变化牵连的实体（退课学生、被退教学班的候补者、
  该学生候补着的教学班）：先短暂读取计算闭包，再用版本号校验，变化则重试。
- 票号登记表由一把短临界区互斥锁保护，其下不做任何选课/撤销/递补工作。
- 互不相连（无已选/候补关系相连）的两组实体可完全并行地变更与读取。

## 使用

```go
e := waitlist.New(
    []waitlist.StudentID{"alice", "bob"},
    []waitlist.SectionSpec{
        {ID: "cs101-a", Capacity: 1, Credits: 3},
        {ID: "cs102-a", Capacity: 2, Credits: 6},
    },
)

_ = e.Enqueue(1, "alice", "cs101-a") // 有空位，立即转为已选
_ = e.Enqueue(2, "bob", "cs101-a")   // 无空位，进入候补
e.Cancel(1)                          // alice 退课，bob 在返回前被递补

snap := e.Inspect(
    []waitlist.StudentID{"alice", "bob"},
    []waitlist.SectionID{"cs101-a"},
)
// snap.Students["bob"].Credits == 3
// snap.Sections["cs101-a"].Enrolled == ["bob"]
```

## 测试

```sh
go test -race ./...
```

- `waitlist/waitlist_test.go`：确定性场景（18 学分并发申请两门 3 学分课、
  21 学分退课后按 ticket 优先递补、低 ticket 超学分不阻塞后继、撤销与递补交错）。
- `waitlist/pause_test.go`：用测试钩子把一组请求暂停在变更临界区内，证明
  不相连的另一组仍能完成变更与读取（channel 屏障，无 sleep 碰运气）。
- `waitlist/random_test.go` + `checker_test.go`：小规模顺序参考模型
  （`model.go`）+ 回溯搜索，对含 Inspect 返回值的短并发历史验证存在合法
  串行解释，而非只核对最终计数。
