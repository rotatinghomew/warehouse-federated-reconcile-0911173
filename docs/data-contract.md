# 地区报送约定

来源声明 `full` 表示其明确覆盖范围内的完整业务键清册，`delta` 仅描述本次变化，`correction` 是替代某个已声明批次的新清册。三者都不能靠文件名推断身份或成功。

## 覆盖范围

`scope` 缺省为 `{"kind":"district"}`，即整个地区；也可以声明 `{"kind":"schools","schools":[...]}`，只覆盖指定学校。范围之外的学校不属于本次报送，清册缺失不能作用于它们，已发布且不在范围内的学校记录不会被清掉。`correction` 必须声明学校范围，且不得超出被 `replaces_batch` 指明的批次范围。

## 完整性声明

`complete` 缺省规则：`full` 与 `correction` 为真，`delta` 为假；`delta` 不能声明完整。未闭合清册（`complete:false`）中的缺失只记为 `unresolved_missing_key`，不能推导为删除。完整清册只删除其声明范围内、且未被较晚批次确认的业务键；范围外或较晚批次确认的键保留并记为业务差异。空 `full` 清册只删除其范围内已存在的键。

## 分片清单与补交

带 `manifest` 的报送声明全部传输片：`shard_id`、`declared_rows` 与 `checksum`。`declared_rows` 是清单内全部传输片行数之和，本次文件必须以 `shard_id` 指明自己是谁。摘要按传输片内容（批次、地区、序号、片标识与实际行）计算，不使用文件名；同名文件内容变化不会被当作同一次成功，改名但内容不变则可以识别为重放。

补交文件不带 `manifest`，只带 `batch`、`district`、`sequence`、`mode`、`shard_id` 与 `rows`，仓层把它归回原批次并沿用原批次的范围、清单与完整性状态。补交不能引入新片、改写范围或单独声明完整。同一片重复到达且内容一致返回 `False`；内容不一致报冲突。跨片重复业务键记为校验失败而不是覆盖。

## 序号与先后

序号由地区源端给定，按地区从 1 连续；`source_heads` 只在批次完整落地后推进，是水位而非最后收件时间。处理较早缺口时，较早批次不能覆盖较晚批次已确认的键，也不能让较晚的删除复活；此类动作记入 `adjustments`，不进入事实。较晚的完整清册覆盖同一范围后，较早批次的缺口记为已取代，不再永久阻塞发布，但仍在核对结果中列出。

## 核对与发布

`reconcile` 把来源声明、落地分片、仓层明细和汇总连接起来，差异分为：

- `missing_shard`：清单声明但尚未收到（传输）
- `checksum_mismatch` / `key_conflict`：摘要、行数或跨片业务键不符（校验）
- `stale_upsert` / `stale_delete`：较早批次被较晚确认挡住（业务差异）
- `missing_key`：未闭合清册中缺少的键，不能当作删除
- `outside_declared_scope` / `pending_declaration`：本就不在此次范围，或落在尚未确认完整的范围

`declared_rows` 是传输条目数，`landed_rows` 是落地条目数，`upserts - deletes` 是净变化，`facts` 是仓层事实数，四者不要求机械相等。`publish` 只发布完整性条件满足的地区，把其余地区连同未决范围写入 `deferred`，不补零、不丢弃有争议记录；`release_district` 在条件不满足时抛出 `IncompleteError` 并携带未决范围。发布名称一经写入不可改写。
