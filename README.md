# 分地区活动报送批次与仓层完整性核对

这是一个处理本地数据交换的仓湖项目。示例使用虚构机构和匿名合成记录，不连接业务平台。

## 数据契约

报送可按传输片声明清单，也可声明只覆盖指定学校。`full` 与 `correction` 默认是闭合清册，`delta` 只描述变化；未闭合清册中的缺失不会推导为删除。补交片归回其原批次，更正批次只替代明确声明的范围。来源声明、实际落地、仓层明细与面向使用方的汇总通过 `reconcile` 对账，差异按传输、校验和业务口径分类。完整性条件满足后才发布对应口径的事实与汇总，否则把未决范围留在 `deferred`。`declared_rows` 是传输行数，不是净新增人数。

## 代码职责

- `districtloads/exchange.py`：报送声明、覆盖范围、分片清单、摘要与数据契约。
- `districtloads/warehouse.py`：分片归批、按序号防回写、范围化替换与显式删除。
- `districtloads/reconcile.py`：来源声明、落地分片、仓层、汇总核对与受完整性约束的发布。
- `districtloads/storage.py`：本地事务、内容摘要和查询工具。

## 安装与运行

使用 Python 3.11（兼容该系列补丁版本，独立容器镜像以 SHA256 摘要固定）。构建工具和运行依赖均按精确版本登记。先执行 `python3 -m pip install -r requirements-build.txt -r requirements.txt`；依赖仅在准备环境时安装，运行与测试均可断网。

```sh
python3 -m compileall -q districtloads tests
python3 -m build --wheel --no-isolation
python3 -m unittest discover -s tests -v
python3 -m districtloads
```

默认示例在独立临时目录执行，结束后删除目录。需要检查持久文件时可执行 `python3 -m districtloads --root state`。测试自己创建临时目录，不依赖固定端口、外部服务或其他用例顺序。

## 容器

```sh
docker build -t federated-reconcile:local .
docker run --rm --network none federated-reconcile:local
docker run --rm --network none federated-reconcile:local python3 -m districtloads
```

容器构建负责预装锁定依赖，启动不下载包。示例输出是真实接入、查询和发布的数据，不读取预制输出。测试及示例可分别运行，退出码非零表示失败。

## 交换口径

进一步的字段含义、范围及兼容边界见 `docs/data-contract.md`。
