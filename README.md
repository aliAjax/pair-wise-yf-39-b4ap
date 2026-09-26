# 野生动物疫病监测与离线同步

这是一个只使用Python标准库和SQLite的模块化项目，默认端口为`8305`。所有业务规则集中在`src/rules.py`，`app.py`只负责组装依赖和启动服务。

## 模块结构

- `app.py`：命令行参数、依赖组装、启动和信号处理。
- `src/domain.py`：角色、数据结构、领域异常和基础校验。
- `src/rules.py`：状态机、权限、领域计算、冲突和跨对象校验。
- `src/repository.py`：SQLite建表、查询、事务和乐观锁。
- `src/service.py`：用例编排、幂等处理、版本控制和审计写入。
- `src/http_api.py`：HTTP路由、请求解析和统一错误响应。
- `src/audit.py`：实体操作审计时间线。
- `static/index.html`：最小演示页面。
- `tests/`：完整流程、规则和失败场景测试。

## 初始化与启动

```bash
python3 app.py --db ./data.db --port 8305
```

服务启动时会自动建表。`--host`可修改监听地址，`--db`可指定其他SQLite文件。

## 核心对象

- `observation`：现场观察；`sample`：样本与实验室结果；`cluster`：异常聚集事件。

## 主要接口

- `GET /health`：健康检查。
- `GET /api/<kind>`：按对象类型查询，可用`?status=`过滤。
- `POST /api/<kind>`：创建对象；请求体为JSON。
- `GET /api/entities/<id>`：读取对象当前版本。
- `POST /api/entities/<id>/actions`：提交`{"action":"动作名","data":{...},"expected_version":数字}`。
- `GET /api/audit`：读取审计记录。
- `POST /api/sync/batches`：提交离线批次`{"batch_id":"...","operations":[...]}`。
- `GET /api/sync/batches`：列出已接收批次及游标。
- `GET /api/sync/conflicts`：列出待处理冲突（`?status=`可过滤）。
- `GET /api/sync/changes?since=<cursor>`：取回游标之后的变更。

请求身份通过`X-User-Id`和`X-Role`请求头传入。创建和动作的可执行角色由规则引擎控制。

## 离线批次同步

野外终端断网期间的修改按批次上传，批次内每个操作形如：

```json
{"op": "create", "kind": "observation", "client_op_id": "o1", "data": {...}}
{"op": "transition", "entity_id": "...", "action": "submit", "base_version": 1, "data": {...}}
```

- 同一`batch_id`重发且内容一致时，返回第一次的处理结果（`replayed: true`），不会重复建单。
- 同一`batch_id`但内容不同时，返回409冲突。
- `transition`必须携带`base_version`；版本过期的操作不会改动实体，只登记一条待处理冲突和失败原因。
- 成功批次返回递增`cursor`，现场端用`GET /api/sync/changes?since=<cursor>`取回后续变更。
- 批次、冲突和变更流水都写入SQLite，服务重启后仍然保留。

## 测试

```bash
python3 -m unittest discover -s tests -v
```

## 局限

离线同步使用批次和幂等键演示，不包含真实野外通信协议、地图底图或完整空间索引。
