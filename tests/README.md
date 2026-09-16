# tests/ — 无需安装 KiraAI 的回归测试

直接用 **真实插件代码** 驱动钩子，调用顺序与框架逐行对齐：

| 测试里的调用 | 框架里的位置 |
|---|---|
| `_track_run_start` | `core/message_manager.py` → `ON_LLM_REQUEST` |
| `_ensure_stop_checkpoint` | `core/agent/agent_executor.py` → `ON_LLM_RESPONSE`（每步） |
| `_handle_tool_result` × N | `core/agent/func_tool_manager.py` → `ON_TOOL_RESULT`，**对一次 LLM 响应里的每个 tool_call 各派发一次** |
| `on_batch_dedup` | `core/message_manager.py` → `ON_IM_BATCH_MESSAGE` |

`tests/core/` 是 KiraAI `core` 的**最小替身**（只含插件真正 import 的那点东西：
`on`/`Priority`/`BasePlugin`/`logger` + 两种消息类型），所以**不需要跑框架、不需要 WebUI、不需要任何第三方依赖**，
只要 Python 3.11+。

## 跑

```bash
python3 tests/run_tests.py                      # 测当前这份代码
python3 tests/run_tests.py . ../midflight-1.2.8 # 前后对比：传多个插件目录
```

退出码非 0 = 有失败。

## 两个测试文件

- **`test_phantom_run.py`** —— 修的是「幽灵运行中」：
  一轮的最后一步**一次派发 3 个工具调用**（就是线上日志里的
  `search_files **/sustained*` / `**/Sustained*` / `**/*sustained*`），
  之后检查：会话不再残留"运行中"状态、后续批次不被拦截、没有消息卡在流入队列、
  也没有消息被当成"停止正在跑的那轮"消费掉。
  > 在 **v1.2.8 及更早**的代码上跑，这 4 项**全部 FAIL**
  > （phantom=True、4/4 批次被拦截、2 条卡住、2 条被吞）—— 就是线上那个现象的完整复现。
- **`test_stuck_paths.py`** —— "本轮收尾信号没送到"的完整清单（v1.3.0 修）：
  S1 命中停止词（工具边界路径）、S2 命中停止词（批次路径）、S3 本轮根本没跑起来
  （被别的插件 stop / 中途异常）、S4 正常跑完（对照）、S5 看门狗兜底。
  每个场景最后都会**再发一条用户消息**：被拦截且没人处理 = 卡住（只能等 180s 兜底）。
  > 在 **v1.2.9** 上 S1/S2/S3 全部失败；v1.3.0 全部通过。
- **`test_scenarios.py`** —— 保证修复没有误伤正常功能：
  S1 运行中批次 → 下个工具边界注入、S2 运行中停止词、S3 buffer 消息 drain 注入、
  S4 同会话新一轮跟踪、S5 中间步仍算运行中。修复前后都应为 PASS。
- **`test_foreign_event.py`** —— "外来事件"（第三方插件自造桩事件）不得污染状态机（v1.3.1 修）：
  F1 空闲时桩事件不造"运行中"、F2 真实轮进行中桩事件不篡改状态/不吃队列/不把用户消息
  灌进子代理的工具结果、F3 桩事件后仍能正常开新轮（对照）、F4 `ON_FINAL_RESULT` 权威收尾、
  F5 `ON_FINAL_RESULT` 对外来事件不响应且幂等、F6 收尾后的迟到边界被忽略（回归）。
  > 在 **v1.3.0** 上 F1/F2/F3 全部失败（幽灵运行中 + 用户消息被灌进子代理工具结果 +
  > 后续批次全被拦）—— 就是"跑了子代理之后群里消息没人处理"的完整复现。

## 复现 / 验证记录（v1.2.9）

| | 幽灵运行中 | 被拦截批次 | 卡在流入队列 | 被当停止词吞掉 |
|---|---|---|---|---|
| v1.2.8（修复前） | **True** | **4/4** | **2** | **2** |
| v1.2.9（修复后） | False | 0/4 | 0 | 0 |

## 复现 / 验证记录（v1.3.1：子代理桩事件）

| | 桩事件造"运行中" | 用户消息被灌进子代理 | 后续批次被拦 |
|---|---|---|---|
| v1.3.0（修复前） | **True** | **True** | **True** |
| v1.3.1（修复后） | False | False | False |
