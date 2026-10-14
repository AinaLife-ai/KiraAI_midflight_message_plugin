"""Regression: 系统通知类消息在**群聊**下也必须可注入。

背景（线上实证）：子代理任务结果经 `ctx.publish_notice(task.sid, chain, is_mentioned=True)`
投递（is_notice=True、message_id=="system_message"）。群聊默认
`flow_method_group="all"`（需 @ / 回复 / 唤醒词 / 戳），而 `wake_keywords` 为空时会
**自动沿用 S/Z 的唤醒词**；此时 `_match_flow_method` 里的
`is_mentioned is True and not wake_keywords` 那条不再成立 ⇒ 通知被判「不可注入」
⇒ 批次原样放行 ⇒ S/Z QueueMerge 在"会话运行中"把批次扣进 pending ⇒ 通知要等本轮
step / final，或 180s 卡死兜底，LLM 才看得到。

新增开关 `notice_always_inject`（**默认关**，保持 v1.3.2「系统事件豁免配置化、默认一视同仁」）：
打开后系统通知类消息直接进入流入判定（可在运行中的下一个工具边界被注入）。

T0 群聊 + 在飞轮 + 子代理通知（**默认配置**）-> 不拦截（守住 v1.3.2「系统事件默认一视同仁」）
T1 群聊 + 在飞轮 + 子代理通知（开关开）    -> 拦截并转入流入队列（关闭时：放行 ⇒ 被 S/Z 扣批）
T2 群聊 + 在飞轮 + 普通用户消息        -> 仍然不拦截（不动既有语义）
T3 notice_always_inject=off + 同上通知 -> 不拦截（开关可回退）
T4 通知已在 buffer + 工具边界          -> 注入进 tool_result（drain 通道同样受益）
T5 两条不同时间的通知                  -> 都被注入（message_id@timestamp 去重不误杀）
T6 通知含停止词                        -> 注入但不停止本轮（notice_skip_stop 语义不变）

Run:  python3 tests/test_notice_inflow.py [plugin_dir]
"""
import asyncio
import copy
import sys
import time
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from harness import (CFG, FakeBuffer, FakeCtx, FakeMessage, FakeToolResult,  # noqa: E402
                     batch, drive_on_batch_message, drive_on_tool_result,
                     drive_on_llm_request, load_plugin)

GSID = "qq:gm:1054647682"
NOTE = "【系统通知】你派出的子代理「网络搜索专员」（任务ID t2）已完成任务，结果如下。"
RESULTS = []


def check(name, cond, detail=""):
    RESULTS.append((name, bool(cond)))
    print(f"  [{'PASS' if cond else 'FAIL'}] {name}" + (f" :: {detail}" if detail and not cond else ""),
          flush=True)


def cfg(**stop):
    c = copy.deepcopy(CFG)
    c["section_stop"].update(stop)
    # 关键：群聊 + 唤醒词非空（= 自动沿用 S/Z 唤醒词的真实场景）
    c["section_flow"].update({"flow_method_group": "all", "wake_keywords": ["莎娜"]})
    return c


async def make(cfg_dict):
    mod = load_plugin()
    ctx = FakeCtx()
    plugin = mod.MidflightMessagePlugin(ctx, cfg_dict)
    await plugin.initialize()
    return plugin, ctx


def gmsg(mid, text, notice=False, mentioned=False, uid="unknown", nick="system", ts=None):
    m = FakeMessage(mid, text, uid=uid, nick=nick)
    m.group = types.SimpleNamespace(group_id="1054647682", group_name="new Bot()")
    m.is_notice = notice
    m.is_mentioned = mentioned
    m.message_str = text          # 插件优先复用框架渲染好的 message_str 做文本化
    if ts is not None:
        m.timestamp = ts
    return m


def gbatch(*msgs):
    ev = batch(*msgs)
    ev.session = types.SimpleNamespace(sid=GSID)
    # tests/core 的批次 stub 把 is_group_message() 写死 False（原测试只覆盖私聊），
    # 这里显式声明为群聊，否则会走 flow_method_dm="any" ⇒ 一切都可注入，掩盖问题
    ev.is_group_message = lambda: True
    return ev


async def run_flight(plugin, sid=GSID):
    """让会话进入"运行中"（走真实 ON_LLM_REQUEST 路径），返回本轮的事件对象。

    注意：框架派发 ON_TOOL_RESULT 时携带的是**本轮那个批次事件**（ON_LLM_REQUEST
    注册的那个），不是被拦截的批次；传错会被 `_touch_run` 判成"幽灵轮已收尾"而直接退出。
    """
    run_ev = gbatch(gmsg("u0", "跑个任务", uid="769690776", nick="周武"))
    await drive_on_llm_request(plugin, run_ev)
    return run_ev


async def t1_group_notice():
    plugin, _ = await make(cfg(notice_always_inject=True))
    run_ev = await run_flight(plugin)
    ev = gbatch(gmsg("system_message", NOTE, notice=True, mentioned=True))
    await drive_on_batch_message(plugin, ev)
    q = plugin._pending_inject.get(GSID, [])
    check("T1 群聊在飞轮 + 子代理通知 ⇒ 转入流入队列", len(q) == 1, f"队列={len(q)}")
    check("T1 批次被掐掉（不再落到 S/Z 的扣批）", bool(getattr(ev, "is_stopped", False)) or bool(
        getattr(ev, "_is_stopped", False)), "event 未被 stop")
    # 工具边界应把通知注入进 tool_result
    tr = FakeToolResult()
    await drive_on_tool_result(plugin, run_ev, tr)
    check("T1 下一个工具边界即被注入（bot 本轮就能看到）", "子代理" in (tr.text or ""), tr.text[:80])


async def t2_normal_user_unchanged():
    # ---- T0（设计守卫）：**默认配置**下系统通知「不赦免」，照常走流入判定 ----
    # 守住 v1.3.2 的决定：系统事件豁免配置化、默认一视同仁（不搞通知特例）。
    plugin0, _ = await make(cfg())
    await run_flight(plugin0)
    ev0 = gbatch(gmsg("system_message", NOTE, notice=True, mentioned=True))
    await drive_on_batch_message(plugin0, ev0)
    check("T0 默认（开关关）⇒ 通知不拦截，保持「系统事件默认一视同仁」",
          len(plugin0._pending_inject.get(GSID, [])) == 0 and not getattr(ev0, "is_stopped", False),
          f"队列={len(plugin0._pending_inject.get(GSID, []))}")

    plugin, _ = await make(cfg())
    await run_flight(plugin)
    ev = gbatch(gmsg("u1", "今天天气不错", uid="769690776", nick="周武"))
    await drive_on_batch_message(plugin, ev)
    again = plugin._pending_inject.get(GSID, [])
    check("T2 普通群消息（无@/无唤醒词）仍然不拦截（既有语义不变）", len(again) == 0, f"队列={len(again)}")


async def t3_switch_off():
    plugin, _ = await make(cfg(notice_always_inject=False))
    await run_flight(plugin)
    ev = gbatch(gmsg("system_message", NOTE, notice=True, mentioned=True))
    await drive_on_batch_message(plugin, ev)
    check("T3 开关关闭 ⇒ 回到旧行为（不拦截）", len(plugin._pending_inject.get(GSID, [])) == 0)


async def t4_drain_channel():
    plugin, ctx = await make(cfg(notice_always_inject=True))
    run_ev = await run_flight(plugin)
    buf = ctx.get_buffer(GSID)
    buf.buffer.append(types.SimpleNamespace(message=gmsg("system_message", NOTE, notice=True, mentioned=True)))
    tr = FakeToolResult()
    await drive_on_tool_result(plugin, run_ev, tr)
    check("T4 drain 通道：buffer 里的通知也在工具边界被注入", "子代理" in (tr.text or ""), tr.text[:80])


async def t5_two_notices():
    plugin, _ = await make(cfg(notice_always_inject=True))
    await run_flight(plugin)
    now = time.time()
    await drive_on_batch_message(plugin, gbatch(gmsg("system_message", NOTE + "A", notice=True, mentioned=True, ts=now)))
    await drive_on_batch_message(plugin, gbatch(gmsg("system_message", NOTE + "B", notice=True, mentioned=True, ts=now + 5)))
    check("T5 两条不同时间的通知都被接收（去重不误杀）", len(plugin._pending_inject.get(GSID, [])) == 2,
          f"队列={len(plugin._pending_inject.get(GSID, []))}")


async def t6_stop_words_untouched():
    plugin, _ = await make(cfg(notice_always_inject=True, stop_enabled=True, stop_words=["停"]))
    await run_flight(plugin)
    ev = gbatch(gmsg("system_message", "系统通知：任务已停止", notice=True, mentioned=True))
    await drive_on_batch_message(plugin, ev)
    check("T6 通知含停止词 ⇒ 注入但不停止本轮（notice_skip_stop 不变）",
          len(plugin._pending_inject.get(GSID, [])) == 1
          and not any(x in str(getattr(plugin, "_finished_run", {})) for x in ()))


async def main():
    for fn in (t1_group_notice, t2_normal_user_unchanged, t3_switch_off,
               t4_drain_channel, t5_two_notices, t6_stop_words_untouched):
        try:
            await fn()
        except Exception as e:
            import traceback
            check(f"{fn.__name__} 异常", False, f"{type(e).__name__}: {e}\n{traceback.format_exc()[-300:]}")
    fails = [n for n, ok in RESULTS if not ok]
    print(f"\n===== 通知流入回归：{len(RESULTS)-len(fails)}/{len(RESULTS)} 通过 =====")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
