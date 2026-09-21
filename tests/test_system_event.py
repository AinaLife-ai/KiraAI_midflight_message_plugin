"""Regression: system-triggered events must pass through untouched.

S版的私聊主动回复（system_proactive_dm）与定时任务（system_scheduled）
构造 is_mentioned=True 的合成事件走正常消息链；官方系统消息 sender 为
system_message。若此时该 sid 有轮在飞，批次守卫**不得**把这批系统提示词
拦截转入流入队列（否则原计划的新一轮 LLM 请求消失、工具黑名单失效）。

T1 有轮在飞 + system_proactive_dm 批次     -> 放行：不掐批次、不入流入队列
T2 有轮在飞 + system_scheduled 含停止词     -> 放行：不判停止词、不停本轮
T3 有轮在飞 + 官方 system_message 批次      -> 放行
T4 有轮在飞 + 未知 system_ 前缀 sender      -> 放行（前缀豁免）
T5 混合批次（系统 + 用户消息）              -> 用户消息仍按原规则转入流入队列
T6 空闲会话 + 系统批次                      -> 照常放行（对照）

Run:  python3 tests/test_system_event.py [plugin_dir]
"""
import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from harness import (FakeMessage, batch, drive_on_batch_message,  # noqa: E402
                     make_plugin)


def live_run(plugin, sid, ev):
    """Put the session into a real 'running' state (registered + heartbeated)."""
    plugin._run_active[sid] = {"event": ev, "ts": time.time(), "ending": False}
    if hasattr(plugin, "_finished_run"):
        plugin._finished_run.pop(sid, None)


async def t1_proactive_dm_not_intercepted(plugin, sid):
    ev = batch(FakeMessage("100", "跑个任务"))
    live_run(plugin, sid, ev)
    b = batch(FakeMessage("200", "（主动回复提示词）",
                          uid="system_proactive_dm", nick="系统"))
    b.messages[0].is_mentioned = True   # 与 S版合成事件一致
    await drive_on_batch_message(plugin, b)
    return (not b.is_stopped and not ev.is_stopped
            and not plugin._pending_inject.get(sid))


async def t2_scheduled_stop_word_not_judged(plugin, sid):
    ev = batch(FakeMessage("100", "跑个任务"))
    live_run(plugin, sid, ev)
    b = batch(FakeMessage("201", "停", uid="system_scheduled", nick="系统"))
    await drive_on_batch_message(plugin, b)
    return (not b.is_stopped and not ev.is_stopped
            and not plugin._pending_inject.get(sid))


async def t3_official_system_message_passes(plugin, sid):
    ev = batch(FakeMessage("100", "跑个任务"))
    live_run(plugin, sid, ev)
    b = batch(FakeMessage("202", "后台任务已完成", uid="system_message", nick="系统"))
    await drive_on_batch_message(plugin, b)
    return (not b.is_stopped and not plugin._pending_inject.get(sid))


async def t4_unknown_system_prefix_passes(plugin, sid):
    ev = batch(FakeMessage("100", "跑个任务"))
    live_run(plugin, sid, ev)
    b = batch(FakeMessage("203", "系统提醒", uid="system_anything_else", nick="系统"))
    await drive_on_batch_message(plugin, b)
    return (not b.is_stopped and not plugin._pending_inject.get(sid))


async def t5_mixed_batch_user_message_still_intercepted(plugin, sid):
    ev = batch(FakeMessage("100", "跑个任务"))
    live_run(plugin, sid, ev)
    b = batch(FakeMessage("204", "（主动回复提示词）",
                          uid="system_proactive_dm", nick="系统"),
              FakeMessage("205", "顺便排个序"))
    await drive_on_batch_message(plugin, b)
    queued = plugin._pending_inject.get(sid, [])
    # 用户消息照常转入流入队列；系统消息不进入任何消费通道
    return (b.is_stopped and len(queued) == 1
            and "顺便排个序" in "".join(it[1] for it in queued))


async def t6_idle_session_system_batch_untouched(plugin, sid):
    b = batch(FakeMessage("206", "（主动回复提示词）",
                          uid="system_proactive_dm", nick="系统"))
    await drive_on_batch_message(plugin, b)
    return not b.is_stopped and not plugin._pending_inject.get(sid)


SCENARIOS = [
    ("T1 live run: system_proactive_dm batch passes through",
     lambda p, s, c: t1_proactive_dm_not_intercepted(p, s)),
    ("T2 live run: system_scheduled stop word is not judged",
     lambda p, s, c: t2_scheduled_stop_word_not_judged(p, s)),
    ("T3 live run: official system_message batch passes through",
     lambda p, s, c: t3_official_system_message_passes(p, s)),
    ("T4 live run: unknown system_* sender passes through",
     lambda p, s, c: t4_unknown_system_prefix_passes(p, s)),
    ("T5 live run: mixed batch still intercepts user message only",
     lambda p, s, c: t5_mixed_batch_user_message_still_intercepted(p, s)),
    ("T6 idle session: system batch untouched (control)",
     lambda p, s, c: t6_idle_session_system_batch_untouched(p, s)),
]


async def main():
    dirs = sys.argv[1:] or ["."]
    failed = False
    for d in dirs:
        plugin_dir = Path(d).resolve()
        print(f"\n=== {plugin_dir} ===")
        for name, fn in SCENARIOS:
            plugin, ctx, sid = await make_plugin(plugin_dir)
            ok = await fn(plugin, sid, ctx)
            print(f"  {'PASS' if ok else 'FAIL'}  {name}")
            failed = failed or not ok
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
