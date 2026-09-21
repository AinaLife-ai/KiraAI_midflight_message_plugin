"""Regression: system event handling is configurable, not unconditional.

v1.3.2 起不再默认特殊照顾系统消息，改为两个开关：
- system_trigger_passthrough（默认关）：触发型系统事件（S版主动回复
  system_proactive_dm / 定时任务 system_scheduled / system_ 前缀 sender）
  撞上在飞轮时不拦截、不判停、不转注入，原样留在批次里放行开新轮。
  默认关 = 照常走注入判定（可拦进在飞轮）。
- notice_skip_stop（默认开）：所有 system 来源消息——系统提醒（is_notice=True
  或 message_id=="system_message"，框架 publish_notice 的产物）与触发型系统
  事件（system_* sender）——可注入在飞轮，但永不参与停止词判定（v1.3.2 后续
  扩展：此前只覆盖系统提醒）。

T1 默认配置 + 在飞轮 + system_proactive_dm   -> 照常拦截：被转入流入队列
T2 passthrough 开 + 在飞轮 + system_scheduled  -> 放行：不掐批次、不判停、不入队
T3 默认 + 在飞轮 + notice 文本含停止词          -> 注入在飞轮且本轮不被停
T4 notice_skip_stop 关 + 同上 notice           -> 命中停止词停轮（一视同仁）
T5 默认 + 普通用户消息含停止词                  -> 停轮（不变性对照）
T6 空闲会话 + 系统触发事件                      -> 放行开新轮（无 run 拦截本就不生效）
T7 默认 + 在飞轮 + system_proactive_dm 含"停"  -> 拦截转注入队列，但本轮不停
T8 默认 + 在飞轮 + system_scheduled 含"停止"   -> 拦截转注入队列，但本轮不停
T9 notice_skip_stop 关 + system_proactive_dm 含"停" -> 命中停止词停轮（开关总控）

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


def notice(mid, text):
    """框架 publish_notice 产物的形状：is_notice=True，message_id 恒为
    "system_message"，sender.user_id 群聊为 unknown / 私聊为 sid。"""
    m = FakeMessage("system_message", text, uid="unknown", nick="系统")
    m.is_notice = True
    return m


async def t1_default_system_trigger_intercepted_like_user(plugin, sid):
    """默认（passthrough 关）：system_proactive_dm 与普通消息一样被拦截转注入。"""
    ev = batch(FakeMessage("100", "跑个任务"))
    live_run(plugin, sid, ev)
    b = batch(FakeMessage("200", "（主动回复提示词）",
                          uid="system_proactive_dm", nick="系统"))
    b.messages[0].is_mentioned = True   # 与 S版合成事件一致
    await drive_on_batch_message(plugin, b)
    queued = plugin._pending_inject.get(sid, [])
    return (b.is_stopped and not ev.is_stopped and len(queued) == 1
            and "主动回复提示词" in "".join(it[1] for it in queued))


async def t2_passthrough_on_system_trigger_untouched(plugin, sid):
    """passthrough 开：system_scheduled 不拦截、不判停（文本含停止词也不停）。"""
    plugin.system_trigger_passthrough = True
    ev = batch(FakeMessage("100", "跑个任务"))
    live_run(plugin, sid, ev)
    b = batch(FakeMessage("201", "停", uid="system_scheduled", nick="系统"))
    await drive_on_batch_message(plugin, b)
    return (not b.is_stopped and not ev.is_stopped
            and not plugin._pending_inject.get(sid))


async def t3_default_notice_injected_but_never_stops(plugin, sid):
    """默认（notice_skip_stop 开）：notice 含停止词仍被注入，本轮不被停。"""
    ev = batch(FakeMessage("100", "跑个任务"))
    live_run(plugin, sid, ev)
    b = batch(notice("ignored", "你已停止响应太久了，记得回来看看"))
    await drive_on_batch_message(plugin, b)
    queued = plugin._pending_inject.get(sid, [])
    return (b.is_stopped and not ev.is_stopped and len(queued) == 1
            and "停止响应" in "".join(it[1] for it in queued))


async def t4_notice_skip_stop_off_notice_can_stop(plugin, sid):
    """notice_skip_stop 关：同一条 notice 命中停止词，停掉在飞轮。"""
    plugin.notice_skip_stop = False
    ev = batch(FakeMessage("100", "跑个任务"))
    live_run(plugin, sid, ev)
    b = batch(notice("ignored", "你已停止响应太久了，记得回来看看"))
    await drive_on_batch_message(plugin, b)
    return (b.is_stopped and ev.is_stopped
            and not plugin._pending_inject.get(sid))


async def t5_user_stop_word_still_stops(plugin, sid):
    """不变性对照：默认配置下普通用户消息含停止词照样停轮。"""
    ev = batch(FakeMessage("100", "跑个任务"))
    live_run(plugin, sid, ev)
    b = batch(FakeMessage("202", "停，不用做了"))
    await drive_on_batch_message(plugin, b)
    return (b.is_stopped and ev.is_stopped
            and not plugin._pending_inject.get(sid))


async def t6_idle_session_system_trigger_untouched(plugin, sid):
    """空闲会话：无 run 时拦截逻辑本就不生效，系统触发事件照常放行开新轮。"""
    b = batch(FakeMessage("203", "（主动回复提示词）",
                          uid="system_proactive_dm", nick="系统"))
    await drive_on_batch_message(plugin, b)
    return not b.is_stopped and not plugin._pending_inject.get(sid)


async def t7_default_system_trigger_with_stop_word_injected_not_stops(plugin, sid):
    """默认（notice_skip_stop 开）：system_proactive_dm 文本含"停"，
    仍被拦截转入流入队列，但本轮不被停（系统来源消息不触发停止词）。"""
    ev = batch(FakeMessage("100", "跑个任务"))
    live_run(plugin, sid, ev)
    b = batch(FakeMessage("204", "（主动回复提示词：停下手头的事，回复用户）",
                          uid="system_proactive_dm", nick="系统"))
    b.messages[0].is_mentioned = True   # 与 S版合成事件一致
    await drive_on_batch_message(plugin, b)
    queued = plugin._pending_inject.get(sid, [])
    return (b.is_stopped and not ev.is_stopped and len(queued) == 1
            and "停下" in "".join(it[1] for it in queued))


async def t8_default_scheduled_with_stop_word_injected_not_stops(plugin, sid):
    """默认（notice_skip_stop 开）：system_scheduled 文本含"停止"，同上。"""
    ev = batch(FakeMessage("100", "跑个任务"))
    live_run(plugin, sid, ev)
    b = batch(FakeMessage("205", "（定时任务：停止等待，立即执行）",
                          uid="system_scheduled", nick="系统"))
    await drive_on_batch_message(plugin, b)
    queued = plugin._pending_inject.get(sid, [])
    return (b.is_stopped and not ev.is_stopped and len(queued) == 1
            and "停止等待" in "".join(it[1] for it in queued))


async def t9_notice_skip_stop_off_system_trigger_can_stop(plugin, sid):
    """notice_skip_stop 关（总控验证）：system_proactive_dm 含"停"命中停止词停轮。"""
    plugin.notice_skip_stop = False
    ev = batch(FakeMessage("100", "跑个任务"))
    live_run(plugin, sid, ev)
    b = batch(FakeMessage("206", "停", uid="system_proactive_dm", nick="系统"))
    b.messages[0].is_mentioned = True
    await drive_on_batch_message(plugin, b)
    return (b.is_stopped and ev.is_stopped
            and not plugin._pending_inject.get(sid))


SCENARIOS = [
    ("T1 default: system_proactive_dm intercepted like a user message",
     lambda p, s, c: t1_default_system_trigger_intercepted_like_user(p, s)),
    ("T2 passthrough on: system_scheduled passes through untouched",
     lambda p, s, c: t2_passthrough_on_system_trigger_untouched(p, s)),
    ("T3 default: notice with stop word is injected, run not stopped",
     lambda p, s, c: t3_default_notice_injected_but_never_stops(p, s)),
    ("T4 notice_skip_stop off: same notice stops the run",
     lambda p, s, c: t4_notice_skip_stop_off_notice_can_stop(p, s)),
    ("T5 default: user stop word still stops (control)",
     lambda p, s, c: t5_user_stop_word_still_stops(p, s)),
    ("T6 idle session: system trigger passes through (no run)",
     lambda p, s, c: t6_idle_session_system_trigger_untouched(p, s)),
    ("T7 default: system_proactive_dm with stop word injected, run not stopped",
     lambda p, s, c: t7_default_system_trigger_with_stop_word_injected_not_stops(p, s)),
    ("T8 default: system_scheduled with stop word injected, run not stopped",
     lambda p, s, c: t8_default_scheduled_with_stop_word_injected_not_stops(p, s)),
    ("T9 notice_skip_stop off: system trigger with stop word stops the run",
     lambda p, s, c: t9_notice_skip_stop_off_system_trigger_can_stop(p, s)),
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
