"""Regression: the normal mid-flight paths must keep working.

S1 a batch arriving while a turn really is running -> queued, then injected
   into the next tool result
S2 a stop word while a turn really is running -> run stopped, nothing queued
S3 messages already in the session buffer -> drained into the tool result
S4 a new turn on the same session -> tracked again
S5 an intermediate step (not the last one) -> still counts as running

Run:  python3 tests/test_scenarios.py [plugin_dir]
"""
import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from harness import (BufferedShim, FakeMessage, FakeResp, FakeToolResult, batch,  # noqa: E402
                     drive_on_batch_message, drive_on_llm_request,
                     drive_on_llm_response, drive_on_tool_result, make_plugin)


def live_run(plugin, sid, ev):
    """Put the session into a real 'running' state (registered + heartbeated)."""
    plugin._run_active[sid] = {"event": ev, "ts": time.time(), "ending": False}
    if hasattr(plugin, "_finished_run"):
        plugin._finished_run.pop(sid, None)


async def s1_live_batch_is_queued_then_injected(plugin, sid):
    ev = batch(FakeMessage("100", "跑个任务"))
    live_run(plugin, sid, ev)
    await drive_on_llm_response(plugin, ev, 1, [{"id": "t"}])

    b = batch(FakeMessage("200", "顺便排个序"))
    await drive_on_batch_message(plugin, b)
    queued = len(plugin._pending_inject.get(sid, []))

    tr = FakeToolResult()
    await drive_on_tool_result(plugin, ev, tr)
    return b.is_stopped and queued == 1 and "顺便排个序" in tr.text


async def s2_stop_word_on_live_run(plugin, sid):
    ev = batch(FakeMessage("100", "跑个任务"))
    live_run(plugin, sid, ev)
    b = batch(FakeMessage("200", "停"))
    await drive_on_batch_message(plugin, b)
    return b.is_stopped and ev.is_stopped and not plugin._pending_inject.get(sid)


async def s3_buffer_message_drained(plugin, sid, ctx):
    ev = batch(FakeMessage("100", "跑个任务"))
    live_run(plugin, sid, ev)
    ctx.get_buffer(sid).buffer.append(BufferedShim(FakeMessage("300", "我在旁边说一句")))
    tr = FakeToolResult()
    await drive_on_tool_result(plugin, ev, tr)
    return "我在旁边说一句" in tr.text


async def s4_new_turn_still_tracked(plugin, sid):
    ev1 = batch(FakeMessage("100", "第一轮"))
    await drive_on_llm_request(plugin, ev1)
    await drive_on_llm_response(plugin, ev1, 2, [{"id": "t"}])   # last step, tool call
    await drive_on_tool_result(plugin, ev1)
    await drive_on_tool_result(plugin, ev1)                      # extra boundary
    if hasattr(plugin, "_finished_run"):
        plugin._finished_run.pop(sid, None)                      # what a new turn does
    ev2 = batch(FakeMessage("400", "第二轮"))
    await drive_on_llm_request(plugin, ev2)
    run = plugin._get_active_run(sid)
    return bool(run) and run["event"] is ev2 and not run.get("ending")


async def s5_intermediate_step_is_running(plugin, sid):
    ev = batch(FakeMessage("100", "多步任务"))
    await drive_on_llm_request(plugin, ev)
    await drive_on_llm_response(plugin, ev, 1, [{"id": "t"}])    # step 1 of 2
    await drive_on_tool_result(plugin, ev)
    return plugin._get_active_run(sid) is not None


SCENARIOS = [
    ("S1 live run: batch queued then injected at next boundary",
     lambda p, s, c: s1_live_batch_is_queued_then_injected(p, s)),
    ("S2 live run: stop word stops the run, nothing queued",
     lambda p, s, c: s2_stop_word_on_live_run(p, s)),
    ("S3 live run: buffered message drained into the tool result",
     lambda p, s, c: s3_buffer_message_drained(p, s, c)),
    ("S4 new turn on the same session is tracked again",
     lambda p, s, c: s4_new_turn_still_tracked(p, s)),
    ("S5 intermediate step still counts as running",
     lambda p, s, c: s5_intermediate_step_is_running(p, s)),
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
