"""Regression: a finished run must not be resurrected by later tool boundaries.

Bug (v1.2.8 and earlier)
------------------------
The framework dispatches ON_TOOL_RESULT **once per tool call** of one LLM
response (``core/agent/func_tool_manager.py``: ``for idx, tool_call in
enumerate(resp.tool_calls)``).  When the *last* step of a run issues several
tool calls, the first boundary finishes the run and pops ``_run_active``, but
the following boundaries ran ``_touch_run`` first — which found no active run
and **rebuilt** it with ``ending=False``: a phantom "still running" state whose
heartbeat is frozen at the last tool boundary.

Consequences
------------
* ``on_batch_dedup`` sees ``_get_active_run()`` -> intercepts every new batch
  into the flow queue and stops it, but the run will never call another tool,
  so nothing is ever injected: messages sit there until the heartbeat timeout
  ("LLM timeout + tool timeout", 180s by default) restores them.
* A message matching a stop word is consumed as "stop the running turn" and
  written into the dedup table -> **never delivered at all**.

Run it against any checkout:  python3 tests/test_phantom_run.py [plugin_dir]
"""
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from harness import (DEFAULT_SID, FakeMessage, batch, drive_on_batch_message,  # noqa: E402
                     drive_on_llm_request, drive_on_llm_response,
                     drive_on_tool_result, make_plugin)

# Exactly what the reported log shows: step 2/2 issued three tool calls
# (search_files **/sustained* , **/Sustained* , **/*sustained*).
LAST_STEP_TOOL_CALLS = [{"id": "t1"}, {"id": "t2"}, {"id": "t3"}]

# The four messages the user sent *after* the run had already finished.
FOLLOW_UP = [("1690512137", "慕娅"), ("1413943636", "慕娅"),
             ("2123703599", "停止"), ("1570128268", "慕娅 停止调用工具")]


async def run(plugin_dir=None):
    plugin, _ctx, sid = await make_plugin(plugin_dir)

    # ---- one full turn: 2 steps (max_tool_loop = 2), last step calls 3 tools
    ev = batch(FakeMessage("100", "你是不是可以自己配置？"))
    await drive_on_llm_request(plugin, ev)

    await drive_on_llm_response(plugin, ev, 1, [{"id": "s1"}])
    await drive_on_tool_result(plugin, ev)

    await drive_on_llm_response(plugin, ev, 2, LAST_STEP_TOOL_CALLS)
    for _ in LAST_STEP_TOOL_CALLS:
        await drive_on_tool_result(plugin, ev)

    # ---- the turn is over; the session must not look busy any more
    phantom = plugin._get_active_run(sid) is not None

    # ---- ~18s later the user keeps talking
    intercepted = []
    for mid, text in FOLLOW_UP:
        b = batch(FakeMessage(mid, text))
        await drive_on_batch_message(plugin, b)
        if b.is_stopped:
            intercepted.append(mid)

    pending = len(plugin._pending_inject.get(sid, []))
    consumed = len(plugin._consumed.get(sid, {}))

    return {
        "phantom_run": phantom,
        "intercepted": intercepted,
        "stuck_in_flow_queue": pending,
        "consumed_as_stop": consumed,
    }


def check(result):
    """Return a list of (name, passed, detail)."""
    return [
        ("no phantom 'running' state after the turn ended",
         not result["phantom_run"], "phantom run_active left behind"),
        ("follow-up batches are not intercepted",
         result["intercepted"] == [], f"intercepted: {result['intercepted']}"),
        ("no message stuck in the flow queue",
         result["stuck_in_flow_queue"] == 0,
         f"{result['stuck_in_flow_queue']} message(s) stranded"),
        ("no follow-up message swallowed as a stop word",
         result["consumed_as_stop"] == 0,
         f"{result['consumed_as_stop']} message(s) consumed against a dead run"),
    ]


async def main():
    dirs = sys.argv[1:] or ["."]
    failed = False
    for d in dirs:
        plugin_dir = Path(d).resolve()
        print(f"\n=== {plugin_dir} ===")
        res = await run(plugin_dir)
        for name, ok, detail in check(res):
            print(f"  {'PASS' if ok else 'FAIL'}  {name}"
                  + ("" if ok else f"   ({detail})"))
            failed = failed or not ok
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
