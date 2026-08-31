"""随时插话（Midflight Inbox）

核心哲学：零拦截、自然流入。
- 不改变任何消息的处理策略，不打断聊天插件的防抖/合并；
- 只是在 bot 连续执行的每个工具调用边界（ON_TOOL_RESULT），把该会话
  SessionBuffer 里"恰好已到达、还没来得及成为下一轮"的消息，以与官方批次
  完全一致的原生样式追加进 tool_result.text，让下一步 LLM 直接读到；
- 被过滤掉的消息会原样放回 buffer，照常走聊天插件开新轮，没有消息会被卡住；
- 已消费 message_id 去重（TTL 10 分钟），若已注入消息又被 flush 成新批次，
  在 ON_IM_BATCH_MESSAGE(HIGH) 掐掉完全重复的批次，防止二次回复。

仅使用官方插件 API，不修改 KiraAI 本体。
"""

import fnmatch
import re
import time

from core.plugin import BasePlugin, logger, on, Priority
from core.chat.message_utils import KiraMessageEvent, KiraMessageBatchEvent
from core.chat.message_elements import (
    Text, At, Reply, Poke,
    Record, Image, Sticker, File, Video, Forward,
)


# 已消费 message_id 的存活时间（秒）
DEDUP_TTL = 600
# 自动读取官方配置失败时的保守内置兜底
FALLBACK_MAX_INJECT = 5
FALLBACK_FRESHNESS = 30
# 唤醒词回退链：按序尝试读取已安装聊天插件的唤醒词配置
# （plugin_id 候选, 可能的配置键）
WAKE_KEYWORD_SOURCES = [
    (("z-chat", "z_chat", "zchat", "z_chat_plugin"),
     ("waking_words", "wake_keywords", "wake_words")),
    (("s-chat", "s_chat", "schat", "sustained_chat", "sustained-chat"),
     ("waking_words", "wake_keywords", "wake_words")),
    (("default-chat",),
     ("waking_words", "wake_keywords", "wake_words")),
]

# 可被 overrides 覆盖的键
OVERRIDABLE_KEYS = {
    "enabled", "flow_method_group", "flow_method_dm", "accept_poke",
    "wake_keywords", "stop_enabled", "stop_words", "stop_match_mode",
    "whitelist_enabled", "whitelist_users", "max_inject_per_run",
    "freshness_seconds", "max_length", "block_patterns", "template",
    "inject_timeout_steps", "debug",
}


class MidflightMessagePlugin(BasePlugin):
    """随时插话插件主类"""

    def __init__(self, ctx, cfg: dict):
        super().__init__(ctx, cfg)
        cfg = cfg or {}

        basic = cfg.get("section_basic", {}) or {}
        self.enabled = bool(basic.get("enabled", True))
        self.inject_timeout_steps = self._to_int(basic.get("inject_timeout_steps", 2), 2)
        self.debug = bool(basic.get("debug", False))

        flow = cfg.get("section_flow", {}) or {}
        self.flow_method_group = str(flow.get("flow_method_group", "all") or "all")
        self.flow_method_dm = str(flow.get("flow_method_dm", "any") or "any")
        self.accept_poke = bool(flow.get("accept_poke", True))
        self.wake_keywords = [str(w) for w in (flow.get("wake_keywords") or []) if str(w).strip()]

        stop = cfg.get("section_stop", {}) or {}
        self.stop_enabled = bool(stop.get("stop_enabled", False))
        self.stop_words = [str(w) for w in (stop.get("stop_words") or []) if str(w)]
        self.stop_match_mode = str(stop.get("stop_match_mode", "contains") or "contains")

        scope = cfg.get("section_scope", {}) or {}
        self.session_blacklist = [str(s) for s in (scope.get("session_blacklist") or []) if str(s).strip()]
        self.whitelist_enabled = bool(scope.get("whitelist_enabled", False))
        self.whitelist_users = [str(u) for u in (scope.get("whitelist_users") or []) if str(u).strip()]

        limits = cfg.get("section_limits", {}) or {}
        self.max_inject_per_run = self._to_int(limits.get("max_inject_per_run", 0), 0)
        self.freshness_seconds = self._to_int(limits.get("freshness_seconds", 0), 0)
        self.max_length = self._to_int(limits.get("max_length", 0), 0)
        self.block_patterns = [str(p) for p in (limits.get("block_patterns") or []) if str(p).strip()]

        inject = cfg.get("section_inject", {}) or {}
        self.template = str(inject.get("template", "") or "")

        media = cfg.get("section_media", {}) or {}
        # 媒体流入开关：语音/图片/文件/合并转发，默认全部允许
        self.allow_record = bool(media.get("allow_record", True))
        self.allow_image = bool(media.get("allow_image", True))
        self.allow_file = bool(media.get("allow_file", True))
        self.allow_forward = bool(media.get("allow_forward", True))

        overrides = cfg.get("section_overrides", {}) or {}
        raw_overrides = overrides.get("overrides", {})
        self.overrides = raw_overrides if isinstance(raw_overrides, dict) else {}

        # ---- 运行时状态（terminate 全部清理）----
        # {sid: {message_id: consumed_ts}}
        self._consumed: dict[str, dict[str, float]] = {}
        # {event_id(一轮 agent 执行): 已注入条数}
        self._run_inject_count: dict[str, int] = {}
        # {sid: 有候选消息但未被消费的连续工具边界数}
        self._wait_steps: dict[str, int] = {}
        # 上次清理时间
        self._last_gc: float = 0.0
        # 自动解析后的生效值（initialize 中解析）
        self._eff_max_inject: int = 0
        self._eff_freshness: int = 0

    # ============ 生命周期 ============

    async def initialize(self):
        """解析自动配置项并记日志；可重入。"""
        # 单轮最多流入条数：0 = 自动读 bot_config.bot.max_buffer_messages
        if self.max_inject_per_run <= 0:
            auto = self._read_core_config("bot_config.bot.max_buffer_messages")
            self._eff_max_inject = self._to_int(auto, FALLBACK_MAX_INJECT)
            if self._eff_max_inject <= 0:
                self._eff_max_inject = FALLBACK_MAX_INJECT
            self._log_debug(f"max_inject_per_run 自动读取: {self._eff_max_inject}")
        else:
            self._eff_max_inject = self.max_inject_per_run

        # 新鲜度：-1 = 不限（默认；buffer 里的消息本来都是本轮执行期间到达的，
        # 配合 S版/Z版 等带合并防抖的聊天插件时，消息常已在队列里等了十几秒，
        # 限时会导致永远赶不上工具边界）；0 = 自动读 bot_config.bot.max_message_interval；
        # >0 = 自定义秒数
        if self.freshness_seconds < 0:
            self._eff_freshness = -1
            self._log_debug("freshness_seconds = -1，不限流入时间")
        elif self.freshness_seconds == 0:
            auto = self._read_core_config("bot_config.bot.max_message_interval")
            self._eff_freshness = self._to_int(auto, FALLBACK_FRESHNESS)
            if self._eff_freshness <= 0:
                self._eff_freshness = FALLBACK_FRESHNESS
            self._log_debug(f"freshness_seconds 自动读取: {self._eff_freshness}")
        else:
            self._eff_freshness = self.freshness_seconds

        # 唤醒词：留空 = 按 Z版 → S版 → 官方 default-chat 顺序自动沿用
        if not self.wake_keywords:
            words, source = self._resolve_wake_keywords()
            if words:
                self.wake_keywords = words
                logger.info(f"[Midflight] 唤醒词自动沿用 {source}: {self.wake_keywords}")
            else:
                self._log_debug("未能从任何聊天插件读取唤醒词，keyword 流入通道不生效")

        logger.info(
            f"[Midflight] 消息流入插件已加载 | enabled={self.enabled} "
            f"群聊={self.flow_method_group} 私聊={self.flow_method_dm} "
            f"poke={self.accept_poke} 停止词={'开' if self.stop_enabled else '关(默认)'} "
            f"上限={self._eff_max_inject} 新鲜度={self._eff_freshness}s"
        )

    async def terminate(self):
        """可重入：清理全部运行时状态。"""
        try:
            self._consumed.clear()
            self._run_inject_count.clear()
            self._wait_steps.clear()
            self._last_gc = 0.0
        except Exception:
            pass
        logger.info("[Midflight] 消息流入插件已卸载")

    # ============ 注入通道：工具结果边界 ============

    @on.tool_result(priority=Priority.MEDIUM)
    async def on_tool_result(self, event: KiraMessageBatchEvent, tool_result, *_):
        """每个工具调用结果返回后触发：drain 该 sid 的 buffer，过滤后注入/停止。"""
        try:
            await self._handle_tool_result(event, tool_result)
        except Exception:
            logger.exception("[Midflight] on_tool_result 处理异常（已自捕获，不影响主流程）")

    async def _handle_tool_result(self, event: KiraMessageBatchEvent, tool_result):
        cfg = self._eff_config(None)
        if not cfg["enabled"]:
            return

        sid = getattr(event, "sid", None)
        if not sid:
            session = getattr(event, "session", None)
            sid = getattr(session, "sid", None)
        if not sid:
            return

        cfg = self._eff_config(sid)
        if not cfg["enabled"]:
            return
        if self._in_blacklist(sid):
            self._log_debug(f"{sid} 在会话黑名单中，跳过")
            return

        buffer = self.ctx.get_buffer(sid)
        if buffer is None or buffer.get_length() == 0:
            self._wait_steps.pop(sid, None)
            return

        # 候选等待步数上限：超过则完全不动 buffer，留给聊天插件正常开新轮
        timeout = cfg["inject_timeout_steps"]
        waited = self._wait_steps.get(sid, 0)
        if timeout > 0 and waited >= timeout:
            self._log_debug(f"{sid} 候选消息已等待 {waited} 个边界（上限 {timeout}），不再消费")
            return

        self._gc()

        # 在 buffer.lock 内 drain，与官方 flush_session_messages 同一互斥域
        async with buffer.lock:
            pending = buffer.flush()

        if not pending:
            return

        consumed_map = self._consumed.setdefault(sid, {})
        stop_hit = []   # 命中停止词（消费，不注入）
        injectable = []  # 通过全部过滤（消费，注入）
        rejected = []    # 未通过过滤（放回 buffer，走官方管线）
        now = time.time()

        for msg_event in pending:
            try:
                mid = str(getattr(getattr(msg_event, "message", None), "message_id", "") or "")
                if mid and mid in consumed_map:
                    # 已被消费过（注入过），直接丢弃，不放回，防重复
                    self._log_debug(f"{sid} 消息 {mid} 已消费，丢弃防重")
                    continue
                text = self._plain_text(msg_event)
                if cfg["stop_enabled"] and self._match_stop(text, cfg):
                    stop_hit.append(msg_event)
                    continue
                if self._pass_filters(msg_event, text, cfg, now):
                    injectable.append((msg_event, text))
                else:
                    rejected.append(msg_event)
            except Exception:
                # 单条识别失败：放回 buffer，绝不丢消息
                rejected.append(msg_event)
                logger.exception("[Midflight] 单条消息过滤异常，已放回原流程")

        # 单轮流入条数上限：超额部分放回 buffer 留给聊天插件开新轮
        run_id = getattr(event, "event_id", None) or sid
        used = self._run_inject_count.get(run_id, 0)
        quota = max(0, cfg["_max_inject"] - used)
        overflow = injectable[quota:]
        injectable = injectable[:quota]

        # 放回未消费的消息（保持原顺序置于 buffer 头部）
        put_back = rejected + [m for m, _ in overflow]
        if put_back:
            async with buffer.lock:
                buffer.buffer[:0] = put_back

        # 停止与注入互斥：命中停止词只 stop 不注入；
        # 同批已通过过滤的消息放回 buffer 走官方管线，绝不丢消息
        if stop_hit:
            leftover = [m for m, _ in injectable]
            if leftover:
                async with buffer.lock:
                    buffer.buffer[:0] = leftover
            for m in stop_hit:
                mid = str(getattr(getattr(m, "message", None), "message_id", "") or "")
                if mid:
                    consumed_map[mid] = now
            self._wait_steps.pop(sid, None)
            logger.info(f"[Midflight] {sid} 命中停止词，停止本轮后续步骤")
            event.stop()
            return

        if not injectable:
            # 有候选但都没消费：累计等待步数
            self._wait_steps[sid] = waited + 1
            return

        # 原生样式文本化（与官方批次 message_format_to_text 一致）
        lines = []
        n = 0
        for msg_event, text in injectable:
            n += 1
            try:
                chain = getattr(getattr(msg_event, "message", None), "chain", None)
                native = await self.ctx.message_processor.message_format_to_text(chain) if chain else text
            except Exception:
                native = text
            native = native or text
            if cfg["max_length"] > 0 and len(native) > cfg["max_length"]:
                native = native[: cfg["max_length"]] + "…"
            if cfg["template"]:
                lines.append(self._render_template(cfg["template"], msg_event, native, n))
            else:
                # 默认：原样流入，不改写/包装
                lines.append(native)

        inject_block = "\n".join(line for line in lines if line)
        if not inject_block.strip():
            self._wait_steps[sid] = waited + 1
            return

        for m, _ in injectable:
            mid = str(getattr(getattr(m, "message", None), "message_id", "") or "")
            if mid:
                consumed_map[mid] = now
        self._run_inject_count[run_id] = used + len(injectable)
        self._wait_steps.pop(sid, None)

        base = getattr(tool_result, "text", "") or ""
        tool_result.text = (base + "\n" if base else "") + inject_block

        # 媒体附件透传：native 多模态模式下官方文本化只产出 "[Image attached]"，
        # 图片字节不会随搭车文本进请求。把消息里的 Image/Record/File 元素挂到
        # ToolResult.attachments（官方支持，tool.py:60），assemble_result 会自动
        # 落盘并把可访问路径写进工具结果，bot 后续可直接读取原图/原文件。
        # 与 S版/Z版 的并行媒体识别兼容：它们已替换为占位/描述文本的链里不再含
        # 原始媒体元素，此处自然为空，互不影响。
        try:
            attachments = getattr(tool_result, "attachments", None)
            if isinstance(attachments, list):
                for msg_event, _ in injectable:
                    chain = getattr(getattr(msg_event, "message", None), "chain", None) or []
                    for ele in chain:
                        if isinstance(ele, (Image, Record, File)) and hasattr(ele, "to_path"):
                            attachments.append(ele)
        except Exception:
            self._log_debug("媒体附件透传失败（已忽略，不影响文本流入）")

        logger.info(f"[Midflight] {sid} 流入 {len(injectable)} 条消息到当前轮")

    # ============ 去重兜底：掐掉完全重复的批次 ============

    @on.llm_response()
    async def _ensure_stop_checkpoint(self, event, *_):
        """空 handler，有意为之：agent_executor 的 is_stopped 停止检查位于
        ON_LLM_RESPONSE handler 循环体内（agent_executor.py:149-164）。
        若环境中没有任何插件注册该事件，循环体不执行，event.stop() 将无法
        阻止后续 LLM 步。注册此空 handler 保证停止检查点必然被执行。"""
        return

    @on.im_batch_message(priority=Priority.HIGH)
    async def on_batch_dedup(self, event: KiraMessageBatchEvent, *_):
        """若批次内全部消息都已被本插件消费过，则掐掉该批次，避免重复开轮。"""
        try:
            if not self.enabled:
                return
            sid = getattr(event, "sid", None)
            if not sid:
                return
            consumed_map = self._consumed.get(sid)
            if not consumed_map:
                return
            messages = getattr(event, "messages", None) or []
            ids = [str(getattr(m, "message_id", "") or "") for m in messages]
            if ids and all(mid and mid in consumed_map for mid in ids):
                logger.info(f"[Midflight] {sid} 批次 {getattr(event, 'event_id', '')} 全部为已消费消息，掐掉重复轮")
                event.stop()
        except Exception:
            logger.exception("[Midflight] on_batch_dedup 异常（已自捕获）")

    # ============ 过滤器 ============

    def _pass_filters(self, msg_event, text: str, cfg: dict, now: float) -> bool:
        message = getattr(msg_event, "message", None)
        if message is None:
            return False

        # 白名单（默认关）
        if cfg["whitelist_enabled"]:
            sender_id = str(getattr(getattr(message, "sender", None), "user_id", "") or "")
            if sender_id not in cfg["whitelist_users"]:
                return False

        # 新鲜度
        ts = getattr(message, "timestamp", 0) or 0
        try:
            if cfg["_freshness"] > 0 and now - float(ts) > cfg["_freshness"]:
                self._log_debug(f"消息 {getattr(message, 'message_id', '')} 超出新鲜度窗口，跳过")
                return False
        except Exception:
            pass

        # 内容正则黑名单
        for pat in cfg["block_patterns"]:
            try:
                if re.search(pat, text):
                    return False
            except re.error:
                continue

        # 媒体类型开关：含被禁用类型媒体的消息不流入（放回 buffer 走官方管线）。
        # 媒体内容本身由官方 message_format_to_text 统一转换（语音→STT 文字、
        # 图片/表情→VLM 描述+落盘路径、文件/视频≤10MB→落盘路径、转发→递归展开），
        # 流入后 bot 不仅能看到描述，还能用 file_read 类工具读取原文件。
        if not self._media_allowed(msg_event, cfg):
            return False

        # 流入方式
        return self._match_flow_method(msg_event, text, cfg)

    def _match_flow_method(self, msg_event, text: str, cfg: dict) -> bool:
        try:
            is_group = bool(msg_event.is_group_message())
        except Exception:
            is_group = getattr(getattr(msg_event, "message", None), "group", None) is not None
        method = cfg["flow_method_group"] if is_group else cfg["flow_method_dm"]

        if method == "any":
            return True

        message = getattr(msg_event, "message", None)
        chain = getattr(message, "chain", None) or []
        self_id = str(getattr(message, "self_id", "") or "")

        hit_at = False
        hit_reply = False
        for ele in chain:
            if isinstance(ele, At) and self_id and ele.pid == self_id:
                hit_at = True
            elif isinstance(ele, Reply):
                hit_reply = True
        # 部分适配器只在 is_mentioned 上体现 @
        if not hit_at and getattr(message, "is_mentioned", None) is True and not cfg["wake_keywords"]:
            hit_at = True

        hit_keyword = bool(cfg["wake_keywords"]) and any(w in text for w in cfg["wake_keywords"])
        hit_poke = cfg["accept_poke"] and self._is_poke(msg_event)

        if method == "at":
            return hit_at
        if method == "reply":
            return hit_reply
        if method == "keyword":
            return hit_keyword
        # all：@ / 回复 / 唤醒词 / 戳一戳 任一即可
        return hit_at or hit_reply or hit_keyword or hit_poke

    def _media_allowed(self, msg_event, cfg: dict) -> bool:
        """检查消息链中的媒体元素是否被对应开关允许（防御式，异常视为允许）。"""
        try:
            message = getattr(msg_event, "message", None)
            chain = getattr(message, "chain", None) or []
            for ele in chain:
                if isinstance(ele, Record) and not cfg["allow_record"]:
                    return False
                if isinstance(ele, (Image, Sticker)) and not cfg["allow_image"]:
                    return False
                if isinstance(ele, (File, Video)) and not cfg["allow_file"]:
                    return False
                if isinstance(ele, Forward) and not cfg["allow_forward"]:
                    return False
            return True
        except Exception:
            return True

    def _is_poke(self, msg_event) -> bool:
        """戳一戳识别（防御式，识别不了就跳过不报错）。"""
        try:
            message = getattr(msg_event, "message", None)
            if message is None:
                return False
            chain = getattr(message, "chain", None) or []
            for ele in chain:
                if isinstance(ele, Poke):
                    return True
            # OneBot 风格 notice：notify/poke
            if getattr(message, "is_notice", False):
                raw = getattr(message, "raw_message", None)
                if isinstance(raw, dict):
                    if raw.get("sub_type") == "poke":
                        return True
                    if raw.get("notice_type") == "notify" and "poke" in str(raw.get("sub_type", "")):
                        return True
        except Exception:
            pass
        return False

    def _match_stop(self, text: str, cfg: dict) -> bool:
        if not text or not cfg["stop_words"]:
            return False
        mode = cfg["stop_match_mode"]
        for w in cfg["stop_words"]:
            try:
                if mode == "exact" and text.strip() == w:
                    return True
                if mode == "regex" and re.search(w, text):
                    return True
                if mode == "contains" and w in text:
                    return True
            except re.error:
                continue
        return False

    # ============ 工具方法 ============

    def _eff_config(self, sid: str | None) -> dict:
        """全局配置 + 会话级 overrides（支持通配）合成生效配置。"""
        cfg = {
            "enabled": self.enabled,
            "flow_method_group": self.flow_method_group,
            "flow_method_dm": self.flow_method_dm,
            "accept_poke": self.accept_poke,
            "wake_keywords": self.wake_keywords,
            "stop_enabled": self.stop_enabled,
            "stop_words": self.stop_words,
            "stop_match_mode": self.stop_match_mode,
            "whitelist_enabled": self.whitelist_enabled,
            "whitelist_users": self.whitelist_users,
            "max_inject_per_run": self.max_inject_per_run,
            "freshness_seconds": self.freshness_seconds,
            "max_length": self.max_length,
            "block_patterns": self.block_patterns,
            "template": self.template,
            "inject_timeout_steps": self.inject_timeout_steps,
            "debug": self.debug,
            "allow_record": self.allow_record,
            "allow_image": self.allow_image,
            "allow_file": self.allow_file,
            "allow_forward": self.allow_forward,
            "_max_inject": self._eff_max_inject,
            "_freshness": self._eff_freshness,
        }
        if sid and self.overrides:
            for pattern, ov in self.overrides.items():
                if not isinstance(ov, dict):
                    continue
                try:
                    if fnmatch.fnmatchcase(sid, str(pattern)):
                        for k, v in ov.items():
                            if k in OVERRIDABLE_KEYS:
                                cfg[k] = v
                        # 覆盖后重算自动项
                        if self._to_int(cfg["max_inject_per_run"], 0) > 0:
                            cfg["_max_inject"] = self._to_int(cfg["max_inject_per_run"], 0)
                        ov_fresh = self._to_int(cfg["freshness_seconds"], 0)
                        if ov_fresh != 0:
                            cfg["_freshness"] = ov_fresh  # -1=不限，>0=自定义秒数
                except Exception:
                    continue
        cfg["_max_inject"] = max(1, self._to_int(cfg.get("_max_inject"), FALLBACK_MAX_INJECT))
        cfg["_freshness"] = max(0, self._to_int(cfg.get("_freshness"), FALLBACK_FRESHNESS))
        if not isinstance(cfg.get("wake_keywords"), list):
            cfg["wake_keywords"] = []
        if not isinstance(cfg.get("stop_words"), list):
            cfg["stop_words"] = []
        if not isinstance(cfg.get("whitelist_users"), list):
            cfg["whitelist_users"] = []
        if not isinstance(cfg.get("block_patterns"), list):
            cfg["block_patterns"] = []
        cfg["whitelist_users"] = [str(u) for u in cfg["whitelist_users"]]
        return cfg

    def _in_blacklist(self, sid: str) -> bool:
        for pattern in self.session_blacklist:
            try:
                if fnmatch.fnmatchcase(sid, pattern):
                    return True
            except Exception:
                continue
        return False

    def _plain_text(self, msg_event) -> str:
        """提取纯文本（仅 Text 元素），防御式取值。"""
        try:
            chain = getattr(getattr(msg_event, "message", None), "chain", None) or []
            return "".join(ele.text for ele in chain if isinstance(ele, Text))
        except Exception:
            return ""

    def _render_template(self, template: str, msg_event, text: str, n: int) -> str:
        message = getattr(msg_event, "message", None)
        nickname = str(getattr(getattr(message, "sender", None), "nickname", "") or "")
        ts = getattr(message, "timestamp", 0) or 0
        try:
            time_str = time.strftime("%H:%M:%S", time.localtime(float(ts)))
        except Exception:
            time_str = ""
        try:
            return template.format(
                sender_nickname=nickname, text=text, time=time_str, n=n)
        except Exception:
            return text

    def _read_core_config(self, key: str):
        try:
            return self.ctx.config.get_config(key)
        except Exception:
            return None

    def _resolve_wake_keywords(self) -> tuple[list, str]:
        """按 Z版 → S版 → 官方 default-chat 顺序尝试读取已安装聊天插件的唤醒词。"""
        for id_candidates, key_candidates in WAKE_KEYWORD_SOURCES:
            for pid in id_candidates:
                cfg = self._get_other_plugin_cfg(pid)
                if not cfg:
                    continue
                words = self._find_keywords_in_cfg(cfg, key_candidates)
                if words:
                    return words, pid
        # 兜底：遍历已安装插件，找 id 以 chat 结尾且有唤醒词配置的
        try:
            mgr = self.ctx.plugin_mgr
            if mgr is not None:
                for info in mgr.list_plugins():
                    pid = getattr(info, "plugin_id", "") or ""
                    if pid == "midflight_message_plugin" or "chat" not in pid:
                        continue
                    cfg = self._get_other_plugin_cfg(pid)
                    words = self._find_keywords_in_cfg(
                        cfg, ("waking_words", "wake_keywords", "wake_words")) if cfg else []
                    if words:
                        return words, pid
        except Exception:
            pass
        return [], ""

    def _get_other_plugin_cfg(self, plugin_id: str) -> dict:
        """读取其他插件配置：优先实例 plugin_cfg，回退配置文件。全部失败返回 {}。"""
        try:
            inst = self.ctx.get_plugin_inst(plugin_id)
            if inst is not None:
                cfg = getattr(inst, "plugin_cfg", None)
                if isinstance(cfg, dict) and cfg:
                    return cfg
        except Exception:
            pass
        try:
            mgr = self.ctx.plugin_mgr
            if mgr is not None:
                cfg = mgr.get_plugin_config(plugin_id)
                if isinstance(cfg, dict):
                    return cfg
        except Exception:
            pass
        return {}

    def _find_keywords_in_cfg(self, cfg: dict, keys) -> list:
        """在插件配置中查找唤醒词列表（兼容平铺与 section 嵌套）。"""
        for key in keys:
            val = cfg.get(key)
            words = self._as_str_list(val)
            if words:
                return words
        # section 嵌套
        for val in cfg.values():
            if isinstance(val, dict):
                for key in keys:
                    words = self._as_str_list(val.get(key))
                    if words:
                        return words
        return []

    @staticmethod
    def _as_str_list(val) -> list:
        if isinstance(val, (list, tuple)):
            return [str(w) for w in val if str(w).strip()]
        return []

    def _gc(self):
        """定期清理过期的去重记录与计数。"""
        now = time.time()
        if now - self._last_gc < 60:
            return
        self._last_gc = now
        cutoff = now - DEDUP_TTL
        for sid in list(self._consumed.keys()):
            m = {k: v for k, v in self._consumed[sid].items() if v > cutoff}
            if m:
                self._consumed[sid] = m
            else:
                self._consumed.pop(sid, None)
        # 流入计数随去重窗口一起过期没有意义，直接限长
        if len(self._run_inject_count) > 200:
            self._run_inject_count.clear()
        if len(self._wait_steps) > 200:
            self._wait_steps.clear()

    def _log_debug(self, msg: str):
        if self.debug:
            logger.info(f"[Midflight][debug] {msg}")
        else:
            logger.debug(f"[Midflight] {msg}")

    @staticmethod
    def _to_int(val, default: int) -> int:
        try:
            return int(val)
        except (TypeError, ValueError):
            return default
