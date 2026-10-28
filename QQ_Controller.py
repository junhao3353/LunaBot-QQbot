#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
QQ群远程Docker控制器（反向WebSocket模式）
- 控制器作为WS服务端监听端口，NapCat主动反向连接上来
- 避免NapCat正向WS只支持一个客户端连接的限制（qq-bot已占6700）
- 白名单QQ才能发指令
- 指令：关闭服务 / 重启服务 / 重启服务 ai / 重启服务 napcat
- 通过docker.sock控制宿主机容器
- 配置从 qqbot-config.yml 的 controller 段读取，环境变量可覆盖
"""
import asyncio
import hmac
import json
import os
import re
import secrets
import shutil
import sys
import threading
import time
import urllib.parse
import urllib.request
from datetime import datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import websockets
import docker
import yaml

# Windows 控制台默认 GBK，打印 emoji 会 UnicodeEncodeError 直接崩掉；容器内是 UTF-8 不受影响
for _stream in ("stdout", "stderr"):
    try:
        getattr(sys, _stream).reconfigure(errors="replace")
    except Exception:
        pass

RECONNECT_DELAY = 5
CONFIG_PATH = os.environ.get("CTRL_CONFIG_PATH", "/app/qqbot-config.yml")
WS_HOST = os.environ.get("CTRL_WS_HOST", "0.0.0.0")
WS_PORT = int(os.environ.get("CTRL_WS_PORT", "6701"))

# ---- WebUI（配置界面，先只做仪表盘） ----
WEBUI_HOST = os.environ.get("CTRL_WEBUI_HOST", "0.0.0.0")
WEBUI_PORT = int(os.environ.get("CTRL_WEBUI_PORT", "6650"))
WEBUI_TOKEN = os.environ.get("CTRL_WEBUI_TOKEN", "").strip()   # 留空=不校验（仅内网用）
STATS_FILE = os.environ.get("CTRL_STATS_FILE", "/data/stats.json")  # 主程序写入的统计数据
TOKEN_FILE = os.environ.get("CTRL_TOKEN_FILE", "/app/webui_token.txt")  # 令牌文件（compose 里 bind mount 到项目目录，方便查看）
BALANCE_HISTORY_FILE = os.environ.get("CTRL_BALANCE_HISTORY_FILE", "/data/balance_history.json")  # 余额历史记录
WEBUI_NO_NET = os.environ.get("CTRL_WEBUI_NO_NET", "") == "1"  # 测试用：不查外网余额

# 待发消息队列：重启NapCat后WS断开，确认消息等重连成功后补发
_pending_msgs = []
# 当前活动的WS连接（NapCat反向连接上来的）
_active_ws = None
# 全局配置（main里加载，handle_message里用）
_cfg = None


def log(msg):
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}", flush=True)


# ===================== 配置加载（yml > 环境变量 > 默认） =====================
def load_config():
    cfg = {}
    if os.path.exists(CONFIG_PATH):
        try:
            with open(CONFIG_PATH, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            ctrl = data.get("controller", {})
            if isinstance(ctrl, dict) and ctrl:
                cfg.update(ctrl)
                log(f"📄 已从配置文件加载控制器设置：{CONFIG_PATH}")
        except Exception as e:
            log(f"⚠️ 读取配置文件失败: {e}")
    else:
        log(f"ℹ️ 未找到配置文件 {CONFIG_PATH}，使用环境变量/默认值")

    if os.environ.get("CTRL_WS_PORT"):
        cfg["ws_port"] = int(os.environ["CTRL_WS_PORT"])
    if os.environ.get("CTRL_ALLOWED_GROUPS"):
        cfg["allowed_groups"] = [int(x) for x in os.environ["CTRL_ALLOWED_GROUPS"].split(",") if x.strip()]
    if os.environ.get("CTRL_ALLOWED_USERS"):
        cfg["allowed_users"] = [int(x) for x in os.environ["CTRL_ALLOWED_USERS"].split(",") if x.strip()]
    if os.environ.get("CTRL_CONTAINER_AI"):
        cfg["container_ai"] = os.environ["CTRL_CONTAINER_AI"]
    if os.environ.get("CTRL_CONTAINER_NAPCAT"):
        cfg["container_napcat"] = os.environ["CTRL_CONTAINER_NAPCAT"]
    if os.environ.get("CTRL_CONTAINER_SELF"):
        cfg["container_self"] = os.environ["CTRL_CONTAINER_SELF"]
    if os.environ.get("CTRL_WEBUI_PORT"):
        cfg["webui_port"] = int(os.environ["CTRL_WEBUI_PORT"])
    if os.environ.get("CTRL_WEBUI_TOKEN"):
        cfg["webui_token"] = os.environ["CTRL_WEBUI_TOKEN"].strip()
    if os.environ.get("CTRL_STATS_FILE"):
        cfg["stats_file"] = os.environ["CTRL_STATS_FILE"]

    cfg.setdefault("ws_port", WS_PORT)
    cfg.setdefault("allowed_groups", [996930225])
    cfg.setdefault("allowed_users", [3353936945])
    cfg.setdefault("container_ai", "qq-bot")
    cfg.setdefault("container_napcat", "qq-napcat")
    cfg.setdefault("container_self", "qq-controller")
    # WebUI（配置界面）
    cfg.setdefault("webui_enabled", True)
    cfg.setdefault("webui_port", WEBUI_PORT)
    cfg.setdefault("webui_token", WEBUI_TOKEN)
    cfg.setdefault("stats_file", STATS_FILE)
    return cfg


# ===================== Docker操作 =====================
_docker_client = None


def get_docker_client():
    global _docker_client
    if _docker_client is None:
        _docker_client = docker.from_env()
    return _docker_client


def docker_restart(name):
    """异步重启容器，立即返回，后台执行实际重启（避免HTTP阻塞）"""
    def _do_restart():
        try:
            c = get_docker_client().containers.get(name)
            c.restart(timeout=10)
            log(f"✅ 已重启容器: {name}")
        except Exception as e:
            log(f"❌ 重启容器 {name} 失败: {e}")
    import threading
    threading.Thread(target=_do_restart, daemon=True).start()
    return True, "重启指令已发送"


def docker_stop(name):
    try:
        c = get_docker_client().containers.get(name)
        c.stop(timeout=15)
        log(f"✅ 已停止容器: {name}")
        return True, "已停止"
    except Exception as e:
        log(f"❌ 停止容器 {name} 失败: {e}")
        return False, f"失败: {e}"


# ===================== 待发消息队列 =====================
def enqueue_pending_msg(group_id, text):
    _pending_msgs.append((group_id, text))


async def flush_pending_msgs(ws):
    if not _pending_msgs:
        return
    log(f"📤 补发 {len(_pending_msgs)} 条待发消息...")
    while _pending_msgs:
        gid, text = _pending_msgs.pop(0)
        await send_group_msg(ws, gid, text)
        await asyncio.sleep(0.3)


# ===================== OneBot消息发送 =====================
async def send_group_msg(ws, group_id, text):
    payload = {
        "action": "send_group_msg",
        "params": {"group_id": group_id, "message": text},
        "echo": f"ctrl_{int(time.time())}"
    }
    try:
        await ws.send(json.dumps(payload, ensure_ascii=False))
    except Exception as e:
        log(f"⚠️ 发消息失败: {e}")


# ===================== 指令解析 =====================
def parse_command(text):
    """只认四个指令：关闭服务 / 重启服务 / 重启服务 ai / 重启服务 napcat"""
    text = text.strip().lower()
    text = re.sub(r'^[的\s]+', '', text)
    if text == "关闭服务":
        return "shutdown", "all"
    if text == "重启服务":
        return "restart", "all"
    m = re.match(r'^重启服务\s*(ai|napcat)$', text)
    if m:
        return "restart", m.group(1)
    return None


# ===================== 执行指令 =====================
async def execute_command(ws, group_id, cmd_type, target):
    global _cfg
    container_ai = _cfg["container_ai"]
    container_napcat = _cfg["container_napcat"]
    container_self = _cfg["container_self"]

    if cmd_type == "shutdown":
        await send_group_msg(ws, group_id, "🔴 收到关闭指令，正在关闭所有服务...")
        await asyncio.sleep(0.5)
        docker_stop(container_ai)
        await asyncio.sleep(1)
        docker_stop(container_napcat)
        await asyncio.sleep(1)
        await send_group_msg(ws, group_id, "🔴 AI和NapCat已关闭。控制器即将退出，下次使用请手动启动 qq-controller。")
        await asyncio.sleep(1)
        docker_stop(container_self)
        return

    if cmd_type == "restart":
        if target == "all":
            await send_group_msg(ws, group_id, "🔄 收到重启指令，先重启AI，再重启NapCat...")
            await asyncio.sleep(0.5)
            ok1, msg1 = docker_restart(container_ai)
            await send_group_msg(ws, group_id, f"{'✅' if ok1 else '❌'} AI {msg1}")
            await asyncio.sleep(2)
            await send_group_msg(ws, group_id, "🔄 正在重启 NapCat...")
            ok2, msg2 = docker_restart(container_napcat)
            enqueue_pending_msg(group_id, f"{'✅' if ok2 else '❌'} NapCat {msg2}")
            enqueue_pending_msg(group_id, "🔄 全部重启完成")
        elif target == "napcat":
            await send_group_msg(ws, group_id, "🔄 正在重启 NapCat...")
            ok, msg = docker_restart(container_napcat)
            enqueue_pending_msg(group_id, f"{'✅' if ok else '❌'} NapCat {msg}")
        elif target == "ai":
            await send_group_msg(ws, group_id, "🔄 正在重启 AI 机器人...")
            ok, msg = docker_restart(container_ai)
            await send_group_msg(ws, group_id, f"{'✅' if ok else '❌'} AI {msg}")
        return


# ===================== 消息处理 =====================
async def handle_message(ws, raw):
    global _cfg
    try:
        data = json.loads(raw)
    except Exception:
        return

    # 忽略API响应（有echo且既无post_type也无message_type）
    if "echo" in data and "post_type" not in data and "message_type" not in data:
        return
    # 消息事件判断：有post_type=message，或有message_type字段（NapCat反向WS可能省略post_type）
    is_message = (data.get("post_type") == "message") or ("message_type" in data)
    if not is_message:
        return
    if data.get("message_type") != "group":
        return

    group_id = data.get("group_id")
    user_id = data.get("user_id")
    raw_msg = data.get("raw_message", "")
    self_id = data.get("self_id")

    allowed_groups = _cfg["allowed_groups"]
    allowed_users = _cfg["allowed_users"]

    if group_id not in allowed_groups:
        return
    if user_id not in allowed_users:
        return

    # 提取去掉CQ码后的纯文字（@判断要用）
    text = re.sub(r"\[CQ:[^]]*\]", "", raw_msg).strip()

    # 必须@了控制器（和AI共用同一个QQ号，@DeepSeek即可；兼容CQ码/@QQ号/@昵称三种格式）
    at_flag = (f"[CQ:at,qq={self_id}]" in raw_msg
               or f"@{self_id}" in raw_msg
               or text.startswith("@"))
    if not at_flag:
        return

    if not text:
        # 只@没说具体指令，控制器不插嘴，让AI正常回复
        return

    log(f"📩 群{group_id} | 用户{user_id}：{text}")

    # 去掉开头的 @昵称 前缀（@DeepSeek 重启服务 napcat → 重启服务 napcat）
    if text.startswith("@"):
        idx = text.find(" ")
        if idx > 0:
            text = text[idx:].strip()

    cmd = parse_command(text)
    if not cmd:
        # 不是控制器指令，直接闭嘴不回复（让AI正常处理）
        return

    cmd_type, target = cmd
    log(f"🎯 执行指令: {cmd_type} {target}")
    await execute_command(ws, group_id, cmd_type, target)


# ===================== 反向WS连接处理 =====================
async def handle_connection(ws):
    global _active_ws
    _active_ws = ws
    log("✅ NapCat 已反向连接，等待指令...")
    await flush_pending_msgs(ws)
    try:
        async for raw in ws:
            await handle_message(ws, raw)
    except websockets.exceptions.ConnectionClosed:
        log("⚠️ NapCat 反向连接断开，等待重连...")
    except Exception as e:
        log(f"❌ 连接处理异常: {e}")
    finally:
        _active_ws = None


# ===================== WebUI（配置界面 · 第一版：仪表盘） =====================
_webui_server = None
_stats_cache = {"mtime": 0.0, "data": {}}
_app_cfg_cache = {"mtime": 0.0, "data": {}}
_balance_cache = {"ts": 0.0, "data": None}


def _app_config():
    """读取整份 qqbot-config.yml（WebUI 需要 deepseek_api_key 等业务配置）"""
    try:
        m = os.path.getmtime(CONFIG_PATH)
    except OSError:
        return {}
    if _app_cfg_cache.get("data") and _app_cfg_cache.get("mtime") == m:
        return _app_cfg_cache["data"]
    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
    except Exception as e:
        log(f"⚠️ WebUI 读取配置失败: {e}")
        data = {}
    _app_cfg_cache.update(mtime=m, data=data)
    return data


SECRET_CLEAR = "__CLEAR__"   # 前端传这个值表示"清空该密钥"


def _has_secret(value) -> bool:
    return bool(str(value or "").strip())


def _apply_secret(data, key, value) -> bool:
    """写密钥的规则：没传/传空=保持不变；传 __CLEAR__=清空；其它=更新"""
    if value is None:
        return False
    s = str(value).strip()
    if s == "":
        return False
    if s == SECRET_CLEAR or s == "重置":
        data[key] = ""
        return True
    data[key] = s
    return True


def _clamp_int(value, low, high, default):
    try:
        n = int(value)
    except Exception:
        return default
    return max(low, min(high, n))


def _backup_config():
    try:
        if os.path.exists(CONFIG_PATH):
            shutil.copy2(CONFIG_PATH, CONFIG_PATH + ".bak")
    except Exception as e:
        log(f"⚠️ 备份配置失败: {e}")


def _load_config_roundtrip():
    """优先 ruamel.yaml（保留注释与格式），不可用时退回 PyYAML（会丢注释，仅本地开发环境会遇到）"""
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        text = f.read()
    try:
        from ruamel.yaml import YAML
        y = YAML()
        y.preserve_quotes = True
        y.width = 4096
        data = y.load(text) or {}
        return data, (lambda d, fp: y.dump(d, fp))
    except ImportError:
        data = yaml.safe_load(text) or {}
        return data, (lambda d, fp: yaml.safe_dump(d, fp, allow_unicode=True,
                                                    default_flow_style=False, sort_keys=False))


def _edit_config(mutator):
    """原子 + 保留注释地修改配置文件。
    mutator(data) -> bool（是否改动）；任何异常都不写盘，原文件不受影响。"""
    try:
        data, dumper = _load_config_roundtrip()
        if not mutator(data):
            return True, None
        _backup_config()
        tmp = CONFIG_PATH + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            dumper(data, f)
            f.flush()
            os.fsync(f.fileno())
        try:
            os.replace(tmp, CONFIG_PATH)
        except OSError:
            # Docker挂载卷不支持原子替换时降级
            try:
                if os.path.exists(CONFIG_PATH):
                    os.remove(CONFIG_PATH)
                os.rename(tmp, CONFIG_PATH)
            except OSError:
                # 最后降级：直接覆盖写入
                with open(tmp, "r", encoding="utf-8") as _rf:
                    _data = _rf.read()
                with open(CONFIG_PATH, "w", encoding="utf-8") as _wf:
                    _wf.write(_data)
                    _wf.flush()
                    os.fsync(_wf.fileno())
                if os.path.exists(tmp):
                    os.remove(tmp)
        _app_cfg_cache.update(mtime=0.0, data={})
        return True, None
    except Exception as e:
        try:
            if os.path.exists(CONFIG_PATH + ".tmp"):
                os.remove(CONFIG_PATH + ".tmp")
        except Exception:
            pass
        return False, str(e)


def _get_system_prompt():
    """读取系统提示词和bot_name"""
    cfg = _app_config()
    return {"prompt": cfg.get("system_prompt", ""), "bot_name": cfg.get("bot_name", "DeepSeek")}


def _get_connection_config():
    """读取连接配置（密钥不回显明文，只告诉前端"有没有设置"）"""
    cfg = _app_config()
    ctrl = cfg.get("controller", {}) or {}
    return {
        "onebot_ws": cfg.get("onebot_ws", ""),
        "napcat_container": cfg.get("napcat_container", ""),
        "napcat_data_dir": cfg.get("napcat_data_dir", ""),
        "napcat_view_dir": cfg.get("napcat_view_dir", ""),
        "douyin_api_enabled": cfg.get("enable_douyin_api", True),
        "douyin_api_key": "",  # 不回显
        "has_douyin_api_key": _has_secret(cfg.get("douyin_api_key")),
        "douyin_api_base": cfg.get("douyin_api_base", "http://dtk-api:8000"),
        "douyin_poll_interval": cfg.get("douyin_poll_interval", 3),
        "douyin_job_timeout": cfg.get("douyin_job_timeout", 300),
        "controller_ws_port": ctrl.get("ws_port", 6701),
        "controller_container_ai": ctrl.get("container_ai", ""),
        "controller_container_napcat": ctrl.get("container_napcat", ""),
    }


def _get_model_config():
    """读取模型配置（密钥不回显明文，只告诉前端"有没有设置"）"""
    cfg = _app_config()
    return {
        "deepseek_api_key": "",  # 不回显
        "has_deepseek_api_key": _has_secret(cfg.get("deepseek_api_key")),
        "deepseek_model": cfg.get("deepseek_model", "deepseek-flash"),
        "deepseek_vision_model": cfg.get("deepseek_vision_model", "deepseek-flash"),
        "bocha_api_key": "",  # 不回显
        "has_bocha_api_key": _has_secret(cfg.get("bocha_api_key")),
        "reasoning_effort": cfg.get("reasoning_effort", "high"),
        "max_context_len": cfg.get("max_context_len", 8),
        "enable_tts": cfg.get("enable_tts", True),
        "tts_engine": cfg.get("tts_engine", "volc"),
        "tts_voice": cfg.get("tts_voice", "zh-CN-XiaoyiNeural"),
        "volc_api_key": "",  # 不回显
        "has_volc_api_key": _has_secret(cfg.get("volc_api_key")),
        "volc_voice": cfg.get("volc_voice", ""),
        "minimax_api_key": "",  # 不回显
        "has_minimax_api_key": _has_secret(cfg.get("minimax_api_key")),
        "embedding_api_key": "",  # 不回显（阿里百炼 / 千问）
        "has_embedding_api_key": _has_secret(cfg.get("embedding_api_key")),
        "embedding_base_url": cfg.get("embedding_base_url", "https://dashscope.aliyuncs.com/compatible-mode/v1"),
        "embedding_model": cfg.get("embedding_model", "qwen3.7-text-embedding"),
        "embedding_dim": cfg.get("embedding_dim", 1024),
        "minimax_image_model": cfg.get("minimax_image_model", "image-01"),
        "minimax_image_ratio": cfg.get("minimax_image_ratio", "1:1"),
        "enable_image_command": cfg.get("enable_image_command", True),
        "enable_image_tool": cfg.get("enable_image_tool", True),
        "minimax_tts_model": cfg.get("minimax_tts_model", "speech-2.8-hd"),
        "minimax_tts_voice": cfg.get("minimax_tts_voice", "Chinese (Mandarin)_Crisp_Girl"),
        "minimax_tts_speed": cfg.get("minimax_tts_speed", 1.0),
        "minimax_tts_vol": cfg.get("minimax_tts_vol", 1.0),
        "minimax_tts_pitch": cfg.get("minimax_tts_pitch", 0),
        "bot_name": cfg.get("bot_name", "DeepSeek"),
    }


def _get_features_config():
    """读取功能开关配置（对话行为/消息处理/多媒体/群管理）"""
    cfg = _app_config()
    return {
        # 对话行为
        "enable_web_search": cfg.get("enable_web_search", True),
        "search_result_num": cfg.get("search_result_num", 8),
        "enable_thinking_chat": cfg.get("enable_thinking_chat", True),
        "enable_thinking_vision": cfg.get("enable_thinking_vision", True),
        "show_thinking_content": cfg.get("show_thinking_content", False),
        "use_chat_model_for_vision": cfg.get("use_chat_model_for_vision", True),
        "enable_ambient_cache": cfg.get("enable_ambient_cache", True),
        "memory_enabled": cfg.get("memory_enabled", True),
        "memory_saver_mode": cfg.get("memory_saver_mode", False),
        "memory_profile_max_chars": cfg.get("memory_profile_max_chars", 3000),
        "memory_compress_threshold": cfg.get("memory_compress_threshold", 2000),
        "memory_context_rounds": cfg.get("memory_context_rounds", 15),
        "memory_compress_interval": cfg.get("memory_compress_interval", 300),
        "memory_bank_enabled": cfg.get("memory_bank_enabled", True),
        "memory_bank_max_entries": cfg.get("memory_bank_max_entries", 20000),
        "memory_search_topk": cfg.get("memory_search_topk", 8),
        "ambient_max_msgs": cfg.get("ambient_max_msgs", 10),
        # 消息处理
        "enable_message_merge": cfg.get("enable_message_merge", True),
        "enable_input_status": cfg.get("enable_input_status", True),
        "input_status_grace": cfg.get("input_status_grace", 5),
        "merge_wait_seconds": cfg.get("merge_wait_seconds", 20),
        "merge_max_count": cfg.get("merge_max_count", 10),
        "enable_random_chat": cfg.get("enable_random_chat", True),
        "chat_trigger_probability": cfg.get("chat_trigger_probability", 0.3),
        "follow_up_trigger_count": cfg.get("follow_up_trigger_count", 3),
        "enable_split_reply": cfg.get("enable_split_reply", True),
        "enable_send_delay": cfg.get("enable_send_delay", True),
        "enable_typing_status": cfg.get("enable_typing_status", True),
        # 多媒体
        "enable_video": cfg.get("enable_video", True),
        "enable_voice": cfg.get("enable_voice", True),
        "enable_emoji": cfg.get("enable_emoji", True),
        "enable_image_command": cfg.get("enable_image_command", True),
        "enable_image_tool": cfg.get("enable_image_tool", True),
        "enable_download": cfg.get("enable_download", True),
        # 群与其他
        "enable_group_moderation": cfg.get("enable_group_moderation", True),
        "enable_voice_tool": cfg.get("enable_voice_tool", True),
        "enable_notify": cfg.get("enable_notify", False),
        "welcome_new_member": cfg.get("welcome_new_member", True),
        # AI主动消息
        "enable_proactive_msg": cfg.get("enable_proactive_msg", False),
        "enable_proactive_private": cfg.get("enable_proactive_private", True),
        "enable_proactive_qzone": cfg.get("enable_proactive_qzone", False),
        "proactive_idle_seconds": cfg.get("proactive_idle_seconds", 3600),
        "proactive_random_max": cfg.get("proactive_random_max", 3600),
        "proactive_cooldown_base": cfg.get("proactive_cooldown_base", 5400),
        "proactive_cooldown_random": cfg.get("proactive_cooldown_random", 5400),
        "proactive_group_giveup": cfg.get("proactive_group_giveup", 2),
        "proactive_private_giveup": cfg.get("proactive_private_giveup", 1),
        "proactive_dead_seconds": cfg.get("proactive_dead_seconds", 43200),
        "proactive_active_protect": cfg.get("proactive_active_protect", 300),
    }


def _save_model_config(data):
    """保存模型配置（只写白名单键；密钥留空=不改动；写盘原子且保留注释）"""
    def _mutate(cfg):
        changed = False
        if _apply_secret(cfg, "deepseek_api_key", data.get("deepseek_api_key")):
            changed = True
        if "deepseek_model" in data: cfg["deepseek_model"] = str(data["deepseek_model"]); changed = True
        if "deepseek_vision_model" in data: cfg["deepseek_vision_model"] = str(data["deepseek_vision_model"]); changed = True
        if _apply_secret(cfg, "bocha_api_key", data.get("bocha_api_key")):
            changed = True
        if "reasoning_effort" in data:
            eff = str(data["reasoning_effort"])
            cfg["reasoning_effort"] = eff if eff in ("off", "low", "medium", "high") else "high"
            changed = True
        if "max_context_len" in data:
            cfg["max_context_len"] = _clamp_int(data["max_context_len"], 1, 60, 8); changed = True
        if "enable_tts" in data: cfg["enable_tts"] = bool(data["enable_tts"]); changed = True
        if "tts_engine" in data:
            eng = str(data["tts_engine"])
            cfg["tts_engine"] = eng if eng in ("volc", "edge", "minimax", "none") else "volc"
            changed = True
        if "tts_voice" in data: cfg["tts_voice"] = str(data["tts_voice"]); changed = True
        if _apply_secret(cfg, "volc_api_key", data.get("volc_api_key")):
            changed = True
        if "volc_voice" in data: cfg["volc_voice"] = str(data["volc_voice"]); changed = True
        if _apply_secret(cfg, "minimax_api_key", data.get("minimax_api_key")):
            changed = True
        if _apply_secret(cfg, "embedding_api_key", data.get("embedding_api_key")):
            changed = True
        if "embedding_base_url" in data:
            _eb = str(data["embedding_base_url"] or "").strip()
            cfg["embedding_base_url"] = _eb or "https://dashscope.aliyuncs.com/compatible-mode/v1"
            changed = True
        if "embedding_model" in data:
            _em = str(data["embedding_model"] or "").strip()
            cfg["embedding_model"] = _em or "qwen3.7-text-embedding"
            changed = True
        if "embedding_dim" in data:
            cfg["embedding_dim"] = _clamp_int(data["embedding_dim"], 256, 2560, 1024)
            changed = True
        if "minimax_image_model" in data: cfg["minimax_image_model"] = str(data["minimax_image_model"]); changed = True
        if "minimax_image_ratio" in data:
            r = str(data["minimax_image_ratio"])
            cfg["minimax_image_ratio"] = r if r in ("1:1", "16:9", "9:16", "4:3", "3:4") else "1:1"
            changed = True
        if "enable_image_command" in data: cfg["enable_image_command"] = bool(data["enable_image_command"]); changed = True
        if "enable_image_tool" in data: cfg["enable_image_tool"] = bool(data["enable_image_tool"]); changed = True
        if "minimax_tts_model" in data:
            m = str(data["minimax_tts_model"])
            cfg["minimax_tts_model"] = m if m in ("speech-2.8-hd", "speech-2.8-turbo", "speech-2.6-hd", "speech-2.6-turbo", "speech-02-hd", "speech-02-turbo") else "speech-2.8-hd"
            changed = True
        if "minimax_tts_voice" in data: cfg["minimax_tts_voice"] = str(data["minimax_tts_voice"]); changed = True
        if "minimax_tts_speed" in data: cfg["minimax_tts_speed"] = float(data["minimax_tts_speed"]); changed = True
        if "minimax_tts_vol" in data: cfg["minimax_tts_vol"] = float(data["minimax_tts_vol"]); changed = True
        if "minimax_tts_pitch" in data: cfg["minimax_tts_pitch"] = int(data["minimax_tts_pitch"]); changed = True
        if "bot_name" in data: cfg["bot_name"] = str(data["bot_name"]); changed = True
        return changed

    return _edit_config(_mutate)

def _save_features_config(data):
    """保存功能开关配置（只写白名单键）"""
    def _b(key, dft=True):
        return bool(data.get(key, dft))
    def _mutate(cfg):
        changed = False
        # 对话行为
        if "enable_web_search" in data: cfg["enable_web_search"] = _b("enable_web_search"); changed = True
        if "search_result_num" in data: cfg["search_result_num"] = _clamp_int(data["search_result_num"], 1, 20, 8); changed = True
        if "enable_thinking_chat" in data: cfg["enable_thinking_chat"] = _b("enable_thinking_chat"); changed = True
        if "enable_thinking_vision" in data: cfg["enable_thinking_vision"] = _b("enable_thinking_vision"); changed = True
        if "show_thinking_content" in data: cfg["show_thinking_content"] = _b("show_thinking_content"); changed = True
        if "use_chat_model_for_vision" in data: cfg["use_chat_model_for_vision"] = _b("use_chat_model_for_vision"); changed = True
        if "enable_ambient_cache" in data: cfg["enable_ambient_cache"] = _b("enable_ambient_cache"); changed = True
        if "memory_enabled" in data: cfg["memory_enabled"] = _b("memory_enabled"); changed = True
        if "memory_saver_mode" in data: cfg["memory_saver_mode"] = _b("memory_saver_mode"); changed = True
        if "memory_profile_max_chars" in data: cfg["memory_profile_max_chars"] = _clamp_int(data["memory_profile_max_chars"], 500, 10000, 3000); changed = True
        if "memory_compress_threshold" in data: cfg["memory_compress_threshold"] = _clamp_int(data["memory_compress_threshold"], 200, 8000, 2000); changed = True
        if "memory_context_rounds" in data: cfg["memory_context_rounds"] = _clamp_int(data["memory_context_rounds"], 1, 100, 15); changed = True
        if "memory_compress_interval" in data: cfg["memory_compress_interval"] = _clamp_int(data["memory_compress_interval"], 60, 3600, 300); changed = True
        if "memory_bank_enabled" in data: cfg["memory_bank_enabled"] = _b("memory_bank_enabled"); changed = True
        if "memory_bank_max_entries" in data: cfg["memory_bank_max_entries"] = _clamp_int(data["memory_bank_max_entries"], 1000, 200000, 20000); changed = True
        if "memory_search_topk" in data: cfg["memory_search_topk"] = _clamp_int(data["memory_search_topk"], 1, 20, 8); changed = True
        if "ambient_max_msgs" in data: cfg["ambient_max_msgs"] = _clamp_int(data["ambient_max_msgs"], 1, 50, 10); changed = True
        # 消息处理
        if "enable_message_merge" in data: cfg["enable_message_merge"] = _b("enable_message_merge"); changed = True
        if "enable_input_status" in data: cfg["enable_input_status"] = _b("enable_input_status"); changed = True
        if "input_status_grace" in data: cfg["input_status_grace"] = _clamp_int(data["input_status_grace"], 1, 60, 5); changed = True
        if "merge_wait_seconds" in data: cfg["merge_wait_seconds"] = _clamp_int(data["merge_wait_seconds"], 5, 120, 20); changed = True
        if "merge_max_count" in data: cfg["merge_max_count"] = _clamp_int(data["merge_max_count"], 2, 30, 10); changed = True
        if "enable_random_chat" in data: cfg["enable_random_chat"] = _b("enable_random_chat"); changed = True
        if "chat_trigger_probability" in data:
            p = float(data["chat_trigger_probability"])
            cfg["chat_trigger_probability"] = max(0.0, min(1.0, p)); changed = True
        if "follow_up_trigger_count" in data: cfg["follow_up_trigger_count"] = _clamp_int(data["follow_up_trigger_count"], 0, 10, 3); changed = True
        if "enable_split_reply" in data: cfg["enable_split_reply"] = _b("enable_split_reply"); changed = True
        if "enable_send_delay" in data: cfg["enable_send_delay"] = _b("enable_send_delay"); changed = True
        if "enable_typing_status" in data: cfg["enable_typing_status"] = _b("enable_typing_status"); changed = True
        # 多媒体
        if "enable_video" in data: cfg["enable_video"] = _b("enable_video"); changed = True
        if "enable_voice" in data: cfg["enable_voice"] = _b("enable_voice"); changed = True
        if "enable_emoji" in data: cfg["enable_emoji"] = _b("enable_emoji"); changed = True
        if "enable_image_command" in data: cfg["enable_image_command"] = _b("enable_image_command"); changed = True
        if "enable_image_tool" in data: cfg["enable_image_tool"] = _b("enable_image_tool"); changed = True
        if "enable_download" in data: cfg["enable_download"] = _b("enable_download"); changed = True
        # 群与其他
        if "enable_group_moderation" in data: cfg["enable_group_moderation"] = _b("enable_group_moderation"); changed = True
        if "enable_voice_tool" in data: cfg["enable_voice_tool"] = _b("enable_voice_tool"); changed = True
        if "enable_notify" in data: cfg["enable_notify"] = _b("enable_notify", False); changed = True
        if "welcome_new_member" in data: cfg["welcome_new_member"] = _b("welcome_new_member"); changed = True
        # AI主动消息（时间单位秒；次数整数）
        if "enable_proactive_msg" in data: cfg["enable_proactive_msg"] = _b("enable_proactive_msg"); changed = True
        if "enable_proactive_private" in data: cfg["enable_proactive_private"] = _b("enable_proactive_private"); changed = True
        if "enable_proactive_qzone" in data: cfg["enable_proactive_qzone"] = _b("enable_proactive_qzone"); changed = True
        if "proactive_idle_seconds" in data:
            cfg["proactive_idle_seconds"] = _clamp_int(data["proactive_idle_seconds"], 60, 86400, 3600); changed = True
        if "proactive_random_max" in data:
            cfg["proactive_random_max"] = _clamp_int(data["proactive_random_max"], 0, 86400, 3600); changed = True
        if "proactive_cooldown_base" in data:
            cfg["proactive_cooldown_base"] = _clamp_int(data["proactive_cooldown_base"], 0, 86400, 5400); changed = True
        if "proactive_cooldown_random" in data:
            cfg["proactive_cooldown_random"] = _clamp_int(data["proactive_cooldown_random"], 0, 86400, 5400); changed = True
        if "proactive_group_giveup" in data:
            cfg["proactive_group_giveup"] = _clamp_int(data["proactive_group_giveup"], 1, 10, 2); changed = True
        if "proactive_private_giveup" in data:
            cfg["proactive_private_giveup"] = _clamp_int(data["proactive_private_giveup"], 1, 10, 1); changed = True
        if "proactive_dead_seconds" in data:
            cfg["proactive_dead_seconds"] = _clamp_int(data["proactive_dead_seconds"], 0, 604800, 43200); changed = True
        if "proactive_active_protect" in data:
            cfg["proactive_active_protect"] = _clamp_int(data["proactive_active_protect"], 0, 3600, 300); changed = True
        return changed
    return _edit_config(_mutate)



def _save_connection_config(data):
    """保存连接配置（只写白名单键；密钥留空=不改动；写盘原子且保留注释）"""
    def _mutate(cfg):
        changed = False
        if "onebot_ws" in data:
            _ws = str(data["onebot_ws"]).strip()
            if _ws and not _ws.startswith(("ws://", "wss://", "http://", "https://")):
                raise ValueError(f"OneBot 地址格式不合法：{_ws[:60]}")
            cfg["onebot_ws"] = _ws
            changed = True
        if "napcat_container" in data:
            _v = str(data["napcat_container"]).strip()
            if _v and not _valid_container_name(_v):
                raise ValueError(f"NapCat 容器名不合法：{_v[:60]}")
            cfg["napcat_container"] = _v
            changed = True
        if "napcat_data_dir" in data: cfg["napcat_data_dir"] = str(data["napcat_data_dir"]); changed = True
        if "napcat_view_dir" in data: cfg["napcat_view_dir"] = str(data["napcat_view_dir"]); changed = True
        if "douyin_api_enabled" in data: cfg["enable_douyin_api"] = bool(data["douyin_api_enabled"]); changed = True
        if _apply_secret(cfg, "douyin_api_key", data.get("douyin_api_key")):
            changed = True
        if "douyin_api_base" in data:
            base = str(data["douyin_api_base"]).strip()
            # 只接受 http/https，避免写出奇怪的地址
            if base == "" or base.startswith(("http://", "https://")):
                cfg["douyin_api_base"] = base
                changed = True
        if "douyin_poll_interval" in data:
            cfg["douyin_poll_interval"] = _clamp_int(data["douyin_poll_interval"], 1, 60, 3); changed = True
        if "douyin_job_timeout" in data:
            cfg["douyin_job_timeout"] = _clamp_int(data["douyin_job_timeout"], 30, 1800, 300); changed = True
        # controller段
        if "controller" not in cfg:
            cfg["controller"] = {}
        if "controller_ws_port" in data:
            cfg["controller"]["ws_port"] = _clamp_int(data["controller_ws_port"], 1, 65535, 6701); changed = True
        if "controller_container_ai" in data:
            _v = str(data["controller_container_ai"]).strip()
            if _v and not _valid_container_name(_v):
                raise ValueError(f"AI 容器名不合法：{_v[:60]}")
            cfg["controller"]["container_ai"] = _v
            changed = True
        if "controller_container_napcat" in data:
            _v = str(data["controller_container_napcat"]).strip()
            if _v and not _valid_container_name(_v):
                raise ValueError(f"NapCat 容器名不合法：{_v[:60]}")
            cfg["controller"]["container_napcat"] = _v
            changed = True
        return changed

    return _edit_config(_mutate)


def _save_system_prompt(prompt_text, bot_name=None):
    """保存系统提示词和bot_name（原子写入、保留注释）"""
    text = str(prompt_text or "")
    if len(text) > 200000:  # 上限，防止写爆配置文件
        return False, "提示词过长（上限 200000 字符）"

    def _mutate(data):
        changed = False
        if data.get("system_prompt") != text:
            data["system_prompt"] = text
            changed = True
        if bot_name is not None and str(bot_name).strip():
            data["bot_name"] = str(bot_name).strip()
            changed = True
        return changed

    return _edit_config(_mutate)


def _read_stats():
    """读主程序写入的统计数据（stats.json）。文件还不存在时仪表盘显示 0 / 未接入"""
    path = (_cfg or {}).get("stats_file") or STATS_FILE
    try:
        m = os.path.getmtime(path)
    except OSError:
        return {"available": False, "path": path}
    if _stats_cache["data"] and _stats_cache["mtime"] == m:
        return _stats_cache["data"]
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
        data["available"] = True
        data["path"] = path
    except Exception as e:
        data = {"available": False, "path": path, "error": str(e)}
    _stats_cache.update(mtime=m, data=data)
    return data


def _read_balance_history():
    """读取余额历史记录文件"""
    try:
        with open(BALANCE_HISTORY_FILE, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except (OSError, json.JSONDecodeError):
        return {}


def _atomic_write_json(path, data):
    """原子写JSON：先写临时文件再替换，避免读到半个文件"""
    try:
        d = os.path.dirname(path)
        if d:
            os.makedirs(d, exist_ok=True)
        tmp = path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
        return True
    except Exception as e:
        log(f"⚠️ 写入失败 {path}: {e}")
        return False


def _write_balance_history(data):
    """写入余额历史记录文件（原子写入）"""
    _atomic_write_json(BALANCE_HISTORY_FILE, data)


def _update_balance_history(total_balance):
    """更新今天的余额记录：不存在则创建（记录起始余额），存在则更新当前余额"""
    if total_balance is None:
        return
    today = datetime.now().strftime("%Y-%m-%d")
    history = _read_balance_history()
    if today not in history:
        # 新的一天：记录起始余额
        history[today] = {
            "start_balance": float(total_balance),
            "end_balance": float(total_balance),
            "used": 0.0
        }
    else:
        # 已存在：更新当前余额，计算消耗
        rec = history[today]
        rec["end_balance"] = float(total_balance)
        rec["used"] = max(0.0, rec["start_balance"] - float(total_balance))
    # 只保留最近60天的记录
    sorted_days = sorted(history.keys())
    if len(sorted_days) > 60:
        for day in sorted_days[:-60]:
            del history[day]
    _write_balance_history(history)


def _get_balance():
    """查 DeepSeek 余额（60 秒缓存；失败只影响那张卡片，不影响页面）"""
    if WEBUI_NO_NET:
        return {"available": False, "reason": "已禁用外网查询（测试模式）"}
    now = time.time()
    if _balance_cache["data"] and now - _balance_cache["ts"] < 60:
        return _balance_cache["data"]
    key = (_app_config().get("deepseek_api_key") or os.environ.get("DEEPSEEK_API_KEY") or "").strip()
    if not key:
        out = {"available": False, "reason": "未配置 deepseek_api_key"}
    else:
        try:
            req = urllib.request.Request("https://api.deepseek.com/user/balance",
                                         headers={"Authorization": f"Bearer {key}"})
            with urllib.request.urlopen(req, timeout=10) as r:
                data = json.loads(r.read().decode("utf-8", "replace"))
            info = (data.get("balance_infos") or [{}])[0] or {}
            out = {"available": bool(data.get("is_available")),
                   "total": info.get("total_balance"), "currency": info.get("currency"),
                   "topped_up": info.get("topped_up_balance"), "granted": info.get("granted_balance")}
        except Exception as e:
            out = {"available": False, "reason": f"查询失败：{str(e)[:80]}"}
    _balance_cache.update(ts=now, data=out)
    if out.get("available") and out.get("total") is not None:
        _update_balance_history(out.get("total"))
    return out


def _containers_status():
    out = {}
    for key in ("container_ai", "container_napcat", "container_self"):
        name = (_cfg or {}).get(key)
        if not name:
            continue
        try:
            out[name] = get_docker_client().containers.get(name).status
        except Exception:
            out[name] = "unknown"
    return out




def _stats_api_payload():
    """前端 /api/stats 接口：各类API调用次数（从stats.json读）+ 剩余额度（直接查DeepSeek）"""
    stats = _read_stats()
    bal = _get_balance()
    available = bool(bal.get("available")) and bal.get("total") is not None
    deepseek = int(stats.get("deepseek_calls") or 0)
    volcengine = int(stats.get("volcengine_calls") or 0)
    websearch = int(stats.get("websearch_calls") or 0)
    vision = int(stats.get("vision_calls") or 0)
    # 从余额历史读取今天的真实消耗
    history = _read_balance_history()
    today_str = datetime.now().strftime("%Y-%m-%d")
    used_today = float(history.get(today_str, {}).get("used", 0.0)) if today_str in history else 0.0
    return {
        "api_calls_today": deepseek + vision + volcengine + websearch,
        "vision_calls": vision,
        "deepseek_calls": deepseek,
        "volcengine_calls": volcengine,
        "websearch_calls": websearch,
        "calls_delta_percent": 0,
        "balance_used_today": used_today,
        "balance_total": float(bal.get("total")) if available else None,
        "unit": "CNY",
    }


def _history_api_payload(days=30):
    """前端 /api/history/{days}d 接口：从余额历史文件读取最近N天消耗数据"""
    history = _read_balance_history()
    result_days = []
    result_used = []
    today = datetime.now().date()
    for i in range(days - 1, -1, -1):
        d = today - timedelta(days=i)
        day_str = d.strftime("%Y-%m-%d")
        result_days.append(d.strftime("%m-%d"))
        if day_str in history:
            result_used.append(round(float(history[day_str].get("used", 0.0)), 4))
        else:
            result_used.append(0.0)
    return {"days": result_days, "balance_used": result_used}



_CONTAINER_NAME_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$")


def _valid_container_name(name) -> bool:
    """容器名白名单：Docker 容器名只允许字母数字和下划线/点/短横，且不能有换行（防请求行注入）"""
    return bool(_CONTAINER_NAME_RE.fullmatch(str(name or "").strip()))


def _docker_logs(container, tail_lines=300):
    """拿容器日志。优先用 docker SDK（安全，不拼 HTTP 请求行）；SDK 不可用时退回原始 socket，但容器名先校验。"""
    try:
        tail_lines = max(1, min(int(tail_lines), 2000))
    except Exception:
        tail_lines = 300
    if not _valid_container_name(container):
        return "[非法容器名，已拒绝]"
    # 首选 docker SDK
    try:
        c = get_docker_client().containers.get(container)
        raw = c.logs(tail=tail_lines, stdout=True, stderr=True, timestamps=False)
        if isinstance(raw, bytes):
            return raw.decode("utf-8", errors="replace")
        return str(raw)
    except Exception as sdk_err:
        # 退路：原始 socket（容器名已通过白名单校验，不含 CRLF）
        import socket as _sock
        s = _sock.socket(_sock.AF_UNIX, _sock.SOCK_STREAM)
        s.settimeout(5)
        try:
            s.connect("/var/run/docker.sock")
            req = ("GET /containers/%s/logs?stdout=1&stderr=1&tail=%d HTTP/1.1\r\n"
                   "Host: localhost\r\n"
                   "Connection: close\r\n\r\n" % (container, tail_lines))
            s.sendall(req.encode())
            chunks = []
            deadline = time.time() + 4
            while time.time() < deadline:
                s.settimeout(max(0.1, deadline - time.time()))
                try:
                    d = s.recv(65536)
                except Exception:
                    break
                if not d:
                    break
                chunks.append(d)
        finally:
            s.close()
        raw = b"".join(chunks)
        if b"\r\n\r\n" in raw:
            _, body = raw.split(b"\r\n\r\n", 1)
        else:
            body = raw
        # 解 chunked encoding
        parts = []
        i = 0
        while i < len(body):
            j = body.find(b"\r\n", i)
            if j < 0:
                break
            try:
                cl = int(body[i:j].split(b";")[0].strip(), 16)
            except ValueError:
                break
            if cl == 0:
                break
            parts.append(body[j + 2:j + 2 + cl])
            i = j + 2 + cl + 2
        body = b"".join(parts) if parts else body
        # Docker multiplexed stream: 8字节头(stream+pad+len)
        out = []
        i = 0
        while i + 8 <= len(body):
            ln = int.from_bytes(body[i + 4:i + 8], "big")
            if i + 8 + ln > len(body):
                break
            out.append(body[i + 8:i + 8 + ln])
            i += 8 + ln
        text = (b"".join(out)).decode("utf-8", errors="replace")
        return text if text else "[读取失败] " + str(sdk_err)


def _dashboard_payload():
    stats = _read_stats()
    bal = _get_balance()
    return {
        "server_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "ws_connected": _active_ws is not None,
        "containers": _containers_status(),
        "stats_available": True,
        "stats_path": stats.get("path"),
        "api_calls_today": int(stats.get("deepseek_calls") or 0) + int(stats.get("volcengine_calls") or 0) + int(stats.get("websearch_calls") or 0),
        "balance": bal,
        "currency": "CNY",
        "series": [],
    }


_WEBUI_HTML = r"""﻿<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>LunaBot 控制台</title>
  <link rel="preconnect" href="https://fonts.googleapis.com" />
  <link href="https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&family=JetBrains+Mono:wght@400;500&display=swap" rel="stylesheet" />
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/farvist@1/dist/farvist.min.css" />
  <script src="https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js"></script>
  <style>
    .dash { display: grid; grid-template-columns: 220px 1fr; min-height: 100vh; }
    .sidebar { position: sticky; top: 0; align-self: start; height: 100vh; padding: 1.25rem 1rem; display: flex; flex-direction: column; gap: .35rem; }
    .sidebar .nav-link { display: flex; align-items: center; gap: .65rem; }
    .content { padding: 1.5rem clamp(1rem, 3vw, 2.5rem) 3rem; min-width: 0; }
    .stat-value { font-size: 2rem; font-weight: 800; letter-spacing: -.02em; }
    .stat-value-big { font-size: 2.5rem; font-weight: 800; letter-spacing: -.02em; }
    .icon-tile { display: inline-flex; align-items: center; justify-content: center; width: 2.75rem; height: 2.75rem; border-radius: .75rem; font-weight: 700; font-size: .85rem; }
    .chart-container { width: 100%; height: 360px; }
    .demo-banner { position: sticky; top: 0; z-index: 100; padding: .5rem 1rem; background: linear-gradient(90deg, rgba(255,193,7,.15), rgba(255,152,0,.1)); border-bottom: 1px solid rgba(255,193,7,.3); text-align: center; font-size: .875rem; }
    .demo-banner .tag { display: inline-block; padding: .15rem .5rem; background: rgba(255,193,7,.2); border-radius: .4rem; margin-right: .5rem; font-weight: 600; }
    @media (max-width: 820px) { .dash { grid-template-columns: 1fr; } .sidebar { display: none; } }
  </style>
</head>
<body>
  <div class="demo-banner" id="demoBanner" style="display:none;">
    <span class="tag">演示数据</span>当前使用 Mock 数据源，关闭演示模式后将连接真实 API
  </div>

  <div class="dash">
    <aside class="sidebar glass-strong">
      <a class="navbar-brand mb-5" href="#" style="margin-left:2.75rem;margin-top:0.5rem;"><span class="text-gradient">LunaBot</span></a>
      <a class="nav-link active" href="#" data-page="dashboard">仪表盘</a>
      <a class="nav-link" href="#" data-page="prompt">系统提示词</a>
      <a class="nav-link" href="#" data-page="connection">连接设置</a>
      <a class="nav-link" href="#" data-page="download">下载设置</a>
      <a class="nav-link" href="#" data-page="features">功能设置</a>
      <a class="nav-link" href="#" data-page="whitelist">白名单</a>
      <a class="nav-link" href="#" data-page="logs">实时日志</a>
      <a class="nav-link" href="#" data-page="webui">WebUI设置</a>
      <div class="mt-auto">
        <div class="alert alert-accent">
          <div class="alert-heading">v0.1 测试版</div>
          <p class="mb-3 fs-sm">各项适配尚不完善，欢迎提交 Issue。</p>
          <button class="btn btn-gradient-primary btn-sm btn-block" onclick="toggleDemo()">
            <span id="demoBtnText">关闭演示模式</span>
          </button>
        </div>
      </div>
    </aside>

    <div id="main" tabindex="-1" class="content">
      <div class="d-flex align-items-center justify-content-between mb-5 flex-wrap gap-3">
        <div>
          <h1 class="h2 mb-1">仪表盘</h1>
          <p class="text-muted mb-0">LunaBot 运行状态概览</p>
        </div>

      </div>

      <div class="row gy-4 mb-6">
        <div class="col-12 col-md-6">
          <div class="card card-glow hover-lift"><div class="card-body">
            <div class="stat-value" id="apiCalls" style="font-size:2.75rem;">--</div>
            <div class="text-muted">总API调用次数</div>
          </div></div>
        </div>
        <div class="col-12 col-md-6">
          <div class="card card-glow hover-lift"><div class="card-body">
            <div class="stat-value" id="balanceUsed" style="font-size:2.75rem;">--</div>
            <div class="text-muted">剩余额度</div>
          </div></div>
        </div>
      </div>

      <div class="card"><div class="card-body">
        <div class="d-flex justify-content-between align-items-center mb-4">
          <h2 class="h4 mb-0">近30天余额消耗</h2>
          <div class="d-flex gap-2">
            <button class="btn btn-glass btn-sm" onclick="switchChartRange(7)">7天</button>
            <button class="btn btn-gradient-primary btn-sm" onclick="switchChartRange(30)">30天</button>
          </div>
        </div>
        <div class="chart-container" id="balanceChart"></div>
        <div class="text-muted text-center mt-2" id="chartTotal">30天合计：-- 元</div>
      </div></div>
    </div>
  </div>

  <script src="https://cdn.jsdelivr.net/npm/farvist@1/dist/farvist.min.js" defer></script>
  <script>
    const API_BASE = localStorage.getItem('lunabot_api_base') || '';
    let demoMode = localStorage.getItem('lunabot_demo') === 'true';
    let chartRange = 30;
    let balanceChart = null;

    const mockStats = {
      api_calls_today: 1497,
      calls_delta_percent: 2.5,
      balance_used_today: 0.80,
      balance_total: 8.56,
      unit: '元',
      msg_count_today: 328,
      active_groups: 3
    };

    function generateMockHistory(days) {
      const result = { days: [], balance_used: [] };
      const today = new Date();
      for (let i = days - 1; i >= 0; i--) {
        const d = new Date(today);
        d.setDate(d.getDate() - i);
        result.days.push(String(d.getMonth()+1).padStart(2,'0') + '-' + String(d.getDate()).padStart(2,'0'));
        const isWeekend = d.getDay() === 0 || d.getDay() === 6;
        const base = isWeekend ? 0.3 : 0.8;
        result.balance_used.push(Math.round((base + Math.random() * 0.6) * 100) / 100);
      }
      return result;
    }

    async function fetchStats() {
      if (demoMode) return mockStats;
      try {
        const res = await fetch(API_BASE + '/api/stats');
        if (!res.ok) throw new Error('API错误');
        return await res.json();
      } catch (e) {
        console.warn('获取真实数据失败，使用演示数据', e);
        return mockStats;
      }
    }

    async function fetchHistory(days) {
      if (demoMode) return generateMockHistory(days);
      try {
        const res = await fetch(API_BASE + '/api/history/' + days + 'd');
        if (!res.ok) throw new Error('API错误');
        return await res.json();
      } catch (e) {
        console.warn('获取历史数据失败，使用演示数据', e);
        return generateMockHistory(days);
      }
    }

    function renderStats(stats) {
      document.getElementById('apiCalls').textContent = stats.api_calls_today ? stats.api_calls_today.toLocaleString() : '--';
      // delta标签已移除
      // delta标签已移除

      document.getElementById('balanceUsed').textContent = stats.balance_used_today ? stats.balance_used_today.toFixed(2) : '--';
      const percent = stats.balance_total ? Math.round((stats.balance_used_today / stats.balance_total) * 100) : 0;
      // percent标签已移除
      // 进度条已移除

      // 消息数卡片已移除
      // 群聊数卡片已移除
    }

    function renderChart(history) {
      if (!balanceChart) {
        balanceChart = echarts.init(document.getElementById('balanceChart'));
      }
      const total = history.balance_used.reduce(function(a, b) { return a + b; }, 0).toFixed(2);
      document.getElementById('chartTotal').textContent = chartRange + '天合计：' + total + ' 元';

      balanceChart.setOption({
        tooltip: { trigger: 'axis', formatter: '{b}<br/>消耗：{c} 元' },
        grid: { left: '3%', right: '4%', bottom: '3%', containLabel: true },
        xAxis: {
          type: 'category',
          data: history.days,
          axisLine: { lineStyle: { color: 'rgba(255,255,255,0.2)' } },
          axisLabel: { color: 'rgba(255,255,255,0.6)' }
        },
        yAxis: {
          type: 'value',
          name: '元',
          axisLine: { lineStyle: { color: 'rgba(255,255,255,0.2)' } },
          axisLabel: { color: 'rgba(255,255,255,0.6)' },
          splitLine: { lineStyle: { color: 'rgba(255,255,255,0.08)' } }
        },
        series: [{
          name: '余额消耗',
          type: 'line',
          smooth: true,
          data: history.balance_used,
          areaStyle: {
            color: new echarts.graphic.LinearGradient(0, 0, 0, 1, [
              { offset: 0, color: 'rgba(139, 92, 246, 0.4)' },
              { offset: 1, color: 'rgba(139, 92, 246, 0.02)' }
            ])
          },
          lineStyle: { color: '#8b5cf6', width: 2 },
          itemStyle: { color: '#8b5cf6' },
          symbol: 'circle',
          symbolSize: 6
        }]
      });
    }

    async function loadData() {
      const results = await Promise.all([fetchStats(), fetchHistory(chartRange)]);
      renderStats(results[0]);
      renderChart(results[1]);
    }

    function toggleDemo() {
      demoMode = !demoMode;
      localStorage.setItem('lunabot_demo', demoMode);
      updateDemoUI();
      loadData();
    }

    function updateDemoUI() {
      document.getElementById('demoBanner').style.display = demoMode ? 'block' : 'none';
      document.getElementById('demoBtnText').textContent = demoMode ? '关闭演示模式' : '开启演示模式';
    }

    function switchChartRange(days) {
      chartRange = days;
      loadData();
    }

    function exportData() {
      alert('导出功能开发中...');
    }

    document.querySelectorAll('.nav-link').forEach(function(link) {
      link.addEventListener('click', function(e) {
        e.preventDefault();
        document.querySelectorAll('.nav-link').forEach(function(l) { l.classList.remove('active'); });
        link.classList.add('active');
        const page = link.dataset.page;
        if (page !== 'dashboard') {
          Farvist.toast({ title: '功能开发中', message: link.textContent.trim() + ' 页面即将上线', variant: 'info' });
        }
      });
    });

    window.addEventListener('load', function() {
      updateDemoUI();
      loadData();
    });

    window.addEventListener('resize', function() {
      if (balanceChart) balanceChart.resize();
    });
  </script>
</body>
</html>
"""

# 优先读取外部 webui-dashboard.html，方便修改UI不用改控制器代码
try:
    _ext_html_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "webui-dashboard.html")
    if os.path.exists(_ext_html_path):
        with open(_ext_html_path, "r", encoding="utf-8") as _f:
            _WEBUI_HTML = _f.read()
        print(f"已加载外部WebUI：{_ext_html_path}")
except Exception as _e:
    print(f"加载外部WebUI失败，使用内嵌版本：{_e}")





_LOGIN_HTML = r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0" />
  <title>LunaBot 控制台 - 登录</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", "Microsoft YaHei", sans-serif;
      background: linear-gradient(135deg, #0f0f1a 0%, #1a1a2e 50%, #16213e 100%);
      min-height: 100vh;
      display: flex;
      align-items: center;
      justify-content: center;
      color: #e2e8f0;
    }
    .login-card {
      background: rgba(30, 30, 50, 0.85);
      backdrop-filter: blur(20px);
      border: 1px solid rgba(124, 58, 237, 0.3);
      border-radius: 16px;
      padding: 40px;
      width: 100%;
      max-width: 380px;
      box-shadow: 0 20px 60px rgba(0, 0, 0, 0.5);
    }
    .login-title {
      font-size: 24px;
      font-weight: 700;
      text-align: center;
      margin-bottom: 8px;
      background: linear-gradient(135deg, #a78bfa, #7c3aed);
      -webkit-background-clip: text;
      -webkit-text-fill-color: transparent;
      background-clip: text;
    }
    .login-subtitle {
      font-size: 13px;
      color: #94a3b8;
      text-align: center;
      margin-bottom: 32px;
    }
    .form-group {
      margin-bottom: 20px;
    }
    .form-label {
      display: block;
      font-size: 13px;
      color: #cbd5e1;
      margin-bottom: 8px;
      font-weight: 500;
    }
    .form-input {
      width: 100%;
      padding: 12px 16px;
      background: rgba(15, 15, 26, 0.6);
      border: 1px solid rgba(148, 163, 184, 0.2);
      border-radius: 10px;
      color: #e2e8f0;
      font-size: 14px;
      outline: none;
      transition: border-color 0.2s, box-shadow 0.2s;
    }
    .form-input:focus {
      border-color: #7c3aed;
      box-shadow: 0 0 0 3px rgba(124, 58, 237, 0.15);
    }
    .form-input::placeholder {
      color: #64748b;
    }
    .login-btn {
      width: 100%;
      padding: 13px;
      background: linear-gradient(135deg, #7c3aed, #6d28d9);
      border: none;
      border-radius: 10px;
      color: #fff;
      font-size: 15px;
      font-weight: 600;
      cursor: pointer;
      transition: transform 0.15s, box-shadow 0.2s;
    }
    .login-btn:hover {
      transform: translateY(-1px);
      box-shadow: 0 8px 20px rgba(124, 58, 237, 0.4);
    }
    .login-btn:active {
      transform: translateY(0);
    }
    .login-btn:disabled {
      opacity: 0.6;
      cursor: not-allowed;
      transform: none;
    }
    .error-msg {
      background: rgba(239, 68, 68, 0.1);
      border: 1px solid rgba(239, 68, 68, 0.3);
      color: #fca5a5;
      padding: 10px 14px;
      border-radius: 8px;
      font-size: 13px;
      margin-bottom: 16px;
      display: none;
      text-align: center;
    }
    .error-msg.show {
      display: block;
    }
    .error-msg.shake {
      animation: loginShake 0.4s ease-in-out;
    }
    @keyframes loginShake {
      0%, 100% { transform: translateX(0); }
      20% { transform: translateX(-8px); }
      40% { transform: translateX(8px); }
      60% { transform: translateX(-6px); }
      80% { transform: translateX(6px); }
    }
    .hint {
      margin-top: 20px;
      font-size: 12px;
      color: #64748b;
      text-align: center;
      line-height: 1.6;
    }
    .hint code {
      background: rgba(15, 15, 26, 0.8);
      padding: 2px 6px;
      border-radius: 4px;
      color: #a78bfa;
      font-size: 11px;
    }
  </style>
</head>
<body>
  <div class="login-card">
    <div class="login-title">LunaBot 控制台</div>
    <div class="login-subtitle">请输入访问令牌以继续</div>
    <div class="error-msg" id="errorMsg">令牌错误，请检查后重试</div>
    <form id="loginForm" onsubmit="return doLogin(event)">
      <div class="form-group">
        <label class="form-label" for="tokenInput">访问令牌</label>
        <input type="password" class="form-input" id="tokenInput" placeholder="请输入访问密钥" autocomplete="off" autofocus />
      </div>
      <button type="submit" class="login-btn" id="loginBtn">登 录</button>
    </form>
    <div class="hint">
      首次登录可查看项目目录下的 <code>webui_token.txt</code>（容器内为 <code>/app/webui_token.txt</code>）
    </div>
  </div>
  <script>
    async function doLogin(e) {
      e.preventDefault();
      const token = document.getElementById('tokenInput').value.trim();
      const btn = document.getElementById('loginBtn');
      const err = document.getElementById('errorMsg');
      if (!token) {
        err.textContent = '请输入访问令牌';
        if (err.classList.contains('show')) {
          err.classList.remove('shake');
          void err.offsetWidth;
          err.classList.add('shake');
        } else {
          err.classList.add('show');
        }
        return false;
      }
      btn.disabled = true;
      btn.textContent = '验证中...';
      try {
        const res = await fetch('/api/login', {
          method: 'POST',
          headers: {'Content-Type': 'application/json'},
          body: JSON.stringify({token: token})
        });
        const data = await res.json();
        if (data.ok) {
          btn.textContent = '登录成功，跳转中...';
          setTimeout(() => { window.location.href = '/'; }, 500);
        } else {
          err.textContent = data.error || '令牌错误，请检查后重试';
          if (err.classList.contains('show')) {
            err.classList.remove('shake');
            void err.offsetWidth;
            err.classList.add('shake');
          } else {
            err.classList.add('show');
          }
          btn.disabled = false;
          btn.textContent = '登 录';
        }
      } catch (e) {
        err.textContent = '网络错误，请重试';
        err.classList.add('show');
        btn.disabled = false;
        btn.textContent = '登 录';
      }
      return false;
    }
  </script>
</body>
</html>"""

# ---- 登录防爆破（按来源IP计数，失败过多临时锁定） ----
LOGIN_MAX_FAILS = 5          # 连续失败次数
LOGIN_LOCK_SECONDS = 300     # 触发后锁定时间（秒）
_login_lock = threading.Lock()
_login_fails = {}            # ip -> [连续失败次数, 锁定截止时间戳]


def _login_locked(ip: str):
    """返回 (是否被锁, 剩余秒数)"""
    now = time.time()
    with _login_lock:
        rec = _login_fails.get(ip)
        if rec and rec[1] > now:
            return True, int(rec[1] - now)
        return False, 0


def _login_fail(ip: str):
    now = time.time()
    with _login_lock:
        rec = _login_fails.get(ip) or [0, 0.0]
        rec[0] += 1
        if rec[0] >= LOGIN_MAX_FAILS:
            rec[1] = now + LOGIN_LOCK_SECONDS
            rec[0] = 0
            log(f"⚠️ WebUI 登录失败次数过多，已锁定 {LOGIN_LOCK_SECONDS} 秒：{ip}")
        else:
            log(f"⚠️ WebUI 登录失败（{rec[0]}/{LOGIN_MAX_FAILS}）：{ip}")
        _login_fails[ip] = rec


def _login_success(ip: str):
    with _login_lock:
        _login_fails.pop(ip, None)


_WEAK_TOKENS = {
    "admin", "administrator", "password", "passwd", "123456", "12345678", "123456789",
    "qwerty", "abc123", "lunabot", "qqbot", "token", "webui", "test", "root", "letmein",
}


def _token_weak_reason(tok: str):
    """返回令牌偏弱的原因；够强则返回 None（只做提示，不阻止用户自定义）"""
    t = (tok or "").strip()
    if not t:
        return "空"
    if len(t) < 8:
        return f"只有 {len(t)} 位"
    if t.lower() in _WEAK_TOKENS:
        return "常见弱口令"
    if t.isdigit():
        return "纯数字，容易被猜到"
    if len(set(t)) <= 3:
        return "字符重复度过高"
    common = r"(luna|bot|admin|password|qwerty|123456|test|qq\d{5,})"
    if len(t) < 16:
        # 不足16位才严格看是否含常见词
        if re.search(common, t, re.I):
            return "长度不足16位且包含常见词"
        if re.fullmatch(r"[a-z]+", t):
            return "全是小写字母"
        return f"长度 {len(t)} 位（建议 ≥16）"
    # 16位以上：只有当"去掉常见词后几乎不剩什么"时才提示，避免误伤如 MyBot_Pass_2026!
    if len(re.sub(common, "", t, flags=re.I)) < 8:
        return "主要构成是常见词"
    return None


class _WebUIHandler(BaseHTTPRequestHandler):
    server_version = "LunaBotWebUI/0.1"

    def log_message(self, fmt, *args):  # 静默，交给 log()
        pass

    def _token(self) -> str:
        # 统一 strip：避免 YAML/环境变量里多打了空格导致"令牌明明对却登不上"
        return str((_cfg or {}).get("webui_token") or "").strip()

    def _authorized(self):
        """令牌校验：支持 X-WebUI-Token 头 / Cookie / 首次访问的 ?token=；用常量时间比较"""
        token = self._token()
        if not token:
            # 正常流程下 start_webui 一定会生成令牌；这里保底拒绝，避免"忘了设置=不校验"
            return False
        supplied = self.headers.get("X-WebUI-Token") or ""
        if not supplied:
            qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
            supplied = qs.get("token", [""])[0]
        if not supplied:
            cookie = self.headers.get("Cookie") or ""
            for part in cookie.split(";"):
                k, _, v = part.strip().partition("=")
                if k == "ctrl_token":
                    supplied = v
                    break
        return bool(supplied) and hmac.compare_digest(str(supplied), token)

    def _csrf_ok(self) -> bool:
        """POST 防CSRF：必须是 JSON 请求；带 Origin 时要求同源"""
        ctype = (self.headers.get("Content-Type") or "").lower()
        if "application/json" not in ctype:
            return False
        origin = self.headers.get("Origin")
        if origin:
            host = self.headers.get("Host") or ""
            try:
                o = urllib.parse.urlparse(origin)
                if o.netloc and o.netloc != host:
                    return False
            except Exception:
                return False
        return True

    def _send(self, code, body, ctype="text/html; charset=utf-8", extra=None):
        if isinstance(body, str):
            body = body.encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Referrer-Policy", "no-referrer")
        for k, v in (extra or {}).items():
            self.send_header(k, v)
        self.end_headers()
        if self.command != "HEAD":
            self.wfile.write(body)

    def _json(self, code, obj):
        self._send(code, json.dumps(obj, ensure_ascii=False), "application/json; charset=utf-8")

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        if not self._authorized():
            # 访问首页时显示登录页面，API请求才返回401
            if path in ("/", "/index.html"):
                self._send(200, _LOGIN_HTML)
            else:
                self._json(401, {"ok": False, "error": "unauthorized"})
            return
        token = self._token()
        # 带着 ?token= 首次访问：下发HttpOnly Cookie 并跳转到干净地址（避免令牌留在浏览器历史/日志里）
        if token and urllib.parse.parse_qs(parsed.query).get("token") and path == "/":
            self._send(302, b"", extra={
                "Location": "/",
                "Set-Cookie": f"ctrl_token={token}; Path=/; Max-Age=2592000; HttpOnly; SameSite=Strict",
            })
            return
        if path in ("/", "/index.html"):
            self._send(200, _WEBUI_HTML)
        elif path == "/api/dashboard":
            self._json(200, {"ok": True, "data": _dashboard_payload()})
        elif path == "/api/stats":
            self._json(200, _stats_api_payload())
        elif path.startswith("/api/history/") and path.endswith("d"):
            try:
                days = int(path[len("/api/history/"):-1])
            except ValueError:
                days = 30
            days = max(1, min(days, 365))   # 防止 /api/history/99999999d 打爆线程
            self._json(200, _history_api_payload(days))
        elif path == "/api/health":
            self._json(200, {"ok": True, "webui": True, "ws_connected": _active_ws is not None})
        elif path == "/api/config/prompt":
            _sp = _get_system_prompt()
            self._json(200, {"ok": True, "prompt": _sp["prompt"], "bot_name": _sp["bot_name"]})
        elif path == "/api/config/connection":
            self._json(200, {"ok": True, "config": _get_connection_config()})
        elif path == "/api/config/model":
            self._json(200, {"ok": True, "config": _get_model_config()})
        elif path == "/api/config/features":
            self._json(200, {"ok": True, "config": _get_features_config()})
        elif path == "/api/logs":
            try:
                qs = urllib.parse.parse_qs(urllib.parse.urlparse(self.path).query)
                lines = int(qs.get("lines", ["150"])[0])
                lines = max(10, min(lines, 500))
                import concurrent.futures as _cf
                result = {}
                names = ("qq-bot", "qq-controller", "qq-napcat")
                with _cf.ThreadPoolExecutor(max_workers=3) as ex:
                    futs = {ex.submit(_docker_logs, n, lines): n for n in names}
                    for f in _cf.as_completed(futs, timeout=8):
                        n = futs[f]
                        try:
                            result[n] = f.result()
                        except Exception as e:
                            result[n] = "[读取失败] " + str(e)
                for n in names:
                    if n not in result: result[n] = "[超时]"
                self._json(200, {"ok": True, "logs": result})
            except Exception as e:
                self._json(200, {"ok": False, "error": str(e)})
        else:
            self._json(404, {"ok": False, "error": "not found"})

    def do_POST(self):
        path = urllib.parse.urlparse(self.path).path
        # 登录接口不需要授权
        if path == "/api/login":
            ip = self.client_address[0] if self.client_address else "?"
            locked, remain = _login_locked(ip)
            if locked:
                self._json(429, {"ok": False, "error": f"尝试次数过多，请 {remain} 秒后再试"})
                return
            try:
                length = int(self.headers.get("Content-Length") or 0)
                if length > 4096:      # 防止超大 body 打内存
                    self._json(413, {"ok": False, "error": "请求体过大"})
                    return
                body = self.rfile.read(length) if length else b"{}"
                data = json.loads(body.decode("utf-8"))
                supplied = str(data.get("token") or "").strip()
                token = self._token()
                if token and supplied and hmac.compare_digest(supplied, token):
                    _login_success(ip)
                    self._send(200, json.dumps({"ok": True}, ensure_ascii=False), "application/json; charset=utf-8", extra={
                        "Set-Cookie": f"ctrl_token={token}; Path=/; Max-Age=2592000; HttpOnly; SameSite=Strict",
                    })
                else:
                    _login_fail(ip)
                    self._json(401, {"ok": False, "error": "令牌错误，请检查后重试"})
            except Exception as e:
                self._json(400, {"ok": False, "error": f"请求格式错误：{e}"})
            return
        if not self._authorized():
            self._json(401, {"ok": False, "error": "unauthorized"})
            return
        if not self._csrf_ok():
            self._json(403, {"ok": False, "error": "请求必须是同源 JSON（CSRF 防护）"})
            return
        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n) if n else b"{}"
        try:
            body = json.loads(raw.decode("utf-8", "replace") or "{}")
        except Exception:
            body = {}
        if path == "/api/config/prompt":
            prompt_text = str(body.get("prompt") or "")
            bot_name = body.get("bot_name")
            ok, err = _save_system_prompt(prompt_text, bot_name)
            self._json(200 if ok else 500, {"ok": ok, "error": err})
            return
        if path == "/api/config/connection":
            ok, err = _save_connection_config(body)
            self._json(200 if ok else 500, {"ok": ok, "error": err})
            return
        if path == "/api/config/model":
            ok, err = _save_model_config(body)
            self._json(200 if ok else 500, {"ok": ok, "error": err})
            return
        if path == "/api/config/features":
            ok, err = _save_features_config(body)
            self._json(200 if ok else 500, {"ok": ok, "error": err})
            return
        if path == "/api/restart":
            target = str(body.get("target") or "ai")
            name = {"ai": (_cfg or {}).get("container_ai"),
                    "napcat": (_cfg or {}).get("container_napcat"),
                    "self": (_cfg or {}).get("container_self")}.get(target)
            if not name:
                self._json(400, {"ok": False, "error": f"未知目标：{target}"})
                return
            ok, msg = docker_restart(name)
            self._json(200 if ok else 500, {"ok": ok, "message": msg, "container": name})
        else:
            self._json(501, {"ok": False, "error": "该功能将在后续版本实现（当前版本仅仪表盘可用）"})


def start_webui():
    """后台线程启动 WebUI，不阻塞反向WS主循环。
    安全：绝不运行在"无令牌"状态——没配置就随机生成一个，并在日志里打印一次。"""
    global _webui_server
    if not (_cfg or {}).get("webui_enabled", True):
        log("ℹ️ WebUI 已在配置中关闭（webui_enabled=false）")
        return None
    _existing = str((_cfg or {}).get("webui_token") or "").strip()
    if not _existing or len(_existing) < 8:
        if _existing:
            log(f"🔐 现有令牌过短（{len(_existing)} 位，容易被猜），已重新生成强令牌")
        _cfg["webui_token"] = secrets.token_urlsafe(24)
        # 持久化到配置文件，下次启动沿用同一个令牌
        try:
            _new_token = _cfg["webui_token"]
            def _persist_token(cfg):
                cfg.setdefault("controller", {})
                cfg["controller"]["webui_token"] = _new_token
                return True
            _edit_config(_persist_token)
            log("🔐 WebUI 未配置访问令牌，已自动生成并写入配置文件")
        except Exception as _e:
            log(f"🔐 WebUI 令牌已生成，但写入配置文件失败（本次运行有效）：{_e}")
    else:
        _weak = _token_weak_reason(_existing)
        if _weak:
            log(f"🔐 提示：当前自定义令牌偏弱（{_weak}），建议改成 16 位以上的随机串"
                f"（改 qqbot-config.yml 的 controller.webui_token，或 .env 的 CTRL_WEBUI_TOKEN；"
                f"注意 .env 的优先级更高）")
    port = int((_cfg or {}).get("webui_port") or WEBUI_PORT)
    # 把令牌写到独立卷里的文件，方便忘记时查看（日志里不再打印完整令牌，避免日志泄露）
    try:
        _token_file = TOKEN_FILE
        if os.path.isdir(_token_file):
            # bind mount 的文件在宿主机不存在时，Docker 会把它建成目录——这里明确提示而不是静默失败
            log(f"⚠️ 令牌路径 {_token_file} 是个目录（宿主机上缺少同名文件，Docker 自动建目录了）："
                f"请在项目目录执行 `New-Item webui_token.txt -ItemType File`（或 touch webui_token.txt）后重启控制器")
        else:
            _token_text = (
                "LunaBot WebUI 访问令牌（本文件含明文令牌，已被 .gitignore 忽略，切勿提交）\n"
                "========================\n"
                f"\n访问令牌：{_cfg['webui_token']}\n"
                f"访问地址：http://<NAS_IP>:{port}/  （在登录页输入上面的令牌）\n"
                "\n修改令牌：编辑 qqbot-config.yml 的 controller.webui_token\n"
                "登录成功后可删除此文件\n"
            )
            os.makedirs(os.path.dirname(_token_file) or ".", exist_ok=True)
            with open(_token_file, "w", encoding="utf-8") as _tf:
                _tf.write(_token_text)
            try:
                os.chmod(_token_file, 0o600)
            except Exception:
                pass
            log(f"🔐 令牌已写入文件：{_token_file}（忘记令牌时查看此文件，或看 qqbot-config.yml）")
    except Exception as _e:
        log(f"⚠️ 令牌文件写入失败：{_e}")
    try:
        _webui_server = ThreadingHTTPServer((WEBUI_HOST, port), _WebUIHandler)
    except OSError as e:
        log(f"❌ WebUI 启动失败（端口 {port} 可能被占用）：{e}")
        return None
    threading.Thread(target=_webui_server.serve_forever, daemon=True).start()
    log(f"🖥️ WebUI 已启动：http://<NAS_IP>:{port}/（已启用令牌校验）")
    return _webui_server


# ===================== 主循环 =====================
async def main():
    global _cfg
    _cfg = load_config()

    log("🚀 QQ远程控制器启动（反向WS模式）")
    log(f"📋 群白名单: {_cfg['allowed_groups']}")
    log(f"👤 用户白名单: {_cfg['allowed_users']}")
    log(f"📦 控制目标: AI={_cfg['container_ai']}, NapCat={_cfg['container_napcat']}, 自身={_cfg['container_self']}")

    try:
        get_docker_client().ping()
        log("✅ Docker socket 连接正常")
    except Exception as e:
        log(f"❌ Docker socket 连接失败: {e}")

    # 启动 WebUI（配置界面，第一版只有仪表盘）
    start_webui()

    ws_port = _cfg.get("ws_port", WS_PORT)
    log(f"🔊 反向WS服务端启动，监听 0.0.0.0:{ws_port}，等待NapCat反向连接...")
    log(f"   请在NapCat WebUI里添加反向WebSocket地址：ws://qq-controller:{ws_port}")

    async with websockets.serve(handle_connection, WS_HOST, ws_port):
        await asyncio.Future()  # 永久运行


if __name__ == "__main__":
    # CTRL_WEBUI_ONLY=1：只启动 WebUI（不连NapCat反向WS），方便本机调试页面
    if os.environ.get("CTRL_WEBUI_ONLY") == "1":
        _cfg = load_config()
        log("🚀 仅WebUI模式启动（CTRL_WEBUI_ONLY=1）")
        start_webui()
        try:
            while True:
                time.sleep(3600)
        except KeyboardInterrupt:
            log("\n👋 WebUI 已退出")
    else:
        try:
            asyncio.run(main())
        except KeyboardInterrupt:
            log("\n👋 控制器已退出")
